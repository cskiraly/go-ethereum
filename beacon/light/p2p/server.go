// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/beacon/light/request"
	"github.com/ethereum/go-ethereum/beacon/light/sync"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	protoLCBootstrap  = protoPrefix + "light_client_bootstrap/1/ssz_snappy"
	protoLCUpdates    = protoPrefix + "light_client_updates_by_range/1/ssz_snappy"
	protoLCFinality   = protoPrefix + "light_client_finality_update/1/ssz_snappy"
	maxUpdatesPerCall = 128 // MAX_REQUEST_LIGHT_CLIENT_UPDATES
	peerTries         = 3
)

// Server makes the light client's p2p node a request server for blsync: gossiped
// updates become head events, and blsync's requests are sent to connected peers.
// It implements the request package's server interface.
type Server struct {
	node *Node
}

// NewServer wraps a node (not started yet) into a request server.
func NewServer(node *Node) *Server { return &Server{node: node} }

// Name implements request.requestServer.
func (s *Server) Name() string { return "consensus-p2p" }

// Subscribe implements request.requestServer.
func (s *Server) Subscribe(cb func(request.Event)) {
	s.node.cfg.OnOptimisticUpdate = func(_ peer.ID, u types.OptimisticUpdate) {
		cb(request.Event{Type: sync.EvNewHead, Data: types.HeadInfo{Slot: u.Attested.Slot, BlockRoot: u.Attested.Hash()}})
		cb(request.Event{Type: sync.EvNewOptimisticUpdate, Data: u})
	}
	s.node.cfg.OnFinalityUpdate = func(_ peer.ID, u types.FinalityUpdate) {
		cb(request.Event{Type: sync.EvNewFinalityUpdate, Data: u})
	}
	s.node.eventCallback = cb
}

// Unsubscribe implements request.requestServer.
func (s *Server) Unsubscribe() {
	s.node.cfg.OnOptimisticUpdate, s.node.cfg.OnFinalityUpdate = nil, nil
}

// SendRequest implements request.requestServer.
func (s *Server) SendRequest(id request.ID, req request.Request) {
	go func() {
		var (
			resp request.Response
			err  error
		)
		switch data := req.(type) {
		case sync.ReqUpdates:
			resp, err = s.node.UpdatesByRange(data.FirstPeriod, data.Count)
		case sync.ReqCheckpointData:
			resp, err = s.node.Bootstrap(common.Hash(data))
		case sync.ReqFinality:
			resp, err = s.node.FinalityUpdate()
		default:
			err = fmt.Errorf("not served over p2p")
		}
		cb := s.node.eventCallback
		if cb == nil {
			return
		}
		if err != nil {
			log.Debug("Consensus p2p request failed", "type", reflect.TypeOf(req), "reqid", id, "err", err)
			cb(request.Event{Type: request.EvFail, Data: request.RequestResponse{ID: id, Request: req}})
			return
		}
		cb(request.Event{Type: request.EvResponse, Data: request.RequestResponse{ID: id, Request: req, Response: resp}})
	}()
}

// request sends a req/resp request to a connected peer (trying a few) and returns
// the response chunks, each with its 4 context bytes. read decodes one chunk.
func (n *Node) request(proto string, body []byte, chunks int, read func(ssz []byte) error) error {
	peers := n.host.Network().Peers()
	if len(peers) == 0 {
		return errors.New("no peers")
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	var lastErr error
	for i := 0; i < len(peers) && i < peerTries; i++ {
		if lastErr = n.requestPeer(peers[i], proto, body, chunks, read); lastErr == nil {
			return nil
		}
		log.Debug("Consensus p2p request to peer failed", "proto", proto, "peer", peers[i], "err", lastErr)
	}
	return lastErr
}

func (n *Node) requestPeer(id peer.ID, proto string, body []byte, chunks int, read func([]byte) error) error {
	ctx, cancel := context.WithTimeout(n.ctx, respTimeout)
	defer cancel()
	s, err := n.host.NewStream(ctx, id, protocol.ID(proto))
	if err != nil {
		return err
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	if body != nil {
		if err := writeSSZ(s, body); err != nil {
			return err
		}
	}
	s.CloseWrite()
	r := bufio.NewReader(s)
	got := 0
	for ; got < chunks; got++ {
		_, ssz, err := readResponse(r, 4)
		if err == io.EOF && got > 0 {
			break
		}
		if err != nil {
			return err
		}
		if err := read(ssz); err != nil {
			return err
		}
	}
	if got == 0 {
		return errors.New("empty response")
	}
	return nil
}

// UpdatesByRange fetches the best light client updates of count periods from first,
// with the next sync committee each announces.
func (n *Node) UpdatesByRange(first, count uint64) (sync.RespUpdates, error) {
	if count > maxUpdatesPerCall {
		count = maxUpdatesPerCall
	}
	var body [16]byte
	binary.LittleEndian.PutUint64(body[0:8], first)
	binary.LittleEndian.PutUint64(body[8:16], count)
	var resp sync.RespUpdates
	err := n.request(protoLCUpdates, body[:], int(count), func(ssz []byte) error {
		u, c, err := DecodeUpdate(n.cfg.ForkName, ssz)
		if err != nil {
			return err
		}
		resp.Updates = append(resp.Updates, u)
		resp.Committees = append(resp.Committees, c)
		return nil
	})
	if err == nil && uint64(len(resp.Updates)) != count {
		err = fmt.Errorf("got %d updates of %d", len(resp.Updates), count)
	}
	return resp, err
}

// Bootstrap fetches the light client bootstrap for a (finalized, epoch boundary) block root.
func (n *Node) Bootstrap(root common.Hash) (*types.BootstrapData, error) {
	var boot *types.BootstrapData
	err := n.request(protoLCBootstrap, root[:], 1, func(ssz []byte) (err error) {
		boot, err = DecodeBootstrap(n.cfg.ForkName, ssz)
		return err
	})
	return boot, err
}

// FinalityUpdate fetches the latest light client finality update.
func (n *Node) FinalityUpdate() (types.FinalityUpdate, error) {
	var u types.FinalityUpdate
	err := n.request(protoLCFinality, nil, 1, func(ssz []byte) (err error) {
		u, err = DecodeFinalityUpdate(n.cfg.ForkName, ssz)
		return err
	})
	return u, err
}
