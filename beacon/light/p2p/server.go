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
	"slices"
	stdsync "sync"
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
	protoLCOptimistic = protoPrefix + "light_client_optimistic_update/1/ssz_snappy"
	maxUpdatesPerCall = 128 // MAX_REQUEST_LIGHT_CLIENT_UPDATES
	peerTries         = 8   // not every consensus node serves light client data (Prysm: off by default)
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
	s.node.callbacks.Store(&callbacks{
		optimistic: func(_ peer.ID, u types.OptimisticUpdate) {
			cb(request.Event{Type: sync.EvNewHead, Data: types.HeadInfo{Slot: u.Attested.Slot, BlockRoot: u.Attested.Hash()}})
			cb(request.Event{Type: sync.EvNewOptimisticUpdate, Data: u})
		},
		finality: func(_ peer.ID, u types.FinalityUpdate) {
			cb(request.Event{Type: sync.EvNewFinalityUpdate, Data: u})
		},
		event: cb,
	})
}

// Unsubscribe implements request.requestServer.
func (s *Server) Unsubscribe() {
	s.node.callbacks.Store(&callbacks{})
}

// SendRequest implements request.requestServer.
func (s *Server) SendRequest(id request.ID, req request.Request) {
	go func() {
		resp, err := s.serve(req)
		cb := s.node.callbacks.Load().event
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

// serve gets the answer to a request from peers.
func (s *Server) serve(req request.Request) (resp request.Response, err error) {
	defer recovered("request", func() { err = errors.New("panic") })
	switch data := req.(type) {
	case sync.ReqUpdates:
		return s.node.UpdatesByRange(data.FirstPeriod, data.Count)
	case sync.ReqCheckpointData:
		return s.node.Bootstrap(common.Hash(data))
	case sync.ReqFinality:
		return s.node.FinalityUpdate()
	}
	return nil, errors.New("not served over p2p")
}

// Timing of blsync's requests over req/resp. blsync cancels a request after 10 s
// (request.hardRequestTimeout), so the wait for a peer and all tries of one request fit
// in requestDeadline.
const (
	requestDeadline = 8 * time.Second
	tryTimeout      = 3 * time.Second        // per peer, plus perChunkTime per expected chunk
	perChunkTime    = 100 * time.Millisecond // an update is ~25 KB
	parallelTries   = 3
)

// candidates returns the connected peers to ask for proto, in random order: first those
// whose identify protocol list includes it, then those whose list isn't known yet. Peers
// known not to support it (many nodes run no light client server) are left out.
func (n *Node) candidates(proto string) []peer.ID {
	return orderCandidates(n.host.Network().Peers(), func(id peer.ID) []protocol.ID {
		protos, err := n.host.Peerstore().GetProtocols(id)
		if err != nil {
			return nil
		}
		return protos
	}, protocol.ID(proto))
}

// orderCandidates orders peers for proto: the ones listing it (shuffled), then the ones
// with no protocol list yet (shuffled); the ones whose list lacks it are dropped.
func orderCandidates(peers []peer.ID, protocols func(peer.ID) []protocol.ID, proto protocol.ID) []peer.ID {
	var serving, unknown []peer.ID
	for _, id := range peers {
		protos := protocols(id)
		switch {
		case len(protos) == 0:
			unknown = append(unknown, id)
		case slices.Contains(protos, proto):
			serving = append(serving, id)
		}
	}
	rand.Shuffle(len(serving), func(i, j int) { serving[i], serving[j] = serving[j], serving[i] })
	rand.Shuffle(len(unknown), func(i, j int) { unknown[i], unknown[j] = unknown[j], unknown[i] })
	return append(serving, unknown...)
}

// chunk is a response chunk: the SSZ payload and the fork of its context bytes.
type chunk struct {
	fork string
	ssz  []byte
}

// request sends a req/resp request to candidate peers, up to parallelTries at a time,
// until one answers with chunks that accept takes (accept decodes and keeps the result;
// it is called for one try at a time, and after a success for no other).
func (n *Node) request(proto string, body []byte, chunks int, accept func(chunks []chunk) error) error {
	ctx, cancel := context.WithTimeout(n.ctx, requestDeadline)
	defer cancel()
	// Right after start (blsync asks for the bootstrap first) there may be no peer yet.
	cands := n.candidates(proto)
	for len(cands) == 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("no peer serves %s", proto)
		case <-time.After(time.Second):
		}
		cands = n.candidates(proto)
	}
	var (
		mu      stdsync.Mutex
		done    bool
		lastErr error
		wg      stdsync.WaitGroup
		slots   = make(chan struct{}, parallelTries)
	)
loop:
	for _, id := range cands {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		mu.Lock()
		finished := done
		mu.Unlock()
		if finished {
			break
		}
		wg.Add(1)
		go func(id peer.ID) {
			defer wg.Done()
			defer func() { <-slots }()
			defer recovered("request "+proto, nil)
			res, err := n.requestPeer(ctx, id, proto, body, chunks)
			mu.Lock()
			defer mu.Unlock()
			if done {
				return
			}
			if err == nil {
				err = accept(res)
			}
			if err == nil {
				done = true
				cancel() // stop the other tries
				return
			}
			lastErr = err
			log.Debug("Consensus p2p request to peer failed", "proto", proto, "peer", id, "err", err)
		}(id)
	}
	wg.Wait()
	if done {
		return nil
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return lastErr
}

// maxResponseSize is the size limit of a light client protocol's response chunks.
func maxResponseSize(proto string) int {
	switch proto {
	case protoLCUpdates, protoLCBootstrap:
		return maxUpdateSize
	}
	return maxGossipSize
}

// requestPeer sends one request to a peer and returns the response chunks (up to chunks).
func (n *Node) requestPeer(ctx context.Context, id peer.ID, proto string, body []byte, chunks int) ([]chunk, error) {
	ctx, cancel := context.WithTimeout(ctx, tryTimeout+time.Duration(chunks)*perChunkTime)
	defer cancel()
	s, err := n.host.NewStream(ctx, id, protocol.ID(proto))
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if dl, ok := ctx.Deadline(); ok {
		s.SetDeadline(dl)
	}
	stop := context.AfterFunc(ctx, func() { s.Reset() }) // cancelled: unblock the reads
	defer stop()
	if body != nil {
		if err := writeSSZ(s, body); err != nil {
			return nil, err
		}
	}
	s.CloseWrite()
	r := bufio.NewReader(s)
	var res []chunk
	for len(res) < chunks {
		context, ssz, err := readResponse(r, 4, maxResponseSize(proto))
		if err == io.EOF && len(res) > 0 {
			break
		}
		if err != nil {
			return nil, err
		}
		res = append(res, chunk{fork: n.forkOf([4]byte(context)), ssz: ssz})
	}
	if len(res) == 0 {
		return nil, errors.New("empty response")
	}
	return res, nil
}

// UpdatesByRange fetches the best light client updates of count periods from first,
// with the next sync committee each announces. Each answer is checked (checkUpdate), so
// a peer's invalid answer loses to a valid one from another peer.
func (n *Node) UpdatesByRange(first, count uint64) (sync.RespUpdates, error) {
	if count > maxUpdatesPerCall {
		count = maxUpdatesPerCall
	}
	var body [16]byte
	binary.LittleEndian.PutUint64(body[0:8], first)
	binary.LittleEndian.PutUint64(body[8:16], count)
	var resp sync.RespUpdates
	err := n.request(protoLCUpdates, body[:], int(count), func(chunks []chunk) error {
		if uint64(len(chunks)) != count {
			return fmt.Errorf("got %d updates of %d", len(chunks), count)
		}
		var r sync.RespUpdates
		for i, ch := range chunks {
			u, c, err := DecodeUpdate(ch.fork, ch.ssz)
			if err != nil {
				return err
			}
			if err := checkUpdate(u, first+uint64(i)); err != nil {
				return err
			}
			r.Updates = append(r.Updates, u)
			r.Committees = append(r.Committees, c)
		}
		resp = r
		return nil
	})
	return resp, err
}

// checkUpdate checks a light client update a peer served for a period: the period, the
// order of its slots and its Merkle proofs, as the beacon API client does. The committee
// chain relies on these checks (CommitteeChain.InsertUpdate verifies only the sync
// committee signature): without them a peer could pair a signed header with a next sync
// committee of its own.
func checkUpdate(u *types.LightClientUpdate, period uint64) error {
	if p := u.AttestedHeader.Header.SyncPeriod(); p != period {
		return fmt.Errorf("update of period %d, asked for %d", p, period)
	}
	if u.AttestedHeader.SignatureSlot <= u.AttestedHeader.Header.Slot {
		return errors.New("update signed before its attested header")
	}
	if u.FinalizedHeader != nil && u.FinalizedHeader.Slot > u.AttestedHeader.Header.Slot {
		return errors.New("update finalized after its attested header")
	}
	if err := u.Validate(); err != nil {
		return fmt.Errorf("update of period %d: %v", period, err)
	}
	return nil
}

// checkOptimistic checks the order of an optimistic update's slots and its Merkle proof;
// the sync committee signature is for its user to check.
func checkOptimistic(u *types.OptimisticUpdate) error {
	if u.SignatureSlot <= u.Attested.Slot {
		return errors.New("update signed before its attested header")
	}
	return u.Validate()
}

// checkFinality checks the order of a finality update's slots and its Merkle proofs; the
// sync committee signature is for its user to check.
func checkFinality(u *types.FinalityUpdate) error {
	if u.SignatureSlot <= u.Attested.Slot {
		return errors.New("update signed before its attested header")
	}
	if u.Finalized.Slot > u.Attested.Slot {
		return errors.New("update finalized after its attested header")
	}
	return u.Validate()
}

// Bootstrap fetches the light client bootstrap for a (finalized, epoch boundary) block root.
func (n *Node) Bootstrap(root common.Hash) (*types.BootstrapData, error) {
	var boot *types.BootstrapData
	err := n.request(protoLCBootstrap, root[:], 1, func(chunks []chunk) error {
		b, err := DecodeBootstrap(chunks[0].fork, chunks[0].ssz)
		if err != nil {
			return err
		}
		if b.Header.Hash() != root {
			return fmt.Errorf("bootstrap for %x, asked for %x", b.Header.Hash(), root)
		}
		if err := b.Validate(); err != nil {
			return fmt.Errorf("bootstrap: %v", err)
		}
		boot = b
		return nil
	})
	return boot, err
}

// FinalityUpdate fetches the latest light client finality update.
func (n *Node) FinalityUpdate() (types.FinalityUpdate, error) {
	var u types.FinalityUpdate
	err := n.request(protoLCFinality, nil, 1, func(chunks []chunk) error {
		f, err := DecodeFinalityUpdate(chunks[0].fork, chunks[0].ssz)
		if err == nil {
			err = checkFinality(&f)
		}
		if err == nil {
			u = f
		}
		return err
	})
	return u, err
}

// AskLatest connects to the node at a multiaddr (with /p2p/<id>) and requests its latest
// light client optimistic and finality updates (for testing a serving node).
func (n *Node) AskLatest(addr string) (types.OptimisticUpdate, types.FinalityUpdate, error) {
	var (
		opt types.OptimisticUpdate
		fin types.FinalityUpdate
	)
	info, err := peer.AddrInfoFromString(addr)
	if err != nil {
		return opt, fin, err
	}
	ctx, cancel := context.WithTimeout(n.ctx, respTimeout)
	defer cancel()
	if err := n.host.Connect(ctx, *info); err != nil {
		return opt, fin, err
	}
	if err := n.exchangeStatus(info.ID); err != nil {
		return opt, fin, fmt.Errorf("status: %v", err)
	}
	chunks, err := n.requestPeer(n.ctx, info.ID, protoLCOptimistic, nil, 1)
	if err == nil {
		opt, err = DecodeOptimisticUpdate(chunks[0].fork, chunks[0].ssz)
	}
	if err != nil {
		return opt, fin, fmt.Errorf("optimistic update: %v", err)
	}
	chunks, err = n.requestPeer(n.ctx, info.ID, protoLCFinality, nil, 1)
	if err == nil {
		fin, err = DecodeFinalityUpdate(chunks[0].fork, chunks[0].ssz)
	}
	if err != nil {
		return opt, fin, fmt.Errorf("finality update: %v", err)
	}
	return opt, fin, nil
}
