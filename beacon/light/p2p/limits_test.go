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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestReadSSZLimits(t *testing.T) {
	for _, size := range []int{0, 1, 92, 70000} {
		var buf bytes.Buffer
		payload := bytes.Repeat([]byte{7}, size)
		if err := writeSSZ(&buf, payload); err != nil {
			t.Fatal(err)
		}
		got, err := readSSZ(bufio.NewReader(&buf), size)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("size %d: read back %d bytes, err %v", size, len(got), err)
		}
	}
	// Larger than the limit: refused before reading it.
	var buf bytes.Buffer
	writeSSZ(&buf, make([]byte, maxStatusSize+1))
	if _, err := readSSZ(bufio.NewReader(&buf), maxStatusSize); err == nil {
		t.Fatal("oversized payload read")
	}
	// Padding chunks without end: read no further than the payload's size allows.
	buf.Reset()
	buf.Write(binary.AppendUvarint(nil, 8))
	buf.Write([]byte{0xff, 6, 0, 0, 's', 'N', 'a', 'P', 'p', 'Y'}) // stream identifier
	for i := 0; i < 1000; i++ {
		buf.Write([]byte{0xfe, 16, 0, 0}) // padding chunk of 16 bytes
		buf.Write(make([]byte, 16))
	}
	total := buf.Len()
	r := bufio.NewReader(&buf)
	if _, err := readSSZ(r, maxPingSize); err == nil {
		t.Fatal("endless padding read as a payload")
	}
	if read := total - buf.Len() - r.Buffered(); int64(read) > 1+maxFramedSize(8) {
		t.Fatalf("read %d bytes for an 8-byte payload", read)
	}
}

func TestDecodeGossipLimit(t *testing.T) {
	if _, err := decodeGossip(snappy.Encode(nil, make([]byte, maxGossipSize))); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGossip(snappy.Encode(nil, make([]byte, maxGossipSize+1))); err == nil {
		t.Fatal("oversized gossip message decoded")
	}
}

func TestTopicsRegexp(t *testing.T) {
	cfg := params.MainnetLightConfig
	sched := digestSchedule(cfg, Networks[cfg.GenesisValidatorsRoot])
	rx := topicsRegexp(sched)
	d := sched[len(sched)-1].digest
	for _, name := range lightClientTopics {
		if topic := fmt.Sprintf("/eth2/%x/%s/ssz_snappy", d, name); !rx.MatchString(topic) {
			t.Errorf("%s not matched", topic)
		}
	}
	for _, topic := range []string{
		fmt.Sprintf("/eth2/%x/beacon_block/ssz_snappy", d),
		"/eth2/00000000/light_client_finality_update/ssz_snappy",
		fmt.Sprintf("/eth2/%x/light_client_finality_update/ssz_snappy/x", d),
	} {
		if rx.MatchString(topic) {
			t.Errorf("%s matched", topic)
		}
	}
}

func TestRecovered(t *testing.T) {
	var failed bool
	func() {
		defer recovered("test", func() { failed = true })
		panic("boom")
	}()
	if !failed {
		t.Fatal("fail not called")
	}
}

// TestStartStop starts a node on localhost without peers (the gossipsub options, score
// parameters and subscriptions included) and stops it.
func TestStartStop(t *testing.T) {
	cfg := params.MainnetLightConfig
	n, err := New(Config{Chain: cfg, Network: Networks[cfg.GenesisValidatorsRoot], GenesisTime: cfg.GenesisTime})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if len(n.subs) == 0 {
		t.Error("no gossip subscriptions")
	}
	n.Stop()
}

// TestOurStatus checks the status built from the verified updates, and the peer's status
// mirrored before those.
func TestOurStatus(t *testing.T) {
	n := new(Node)
	if st := n.ourStatus(true); len(st) != 92 || !bytes.Equal(st, make([]byte, 92)) {
		t.Fatalf("status before any: %x", st)
	}
	peerStatus := bytes.Repeat([]byte{9}, 84)
	n.status = peerStatus
	if st := n.ourStatus(false); !bytes.Equal(st[4:], peerStatus[4:]) {
		t.Fatalf("status before verified updates: %x, want the peer's", st)
	}
	n.finality.header = types.Header{Slot: 32*100 + 5, ProposerIndex: 1} // epoch 100's first slots empty
	n.optimistic.header = types.Header{Slot: 32*102 + 7, ProposerIndex: 2}
	st := n.ourStatus(true)
	fin, head := n.finality.header.Hash(), n.optimistic.header.Hash()
	if !bytes.Equal(st[4:36], fin[:]) || binary.LittleEndian.Uint64(st[36:44]) != 101 ||
		!bytes.Equal(st[44:76], head[:]) || binary.LittleEndian.Uint64(st[76:84]) != 32*102+7 ||
		binary.LittleEndian.Uint64(st[84:92]) != 32*102+7 {
		t.Fatalf("status %x", st)
	}
	n.finality.header.Slot = 32 * 101 // the epoch's first block
	if st := n.ourStatus(false); binary.LittleEndian.Uint64(st[36:44]) != 101 {
		t.Fatalf("finalized epoch %d, want 101", binary.LittleEndian.Uint64(st[36:44]))
	}
}

func TestLoadOrCreateKey(t *testing.T) {
	file := filepath.Join(t.TempDir(), "geth", "beacon-p2p-nodekey")
	k1, err := LoadOrCreateKey(file)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := LoadOrCreateKey(file)
	if err != nil || !k1.Equal(k2) {
		t.Fatalf("key not kept: %v", err)
	}
}

// TestTwoNodes connects two nodes on localhost: the status exchange, the metadata (with
// the custody group count peers require) and the serving peer count (nodes like these
// serve no updates by range).
func TestTwoNodes(t *testing.T) {
	cfg := params.MainnetLightConfig
	start := func() *Node {
		n, err := New(Config{Chain: cfg, Network: Networks[cfg.GenesisValidatorsRoot], GenesisTime: cfg.GenesisTime,
			VerifyHeader: func(types.SignedHeader) (bool, error) { return true, nil }})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(n.Stop)
		return n
	}
	a, b := start(), start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.host.Connect(ctx, peer.AddrInfo{ID: a.host.ID(), Addrs: a.host.Addrs()}); err != nil {
		t.Fatal(err)
	}
	if err := b.exchangeStatus(a.host.ID()); err != nil {
		t.Fatal("status:", err)
	}
	s, err := b.host.NewStream(ctx, a.host.ID(), protoMetadata3)
	if err != nil {
		t.Fatal(err)
	}
	s.CloseWrite()
	_, md, err := readResponse(bufio.NewReader(s), 0, 25)
	if err != nil || len(md) != 25 || binary.LittleEndian.Uint64(md[17:]) != custodyRequirement {
		t.Fatalf("metadata %x, err %v", md, err)
	}
	for { // identify runs after the connection
		if protos, _ := b.host.Peerstore().GetProtocols(a.host.ID()); len(protos) > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("no identify")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if n := b.servingPeers(); n != 0 {
		t.Fatalf("%d serving peers, want 0", n)
	}
	// Once each sees the other on the topic, gossipsub meshes them at its next heartbeat
	// and scores them from then on: let a few heartbeats run.
	topic := fmt.Sprintf("/eth2/%x/%s/ssz_snappy", a.Digest(), lightClientTopics[0])
	for len(a.ps.ListPeers(topic)) == 0 || len(b.ps.ListPeers(topic)) == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("peers not on the light client topic")
		case <-time.After(50 * time.Millisecond):
		}
	}
	time.Sleep(3 * time.Second)
}

func TestAvoidBound(t *testing.T) {
	n := &Node{backoff: make(map[peer.ID]time.Time)}
	for i := 0; i < 3*maxBackoff; i++ {
		n.avoid(peer.ID(fmt.Sprint(i)), time.Hour)
		if len(n.backoff) > maxBackoff {
			t.Fatalf("%d peers kept", len(n.backoff))
		}
	}
	if _, ok := n.backoff[peer.ID(fmt.Sprint(3*maxBackoff-1))]; !ok {
		t.Fatal("the last peer isn't kept")
	}
}
