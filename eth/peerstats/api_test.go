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

package peerstats

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

// startAPIServer wires the peerstats namespace on an in-process
// rpc.Server and returns the connected client + cleanup.
func startAPIServer(t *testing.T, s *Stats) (*rpc.Client, func()) {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("peerstats", NewAPI(s)); err != nil {
		t.Fatalf("register peerstats namespace: %v", err)
	}
	client := rpc.DialInProc(srv)
	return client, func() {
		client.Close()
		srv.Stop()
	}
}

// TestAPIGetAllPeerStats exercises the all-peers RPC end-to-end:
// produces some EMA samples on the underlying Stats, calls the RPC,
// asserts the returned map contains the expected peer with the right
// shape.
func TestAPIGetAllPeerStats(t *testing.T) {
	s := New()
	s.NotifyPeerConnect("peerA")
	// One inclusion + one finalization for peerA + one request sample.
	s.NotifyBlock(map[string]int{"peerA": 1}, map[string]int{"peerA": 1})
	s.NotifyRequestResult("peerA", 50*time.Millisecond, false)

	client, stop := startAPIServer(t, s)
	defer stop()

	var got map[string]PeerStats
	if err := client.Call(&got, "peerstats_getAllPeerStats"); err != nil {
		t.Fatalf("call: %v", err)
	}
	ps, ok := got["peerA"]
	if !ok {
		t.Fatalf("peerA missing from response: %+v", got)
	}
	if ps.RecentIncluded == 0 {
		t.Errorf("RecentIncluded should be non-zero after NotifyBlock")
	}
	if ps.RecentFinalized == 0 {
		t.Errorf("RecentFinalized should be non-zero after NotifyBlock")
	}
	if ps.RequestLatencyEMA == 0 {
		t.Errorf("RequestLatencyEMA should be non-zero after a sample")
	}
}

// TestAPIGetPeerStatsKnown verifies the per-peer RPC returns the
// correct snapshot for a tracked peer.
func TestAPIGetPeerStatsKnown(t *testing.T) {
	s := New()
	s.NotifyPeerConnect("peerA")
	s.NotifyBlock(map[string]int{"peerA": 1}, nil)

	client, stop := startAPIServer(t, s)
	defer stop()

	var got *PeerStats
	if err := client.Call(&got, "peerstats_getPeerStats", "peerA"); err != nil {
		t.Fatalf("call: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil snapshot for known peer")
	}
	if got.RecentIncluded == 0 {
		t.Errorf("RecentIncluded should be non-zero")
	}
}

// TestAPIGetPeerStatsUnknown verifies the per-peer RPC returns null
// (a JSON null) for an untracked peer rather than erroring.
func TestAPIGetPeerStatsUnknown(t *testing.T) {
	s := New()
	client, stop := startAPIServer(t, s)
	defer stop()

	var raw json.RawMessage
	if err := client.Call(&raw, "peerstats_getPeerStats", "nobody"); err != nil {
		t.Fatalf("call: %v", err)
	}
	if string(raw) != "null" {
		t.Errorf("expected null for unknown peer, got %s", raw)
	}
}

// TestAPIGetProtectedSet covers both phases: before any
// SetProtectedSetFunc the RPC returns an empty map (not null), and
// after a callback is installed the RPC returns whatever the
// callback yields. This is the contract mempool-lens depends on
// for marking protected rows.
func TestAPIGetProtectedSet(t *testing.T) {
	srv := rpc.NewServer()
	api := NewAPI(New())
	if err := srv.RegisterName("peerstats", api); err != nil {
		t.Fatalf("register peerstats namespace: %v", err)
	}
	client := rpc.DialInProc(srv)
	defer client.Close()
	defer srv.Stop()

	// No callback installed yet → empty map (not null), so JS
	// consumers can iterate without a guard.
	var got map[string]bool
	if err := client.Call(&got, "peerstats_getProtectedSet"); err != nil {
		t.Fatalf("call (no callback): %v", err)
	}
	if got == nil {
		t.Errorf("expected empty map (not nil) when no callback installed")
	}
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}

	// Install a callback returning a fixed protected set.
	api.SetProtectedSetFunc(func() map[string]bool {
		return map[string]bool{"peerA": true, "peerC": true}
	})
	got = nil
	if err := client.Call(&got, "peerstats_getProtectedSet"); err != nil {
		t.Fatalf("call (with callback): %v", err)
	}
	if !got["peerA"] || !got["peerC"] {
		t.Errorf("expected peerA and peerC protected, got %v", got)
	}
	if got["peerB"] {
		t.Error("peerB should not be protected")
	}
}
