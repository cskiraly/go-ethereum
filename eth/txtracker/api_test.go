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

package txtracker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// startAPIServer wires a tracker through the txtracker namespace on an
// in-process rpc.Server and returns the connected client + cleanup.
func startAPIServer(t *testing.T, tr *Tracker) (*rpc.Client, func()) {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("txtracker", NewAPI(tr)); err != nil {
		t.Fatalf("register txtracker namespace: %v", err)
	}
	client := rpc.DialInProc(srv)
	return client, func() {
		client.Close()
		srv.Stop()
	}
}

// TestAPIGetTxRoundTrip exercises txtracker_getTx end-to-end through
// the JSON-RPC server: NotifyReceived populates body fields, the
// client's call returns a JSON-decoded TxInfo with matching content.
func TestAPIGetTxRoundTrip(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(42)
	tr.NotifyReceived("peerA", []*types.Transaction{tx})

	client, stop := startAPIServer(t, tr)
	defer stop()

	var info TxInfo
	if err := client.Call(&info, "txtracker_getTx", tx.Hash()); err != nil {
		t.Fatalf("txtracker_getTx: %v", err)
	}
	if info.Hash != tx.Hash() {
		t.Errorf("Hash: got %x, want %x", info.Hash, tx.Hash())
	}
	if info.Nonce != 42 {
		t.Errorf("Nonce: got %d, want 42", info.Nonce)
	}
	if info.TxSize == 0 {
		t.Error("TxSize should be non-zero after NotifyReceived")
	}
}

// TestAPIGetTxUnknownReturnsNull verifies that an unknown hash maps
// to a JSON null, not an error.
func TestAPIGetTxUnknownReturnsNull(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	client, stop := startAPIServer(t, tr)
	defer stop()

	var raw json.RawMessage
	if err := client.Call(&raw, "txtracker_getTx", common.Hash{1, 2, 3}); err != nil {
		t.Fatalf("call: %v", err)
	}
	if string(raw) != "null" {
		t.Errorf("expected null for unknown hash, got %s", raw)
	}
}

// TestAPIDiagnostics verifies the diagnostics method reports tracker
// drop counters.
func TestAPIDiagnostics(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	client, stop := startAPIServer(t, tr)
	defer stop()

	var diag Diagnostics
	if err := client.Call(&diag, "txtracker_diagnostics"); err != nil {
		t.Fatalf("txtracker_diagnostics: %v", err)
	}
	if diag.ObsDropped != 0 || diag.StateDropped != 0 {
		t.Errorf("expected zero drops on fresh tracker, got %+v", diag)
	}
	if len(diag.EvictedByStatus) != 0 {
		t.Errorf("expected empty eviction histogram on fresh tracker, got %+v", diag.EvictedByStatus)
	}
}

// TestAPIDiagnosticsEvictionHistogram verifies the diagnostics
// endpoint surfaces eviction counts attributed by status label.
func TestAPIDiagnosticsEvictionHistogram(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()
	tr.setMaxTracked(1)

	// 3 announced, 2 of them get FIFO-evicted with cap=1.
	hashes := []common.Hash{makeTx(1).Hash(), makeTx(2).Hash(), makeTx(3).Hash()}
	tr.NotifyAnnounced("peerA", hashes, nil, nil)

	client, stop := startAPIServer(t, tr)
	defer stop()

	var diag Diagnostics
	if err := client.Call(&diag, "txtracker_diagnostics"); err != nil {
		t.Fatalf("call: %v", err)
	}
	if diag.EvictedByStatus["announced"] != 2 {
		t.Errorf(`EvictedByStatus["announced"]: got %d, want 2 (full diag=%+v)`, diag.EvictedByStatus["announced"], diag.EvictedByStatus)
	}
}

// TestAPISubscribeStateChanges verifies a client can subscribe to the
// state-change feed and receive forwarded events.
func TestAPISubscribeStateChanges(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	client, stop := startAPIServer(t, tr)
	defer stop()

	// StateChanges notifications carry an array of events (batched by
	// the forwarder so a peer-announce burst lands as one frame).
	ch := make(chan []StateChange, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sub, err := client.Subscribe(ctx, "txtracker", ch, "stateChanges")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	hash := makeTx(1).Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)

	select {
	case batch := <-ch:
		if len(batch) != 1 {
			t.Fatalf("expected 1 event, got %d", len(batch))
		}
		ev := batch[0]
		if ev.NewStatus != StatusAnnounced {
			t.Errorf("expected StatusAnnounced, got %v", ev.NewStatus)
		}
		if ev.TxHash != hash {
			t.Errorf("expected tx hash match, got %x", ev.TxHash)
		}
	case err := <-sub.Err():
		t.Fatalf("subscription error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for state-change event")
	}
}

// TestAPISubscribeObservations verifies the observation-feed
// subscription.
func TestAPISubscribeObservations(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	client, stop := startAPIServer(t, tr)
	defer stop()

	ch := make(chan []Observation, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sub, err := client.Subscribe(ctx, "txtracker", ch, "observations")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	hash := makeTx(1).Hash()
	tr.NotifyAnnouncedOutbound("peerA", []common.Hash{hash})

	select {
	case batch := <-ch:
		if len(batch) != 1 {
			t.Fatalf("expected 1 event, got %d", len(batch))
		}
		if batch[0].Kind != ObsAnnouncedOutbound {
			t.Errorf("expected ObsAnnouncedOutbound, got %v", batch[0].Kind)
		}
	case err := <-sub.Err():
		t.Fatalf("subscription error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for observation event")
	}
}
