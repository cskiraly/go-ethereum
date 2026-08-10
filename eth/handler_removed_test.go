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

package eth

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/txtracker"
)

// TestHandlerForwardsRemovedTxsToTracker verifies the wiring added in
// the txpool-removed-events branch: the handler subscribes to
// txpool.SubscribeRemovedTransactions on Start and the removedTxsLoop
// goroutine forwards each (Hash, Reason) pair to txTracker.NotifyDropped.
//
// The test pre-populates the tracker with a hash so NotifyDropped is
// not a no-op (it skips unknown hashes), then emits a RemovedTxsEvent
// directly on the testTxPool's feed and asserts the tracker reached
// StatusDropped with the supplied reason on the StateChange and on
// the resulting TxInfo.DropReason.
func TestHandlerForwardsRemovedTxsToTracker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(ethconfig.FullSync)
	defer h.close()

	h.handler.txTracker.Start(h.chain, nil)
	defer h.handler.txTracker.Stop()

	stateCh := make(chan txtracker.StateChange, 4)
	sub := h.handler.txTracker.SubscribeStateChanges(stateCh)
	defer sub.Unsubscribe()

	// Seed the tracker with a known hash and walk it through to Pooled
	// so NotifyDropped fires the transition. The strict eviction guard
	// only transitions out of StatusPooled — Announced → Dropped is
	// suppressed (see TestPoolEvictAfterIncludeIsNoop in eth/txtracker).
	hash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	h.handler.txTracker.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	h.handler.txTracker.NotifyAccepted("peerA", []common.Hash{hash})

	// Drain Announced + Pooled state changes so we observe Dropped cleanly.
	for i := 0; i < 2; i++ {
		select {
		case <-stateCh:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for setup state changes")
		}
	}

	// Emit the removal directly on the test pool's feed; the handler's
	// removedTxsLoop should pick it up and forward to NotifyDropped.
	h.txpool.removedFeed.Send(core.RemovedTxsEvent{
		Hashes:  []common.Hash{hash},
		Reasons: []core.RemovalReason{core.RemovalReplaced},
	})

	select {
	case ev := <-stateCh:
		if ev.TxHash != hash {
			t.Errorf("unexpected hash: got %x", ev.TxHash)
		}
		if ev.NewStatus != txtracker.StatusDropped {
			t.Errorf("status: got %v, want StatusDropped", ev.NewStatus)
		}
		if ev.Reason != "replaced" {
			t.Errorf("reason: got %q, want %q", ev.Reason, "replaced")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Dropped state change forwarded by handler")
	}

	info := h.handler.txTracker.GetTx(hash)
	if info == nil {
		t.Fatal("expected TxInfo for the dropped tx")
	}
	if info.Status != txtracker.StatusDropped {
		t.Errorf("TxInfo Status: got %v, want StatusDropped", info.Status)
	}
	if info.DropReason != "replaced" {
		t.Errorf("DropReason: got %q, want %q", info.DropReason, "replaced")
	}
	if info.Dropped.IsZero() {
		t.Error("expected Dropped timestamp to be set")
	}
}
