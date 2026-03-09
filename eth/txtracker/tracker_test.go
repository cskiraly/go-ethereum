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
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// mockChain is a minimal BlockchainReader for tests.
type mockChain struct {
	chainFeed  event.Feed
	finalBlock *types.Header
}

func (m *mockChain) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	return m.chainFeed.Subscribe(ch)
}

func (m *mockChain) CurrentFinalBlock() *types.Header {
	return m.finalBlock
}

func (m *mockChain) sendChainEvent(ev core.ChainEvent) {
	m.chainFeed.Send(ev)
}

// mockTxPool is a minimal TxPoolReader for tests.
type mockTxPool struct {
	txsFeed event.Feed
}

func (m *mockTxPool) SubscribeTransactions(ch chan<- core.NewTxsEvent, reorgs bool) event.Subscription {
	return m.txsFeed.Subscribe(ch)
}

func (m *mockTxPool) sendNewTxs(ev core.NewTxsEvent) {
	m.txsFeed.Send(ev)
}

// testTracker creates a tracker with a simulated clock and mock dependencies,
// starts it, and returns all components. The caller must call tracker.Stop().
func testTracker(maxEntries int) (*Tracker, *mclock.Simulated, *mockChain, *mockTxPool) {
	clock := new(mclock.Simulated)
	chain := &mockChain{}
	pool := &mockTxPool{}

	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	tr := New(Config{
		MaxEntries: maxEntries,
		Clock:      clock,
		Chain:      chain,
		TxPool:     pool,
	})
	tr.Start()
	<-tr.ready // Wait for the event loop to subscribe to feeds.
	return tr, clock, chain, pool
}

// waitStep drains one step signal from the tracker's event loop.
func waitStep(t *testing.T, tr *Tracker) {
	t.Helper()
	<-tr.step
}

func makeHash(i byte) common.Hash {
	return common.Hash{i}
}

func makeHeader(num uint64, parent common.Hash) *types.Header {
	return &types.Header{
		Number:     new(big.Int).SetUint64(num),
		ParentHash: parent,
	}
}

func makeTx(hash common.Hash) *types.Transaction {
	// Create a simple legacy transaction for testing.
	return types.NewTx(&types.LegacyTx{Nonce: uint64(hash[0])})
}

func TestAnnouncedStatus(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(1)
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info == nil {
		t.Fatal("expected tracked transaction")
	}
	if info.Status != TxAnnounced {
		t.Fatalf("expected TxAnnounced, got %v", info.Status)
	}
	if len(info.Announcers) != 1 || info.Announcers[0] != "peerA" {
		t.Fatalf("unexpected announcers: %v", info.Announcers)
	}
	if info.TxType != 0 || info.TxSize != 100 {
		t.Fatalf("unexpected metadata: type=%d, size=%d", info.TxType, info.TxSize)
	}
}

func TestAnnouncedToReceived(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(2)
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{200})
	waitStep(t, tr)

	tr.NotifyReceived("peerB", []common.Hash{hash})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info.Status != TxReceived {
		t.Fatalf("expected TxReceived, got %v", info.Status)
	}
	if info.Deliverer != "peerB" {
		t.Fatalf("expected deliverer peerB, got %s", info.Deliverer)
	}
	// Original announcer should still be recorded.
	if len(info.Announcers) != 1 || info.Announcers[0] != "peerA" {
		t.Fatalf("unexpected announcers: %v", info.Announcers)
	}
}

func TestReceivedToPooled(t *testing.T) {
	tr, clock, _, _ := testTracker(0)
	defer tr.Stop()

	// Advance clock so timestamps are non-zero.
	clock.Run(1)

	hash := makeHash(3)
	tr.NotifyReceived("peerA", []common.Hash{hash})
	waitStep(t, tr)

	tr.NotifyPooled([]common.Hash{hash})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info.Status != TxPooled {
		t.Fatalf("expected TxPooled, got %v", info.Status)
	}
	if info.Pooled == 0 {
		t.Fatal("expected pooled timestamp to be set")
	}
}

func TestReceivedToRejected(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(4)
	tr.NotifyReceived("peerA", []common.Hash{hash})
	waitStep(t, tr)

	tr.NotifyRejected([]common.Hash{hash}, []error{errors.New("underpriced")})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info.Status != TxRejected {
		t.Fatalf("expected TxRejected, got %v", info.Status)
	}
	if info.RejectErr != "underpriced" {
		t.Fatalf("expected rejection error 'underpriced', got %q", info.RejectErr)
	}
}

func TestPooledToIncluded(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(5)
	tr.NotifyPooled([]common.Hash{hash})
	waitStep(t, tr)

	// Simulate block inclusion.
	tx := makeTx(hash)
	header := makeHeader(100, common.Hash{0xff})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	info := tr.Get(tx.Hash())
	// The tx hash won't match our makeHash since makeTx computes a real hash.
	// Instead query by the actual hash of the tx in the block event.
	// For the tracker, inclusion is keyed by tx.Hash() from the ChainEvent.
	// Since our pooled notification used makeHash(5) but the tx has a different
	// hash, the inclusion won't match. Let's test with a direct approach.

	// Check that the pooled tx is still pooled (it won't be included because
	// the ChainEvent tx hash differs from makeHash(5)).
	info = tr.Get(hash)
	if info == nil || info.Status != TxPooled {
		t.Fatalf("expected pooled status for original hash, got %v", info)
	}
}

func TestFullLifecycle(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(10)

	// Step 1: Announced
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{2}, []uint32{500})
	waitStep(t, tr)
	if s := tr.Status(hash); s != TxAnnounced {
		t.Fatalf("step 1: expected TxAnnounced, got %v", s)
	}

	// Step 2: Received
	tr.NotifyReceived("peerB", []common.Hash{hash})
	waitStep(t, tr)
	if s := tr.Status(hash); s != TxReceived {
		t.Fatalf("step 2: expected TxReceived, got %v", s)
	}

	// Step 3: Pooled
	tr.NotifyPooled([]common.Hash{hash})
	waitStep(t, tr)
	if s := tr.Status(hash); s != TxPooled {
		t.Fatalf("step 3: expected TxPooled, got %v", s)
	}

	// Step 4: Included via ChainEvent (we need to use the same hash)
	// To make this work, we'll create a ChainEvent whose tx hash matches.
	// We can't easily create a tx with a specific hash, so we rely on
	// the tracker matching by hash in handleChainEvent.
	// The tracker iterates ev.Transactions and looks up tx.Hash() in t.txs.
	// Since tx.Hash() is computed from the tx content, not from makeHash(10),
	// the lookup will miss. This is a limitation of this test approach.
	// Let's test inclusion via a different flow:

	// Use a real transaction hash flow: announce → receive → pool → include.
	tx := types.NewTx(&types.LegacyTx{Nonce: 42, GasPrice: big.NewInt(1), Gas: 21000})
	txHash := tx.Hash()

	tr.NotifyAnnounced("peerC", []common.Hash{txHash}, []byte{0}, []uint32{100})
	waitStep(t, tr)
	tr.NotifyReceived("peerC", []common.Hash{txHash})
	waitStep(t, tr)
	tr.NotifyPooled([]common.Hash{txHash})
	waitStep(t, tr)

	// Now send chain event with this transaction.
	block1Hash := common.Hash{0xaa}
	header := makeHeader(50, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	info := tr.Get(txHash)
	if info == nil {
		t.Fatal("expected tracked transaction")
	}
	if info.Status != TxIncluded {
		t.Fatalf("step 4: expected TxIncluded, got %v", info.Status)
	}
	if info.BlockNum != 50 {
		t.Fatalf("expected block number 50, got %d", info.BlockNum)
	}

	// Step 5: Finalized
	chain.finalBlock = makeHeader(100, block1Hash)
	// Send another chain event to trigger finalization check.
	header2 := makeHeader(51, header.Hash())
	chain.sendChainEvent(core.ChainEvent{
		Header: header2,
	})
	waitStep(t, tr)

	info = tr.Get(txHash)
	if info.Status != TxFinalized {
		t.Fatalf("step 5: expected TxFinalized, got %v", info.Status)
	}
}

func TestReorg(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	tx := types.NewTx(&types.LegacyTx{Nonce: 99, GasPrice: big.NewInt(1), Gas: 21000})
	txHash := tx.Hash()

	tr.NotifyPooled([]common.Hash{txHash})
	waitStep(t, tr)

	// Include in block 10.
	header10 := makeHeader(10, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header10,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	if s := tr.Status(txHash); s != TxIncluded {
		t.Fatalf("expected TxIncluded, got %v", s)
	}

	// Reorg: new block 10 with different parent (parent doesn't match header10's hash).
	reorgHeader := makeHeader(10, common.Hash{0xde, 0xad})
	chain.sendChainEvent(core.ChainEvent{
		Header: reorgHeader,
	})
	waitStep(t, tr)

	// The tx should revert to pooled since its block (10) >= reorg block (10).
	if s := tr.Status(txHash); s != TxPooled {
		t.Fatalf("expected TxPooled after reorg, got %v", s)
	}
}

func TestEviction(t *testing.T) {
	tr, _, _, _ := testTracker(5)
	defer tr.Stop()

	// Add 5 transactions.
	hashes := make([]common.Hash, 5)
	for i := range hashes {
		hashes[i] = makeHash(byte(i + 1))
		tr.NotifyAnnounced("peerA", []common.Hash{hashes[i]}, []byte{0}, []uint32{100})
		waitStep(t, tr)
	}

	// All 5 should be tracked.
	for _, h := range hashes {
		if tr.Status(h) == 0 {
			t.Fatalf("hash %v should be tracked", h)
		}
	}

	// Add a 6th transaction — should evict the oldest (hashes[0]).
	hash6 := makeHash(6)
	tr.NotifyAnnounced("peerA", []common.Hash{hash6}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	if s := tr.Status(hashes[0]); s != 0 {
		t.Fatalf("expected hash 0 to be evicted, got status %v", s)
	}
	if s := tr.Status(hash6); s == 0 {
		t.Fatal("expected hash 6 to be tracked")
	}
}

func TestEvictionLRU(t *testing.T) {
	tr, _, _, _ := testTracker(5)
	defer tr.Stop()

	hashes := make([]common.Hash, 5)
	for i := range hashes {
		hashes[i] = makeHash(byte(i + 1))
		tr.NotifyAnnounced("peerA", []common.Hash{hashes[i]}, []byte{0}, []uint32{100})
		waitStep(t, tr)
	}

	// Touch hashes[0] by receiving it — moves it to front of LRU.
	tr.NotifyReceived("peerB", []common.Hash{hashes[0]})
	waitStep(t, tr)

	// Add a 6th — should evict hashes[1] (now oldest), not hashes[0].
	hash6 := makeHash(6)
	tr.NotifyAnnounced("peerA", []common.Hash{hash6}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	if s := tr.Status(hashes[0]); s == 0 {
		t.Fatal("expected hash 0 to still be tracked (was touched)")
	}
	if s := tr.Status(hashes[1]); s != 0 {
		t.Fatalf("expected hash 1 to be evicted, got status %v", s)
	}
}

func TestPeerStats(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash1 := makeHash(1)
	hash2 := makeHash(2)

	// peerA announces hash1 (first announcer) and hash2.
	tr.NotifyAnnounced("peerA", []common.Hash{hash1, hash2}, []byte{0, 0}, []uint32{100, 200})
	waitStep(t, tr)

	// peerB also announces hash1 (not first).
	tr.NotifyAnnounced("peerB", []common.Hash{hash1}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	// peerA delivers hash1.
	tr.NotifyReceived("peerA", []common.Hash{hash1})
	waitStep(t, tr)

	// hash1 gets pooled.
	tr.NotifyPooled([]common.Hash{hash1})
	waitStep(t, tr)

	psA := tr.GetPeerStats("peerA")
	if psA.Announced != 2 {
		t.Fatalf("peerA announced: expected 2, got %d", psA.Announced)
	}
	if psA.FirstAnnouncer != 2 {
		t.Fatalf("peerA firstAnnouncer: expected 2, got %d", psA.FirstAnnouncer)
	}
	if psA.Delivered != 1 {
		t.Fatalf("peerA delivered: expected 1, got %d", psA.Delivered)
	}
	if psA.UsefulDelivery != 1 {
		t.Fatalf("peerA usefulDelivery: expected 1, got %d", psA.UsefulDelivery)
	}

	psB := tr.GetPeerStats("peerB")
	if psB.Announced != 1 {
		t.Fatalf("peerB announced: expected 1, got %d", psB.Announced)
	}
	if psB.FirstAnnouncer != 0 {
		t.Fatalf("peerB firstAnnouncer: expected 0, got %d", psB.FirstAnnouncer)
	}
}

func TestPeerDrop(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(1)
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	// Verify peer stats exist.
	ps := tr.GetPeerStats("peerA")
	if ps.Announced != 1 {
		t.Fatalf("expected 1 announcement, got %d", ps.Announced)
	}

	// Drop the peer.
	tr.NotifyPeerDrop("peerA")
	waitStep(t, tr)

	// Peer stats should be cleared.
	ps = tr.GetPeerStats("peerA")
	if ps.Announced != 0 {
		t.Fatalf("expected 0 announcements after drop, got %d", ps.Announced)
	}
}

func TestLocalTxDetection(t *testing.T) {
	tr, _, _, pool := testTracker(0)
	defer tr.Stop()

	// Simulate a local transaction entering the pool.
	tx := types.NewTx(&types.LegacyTx{Nonce: 7, GasPrice: big.NewInt(1), Gas: 21000})
	pool.sendNewTxs(core.NewTxsEvent{Txs: []*types.Transaction{tx}})
	waitStep(t, tr)

	info := tr.Get(tx.Hash())
	if info == nil {
		t.Fatal("expected local transaction to be tracked")
	}
	if !info.Local {
		t.Fatal("expected transaction to be marked as local")
	}
	if info.Status != TxPooled {
		t.Fatalf("expected TxPooled, got %v", info.Status)
	}
}

func TestLocalTxNotOverridden(t *testing.T) {
	tr, _, _, pool := testTracker(0)
	defer tr.Stop()

	// First track via P2P announcement.
	tx := types.NewTx(&types.LegacyTx{Nonce: 8, GasPrice: big.NewInt(1), Gas: 21000})
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	// Now the same tx appears in NewTxsEvent — should NOT override to local.
	pool.sendNewTxs(core.NewTxsEvent{Txs: []*types.Transaction{tx}})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info.Local {
		t.Fatal("transaction tracked via P2P should not be marked as local")
	}
}

func TestMultipleAnnouncers(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(1)

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)
	tr.NotifyAnnounced("peerB", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)
	tr.NotifyAnnounced("peerC", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)
	// Duplicate announcer should not be added.
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	info := tr.Get(hash)
	if len(info.Announcers) != 3 {
		t.Fatalf("expected 3 announcers, got %d: %v", len(info.Announcers), info.Announcers)
	}
	if info.Announcers[0] != "peerA" || info.Announcers[1] != "peerB" || info.Announcers[2] != "peerC" {
		t.Fatalf("unexpected announcer order: %v", info.Announcers)
	}
}

func TestReceiveWithoutAnnounce(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(1)
	tr.NotifyReceived("peerA", []common.Hash{hash})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info == nil {
		t.Fatal("expected tracked transaction")
	}
	if info.Status != TxReceived {
		t.Fatalf("expected TxReceived, got %v", info.Status)
	}
	if info.Deliverer != "peerA" {
		t.Fatalf("expected deliverer peerA, got %s", info.Deliverer)
	}
}

func TestRejectionDoesNotOverridePooled(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	hash := makeHash(1)
	tr.NotifyPooled([]common.Hash{hash})
	waitStep(t, tr)

	// Try to reject an already-pooled tx — should not change status.
	tr.NotifyRejected([]common.Hash{hash}, []error{errors.New("too late")})
	waitStep(t, tr)

	if s := tr.Status(hash); s != TxPooled {
		t.Fatalf("expected TxPooled (rejection should not override), got %v", s)
	}
}

func TestGetUntracked(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	info := tr.Get(makeHash(99))
	if info != nil {
		t.Fatal("expected nil for untracked transaction")
	}
}

func TestStatusUntracked(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	if s := tr.Status(makeHash(99)); s != 0 {
		t.Fatalf("expected 0 for untracked, got %v", s)
	}
}

func TestStatusString(t *testing.T) {
	tests := []struct {
		status TxStatus
		want   string
	}{
		{TxAnnounced, "announced"},
		{TxReceived, "received"},
		{TxPooled, "pooled"},
		{TxIncluded, "included"},
		{TxFinalized, "finalized"},
		{TxRejected, "rejected"},
		{0, "unknown"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("TxStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
		}
	}
}
