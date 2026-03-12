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
	"time"

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
	txsFeed     event.Feed
	removedFeed event.Feed
}

func (m *mockTxPool) SubscribeTransactions(ch chan<- core.NewTxsEvent, reorgs bool) event.Subscription {
	return m.txsFeed.Subscribe(ch)
}

func (m *mockTxPool) SubscribeRemovedTransactions(ch chan<- core.RemovedTxsEvent) event.Subscription {
	return m.removedFeed.Subscribe(ch)
}

func (m *mockTxPool) sendNewTxs(ev core.NewTxsEvent) {
	m.txsFeed.Send(ev)
}

func (m *mockTxPool) sendRemoved(hashes ...common.Hash) {
	m.removedFeed.Send(core.RemovedTxsEvent{Hashes: hashes})
}

func (m *mockTxPool) sendRemovedWithReasons(hashes []common.Hash, reasons []string) {
	m.removedFeed.Send(core.RemovedTxsEvent{Hashes: hashes, Reasons: reasons})
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

// drainEvents collects events from the channel until no event arrives for
// a short timeout. This accounts for the async emitLoop goroutine that
// forwards events from the event loop to the event.Feed.
func drainEvents(ch <-chan TxTrackerEvent) []TxTrackerEvent {
	var events []TxTrackerEvent
	for {
		select {
		case ev := <-ch:
			events = append(events, ev)
		case <-time.After(50 * time.Millisecond):
			return events
		}
	}
}

func makeHash(i byte) common.Hash {
	return common.Hash{i}
}

func makeTx(nonce uint64) *types.Transaction {
	return types.NewTx(&types.LegacyTx{Nonce: nonce, GasPrice: big.NewInt(1), Gas: 21000})
}

// poolAndWait notifies the tracker that hashes are pooled and waits for processing.
func poolAndWait(t *testing.T, tr *Tracker, hashes []common.Hash) {
	t.Helper()
	tr.NotifyPooled(hashes)
	waitStep(t, tr)
}

// dropAndWait sends a removal event for hash and waits for processing.
func dropAndWait(t *testing.T, tr *Tracker, pool *mockTxPool, hash common.Hash) {
	t.Helper()
	pool.sendRemoved(hash)
	waitStep(t, tr)
}

func makeHeader(num uint64, parent common.Hash) *types.Header {
	return &types.Header{
		Number:     new(big.Int).SetUint64(num),
		ParentHash: parent,
	}
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

func TestAnnouncedToRequested(t *testing.T) {
	tr, clock, _, _ := testTracker(0)
	defer tr.Stop()

	clock.Run(1)
	hash := makeHash(10)
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	tr.NotifyFetchRequested("peerA", []common.Hash{hash})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info.Status != TxRequested {
		t.Fatalf("expected TxRequested, got %v", info.Status)
	}
	if info.RequestedFrom != "peerA" {
		t.Fatalf("expected requestedFrom peerA, got %q", info.RequestedFrom)
	}
	if info.Requested.IsZero() {
		t.Fatal("expected requested timestamp to be set")
	}
}

func TestRequestedToReceived(t *testing.T) {
	tr, clock, _, _ := testTracker(0)
	defer tr.Stop()

	clock.Run(1)
	tx := makeTx(11)
	hash := tx.Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	tr.NotifyFetchRequested("peerA", []common.Hash{hash})
	waitStep(t, tr)

	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info.Status != TxReceived {
		t.Fatalf("expected TxReceived, got %v", info.Status)
	}
	if info.Requested.IsZero() {
		t.Fatal("expected requested timestamp to be preserved")
	}
	if info.Received.IsZero() {
		t.Fatal("expected received timestamp to be set")
	}
}

func TestFetchRequestedIgnoredIfNotAnnounced(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	// Fetch requested for untracked tx should be a no-op.
	hash := makeHash(12)
	tr.NotifyFetchRequested("peerA", []common.Hash{hash})
	waitStep(t, tr)

	if info := tr.Get(hash); info != nil {
		t.Fatal("expected untracked tx to remain untracked")
	}

	// Fetch requested for already-received tx should not regress status.
	tx2 := makeTx(13)
	hash2 := tx2.Hash()
	tr.NotifyReceived("peerA", []*types.Transaction{tx2})
	waitStep(t, tr)

	tr.NotifyFetchRequested("peerB", []common.Hash{hash2})
	waitStep(t, tr)

	if s := tr.Status(hash2); s != TxReceived {
		t.Fatalf("expected TxReceived (no regression), got %v", s)
	}
}

func TestAnnouncedToReceived(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	tx := makeTx(2)
	hash := tx.Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{200})
	waitStep(t, tr)

	tr.NotifyReceived("peerB", []*types.Transaction{tx})
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

	tx := makeTx(3)
	hash := tx.Hash()
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	waitStep(t, tr)

	tr.NotifyPooled([]common.Hash{hash})
	waitStep(t, tr)

	info := tr.Get(hash)
	if info.Status != TxPooled {
		t.Fatalf("expected TxPooled, got %v", info.Status)
	}
	if info.Pooled.IsZero() {
		t.Fatal("expected pooled timestamp to be set")
	}
}

func TestReceivedToRejected(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	tx := makeTx(4)
	hash := tx.Hash()
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
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

func TestFullLifecycle(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	// Use a real tx so that tx.Hash() in ChainEvent matches the tracked hash.
	tx := makeTx(42)
	txHash := tx.Hash()

	// Step 1: Announced
	tr.NotifyAnnounced("peerA", []common.Hash{txHash}, []byte{2}, []uint32{500})
	waitStep(t, tr)
	if s := tr.Status(txHash); s != TxAnnounced {
		t.Fatalf("step 1: expected TxAnnounced, got %v", s)
	}

	// Step 2: Received
	tr.NotifyReceived("peerB", []*types.Transaction{tx})
	waitStep(t, tr)
	if s := tr.Status(txHash); s != TxReceived {
		t.Fatalf("step 2: expected TxReceived, got %v", s)
	}

	// Step 3: Pooled
	tr.NotifyPooled([]common.Hash{txHash})
	waitStep(t, tr)
	if s := tr.Status(txHash); s != TxPooled {
		t.Fatalf("step 3: expected TxPooled, got %v", s)
	}

	// Step 4: Included
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
		t.Fatalf("expected TxIncluded, got %v", info.Status)
	}
	if info.BlockNum != 50 {
		t.Fatalf("expected block number 50, got %d", info.BlockNum)
	}

	// Step 5: Finalized
	chain.finalBlock = makeHeader(100, common.Hash{0xaa})
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

	tx := makeTx(99)
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

	// Use real txs so we can call NotifyReceived with matching hashes.
	txs := make([]*types.Transaction, 5)
	hashes := make([]common.Hash, 5)
	for i := range txs {
		txs[i] = makeTx(uint64(i + 1))
		hashes[i] = txs[i].Hash()
		tr.NotifyAnnounced("peerA", []common.Hash{hashes[i]}, []byte{0}, []uint32{100})
		waitStep(t, tr)
	}

	// Touch hashes[0] by receiving it — moves it to front of LRU.
	tr.NotifyReceived("peerB", []*types.Transaction{txs[0]})
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

	tx1 := makeTx(1)
	hash1 := tx1.Hash()
	hash2 := makeHash(2)

	// peerA announces hash1 (first announcer) and hash2.
	tr.NotifyAnnounced("peerA", []common.Hash{hash1, hash2}, []byte{0, 0}, []uint32{100, 200})
	waitStep(t, tr)

	// peerB also announces hash1 (not first).
	tr.NotifyAnnounced("peerB", []common.Hash{hash1}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	// peerA delivers hash1.
	tr.NotifyReceived("peerA", []*types.Transaction{tx1})
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
	tx := makeTx(7)
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
	tx := makeTx(8)
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

	tx := makeTx(1)
	hash := tx.Hash()
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
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

func TestRejectedToIncluded(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	tx := makeTx(77)
	txHash := tx.Hash()

	// Receive and reject the transaction (e.g., underpriced locally).
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	waitStep(t, tr)
	tr.NotifyRejected([]common.Hash{txHash}, []error{errors.New("underpriced")})
	waitStep(t, tr)

	if s := tr.Status(txHash); s != TxRejected {
		t.Fatalf("expected TxRejected, got %v", s)
	}

	// The transaction is mined by another node and appears in a canonical block.
	header := makeHeader(200, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	info := tr.Get(txHash)
	if info.Status != TxIncluded {
		t.Fatalf("expected TxIncluded after chain event, got %v", info.Status)
	}
	if info.BlockNum != 200 {
		t.Fatalf("expected block number 200, got %d", info.BlockNum)
	}
}

func TestChainOnlyTracking(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	// A transaction that was never announced, received, or pooled locally —
	// it appears for the first time in a canonical block.
	tx := makeTx(200)
	txHash := tx.Hash()

	header := makeHeader(300, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	info := tr.Get(txHash)
	if info == nil {
		t.Fatal("expected chain-only transaction to be tracked")
	}
	if info.Status != TxIncluded {
		t.Fatalf("expected TxIncluded, got %v", info.Status)
	}
	if info.BlockNum != 300 {
		t.Fatalf("expected block number 300, got %d", info.BlockNum)
	}
	if info.TxType != 0 {
		t.Fatalf("expected tx type 0, got %d", info.TxType)
	}
}

func TestChainOnlyFinalization(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	// Chain-only tx gets included then finalized.
	tx := makeTx(201)
	txHash := tx.Hash()

	header1 := makeHeader(400, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header1,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	// Set finalized block past 400, send another chain event to trigger check.
	chain.finalBlock = makeHeader(500, common.Hash{0xbb})
	header2 := makeHeader(401, header1.Hash())
	chain.sendChainEvent(core.ChainEvent{Header: header2})
	waitStep(t, tr)

	info := tr.Get(txHash)
	if info.Status != TxFinalized {
		t.Fatalf("expected TxFinalized, got %v", info.Status)
	}
}

func TestShutdownDuringQuery(t *testing.T) {
	tr, _, _, _ := testTracker(0)

	// Seed one record so Get returns non-nil under normal operation.
	hash := makeHash(1)
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	// Stop the tracker, then query — should return zero values, not hang.
	tr.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if info := tr.Get(hash); info != nil {
			t.Error("expected nil from Get after Stop")
		}
		if s := tr.Status(hash); s != 0 {
			t.Errorf("expected 0 from Status after Stop, got %v", s)
		}
		if ps := tr.GetPeerStats("peerA"); ps.Announced != 0 {
			t.Errorf("expected zero PeerStats after Stop, got %+v", ps)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("query methods hung after Stop (deadlock)")
	}
}

func TestReceiveRepairsLocalFlag(t *testing.T) {
	tr, _, _, pool := testTracker(0)
	defer tr.Stop()

	tx := makeTx(55)
	hash := tx.Hash()

	// Simulate the race: NewTxsEvent arrives before the receive event.
	pool.sendNewTxs(core.NewTxsEvent{Txs: []*types.Transaction{tx}})
	waitStep(t, tr)

	info := tr.Get(hash)
	if !info.Local {
		t.Fatal("expected local=true before receive repair")
	}

	// Now the receive event arrives from the peer.
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	waitStep(t, tr)

	info = tr.Get(hash)
	if info.Local {
		t.Fatal("expected local=false after receive repair")
	}
	if info.Deliverer != "peerA" {
		t.Fatalf("expected deliverer peerA, got %q", info.Deliverer)
	}
}

func TestPoolDropDetection(t *testing.T) {
	tr, _, _, pool := testTracker(0)
	defer tr.Stop()

	hash := makeHash(1)

	poolAndWait(t, tr, []common.Hash{hash})

	if s := tr.Status(hash); s != TxPooled {
		t.Fatalf("expected TxPooled, got %v", s)
	}

	dropAndWait(t, tr, pool, hash)

	if s := tr.Status(hash); s != TxDropped {
		t.Fatalf("expected TxDropped, got %v", s)
	}

	info := tr.Get(hash)
	if info.Dropped.IsZero() {
		t.Fatal("expected non-zero Dropped timestamp")
	}
}

func TestDroppedToIncluded(t *testing.T) {
	tr, _, chain, pool := testTracker(0)
	defer tr.Stop()

	tx := makeTx(200)
	hash := tx.Hash()

	poolAndWait(t, tr, []common.Hash{hash})
	dropAndWait(t, tr, pool, hash)

	if s := tr.Status(hash); s != TxDropped {
		t.Fatalf("expected TxDropped, got %v", s)
	}

	// Now include it in a block.
	header := makeHeader(50, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	if s := tr.Status(hash); s != TxIncluded {
		t.Fatalf("expected TxIncluded, got %v", s)
	}
}

func TestDroppedToPooled(t *testing.T) {
	tr, _, _, pool := testTracker(0)
	defer tr.Stop()

	hash := makeHash(2)

	poolAndWait(t, tr, []common.Hash{hash})
	dropAndWait(t, tr, pool, hash)

	if s := tr.Status(hash); s != TxDropped {
		t.Fatalf("expected TxDropped, got %v", s)
	}

	// Re-add to pool.
	poolAndWait(t, tr, []common.Hash{hash})

	if s := tr.Status(hash); s != TxPooled {
		t.Fatalf("expected TxPooled after re-add, got %v", s)
	}
}

func TestDroppedToRequested(t *testing.T) {
	tr, _, _, pool := testTracker(0)
	defer tr.Stop()

	hash := makeHash(3)

	poolAndWait(t, tr, []common.Hash{hash})
	dropAndWait(t, tr, pool, hash)

	if s := tr.Status(hash); s != TxDropped {
		t.Fatalf("expected TxDropped, got %v", s)
	}

	// Re-request (peer re-announced, fetcher requesting again).
	tr.NotifyFetchRequested("peerC", []common.Hash{hash})
	waitStep(t, tr)

	if s := tr.Status(hash); s != TxRequested {
		t.Fatalf("expected TxRequested after re-request, got %v", s)
	}
}

func TestEventFeed(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	// Subscribe to events.
	eventCh := make(chan TxTrackerEvent, 64)
	sub := tr.SubscribeEvents(eventCh)
	defer sub.Unsubscribe()

	// Use a real tx so ChainEvent hash matches.
	tx := makeTx(100)
	txHash := tx.Hash()

	// Step 1: Announced
	tr.NotifyAnnounced("peerA", []common.Hash{txHash}, []byte{2}, []uint32{500})
	waitStep(t, tr)

	// Step 2: Received
	tr.NotifyReceived("peerB", []*types.Transaction{tx})
	waitStep(t, tr)

	// Step 3: Pooled
	tr.NotifyPooled([]common.Hash{txHash})
	waitStep(t, tr)

	// Step 4: Included
	header := makeHeader(50, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	// Step 5: Finalized
	chain.finalBlock = makeHeader(100, common.Hash{0xaa})
	header2 := makeHeader(51, header.Hash())
	chain.sendChainEvent(core.ChainEvent{Header: header2})
	waitStep(t, tr)

	// Collect all events (with timeout for async emit delivery).
	events := drainEvents(eventCh)

	// Expect: Announced, Received, Pooled, Included, Finalized
	expected := []struct {
		old, new TxStatus
	}{
		{0, TxAnnounced},
		{TxAnnounced, TxReceived},
		{TxReceived, TxPooled},
		{TxPooled, TxIncluded},
		{TxIncluded, TxFinalized},
	}
	if len(events) != len(expected) {
		t.Fatalf("expected %d events, got %d: %+v", len(expected), len(events), events)
	}
	for i, exp := range expected {
		if events[i].OldStatus != exp.old || events[i].NewStatus != exp.new {
			t.Errorf("event %d: expected (%v→%v), got (%v→%v)", i, exp.old, exp.new, events[i].OldStatus, events[i].NewStatus)
		}
		if events[i].TxHash != txHash {
			t.Errorf("event %d: unexpected hash %v", i, events[i].TxHash)
		}
	}
	// Verify peer field on the first two events.
	if events[0].Peer != "peerA" {
		t.Errorf("announce event: expected peer peerA, got %q", events[0].Peer)
	}
	if events[1].Peer != "peerB" {
		t.Errorf("receive event: expected peer peerB, got %q", events[1].Peer)
	}
}

func TestEventFeedReorg(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	eventCh := make(chan TxTrackerEvent, 64)
	sub := tr.SubscribeEvents(eventCh)
	defer sub.Unsubscribe()

	tx := makeTx(101)
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

	// Reorg: new block 10 with different parent.
	reorgHeader := makeHeader(10, common.Hash{0xde, 0xad})
	chain.sendChainEvent(core.ChainEvent{Header: reorgHeader})
	waitStep(t, tr)

	// Drain events (with timeout for async emit delivery).
	events := drainEvents(eventCh)

	// Find the reorg event (TxIncluded → TxPooled).
	found := false
	for _, ev := range events {
		if ev.TxHash == txHash && ev.OldStatus == TxIncluded && ev.NewStatus == TxPooled {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected reorg event (TxIncluded→TxPooled), events: %+v", events)
	}
}

func TestStatusString(t *testing.T) {
	tests := []struct {
		status TxStatus
		want   string
	}{
		{TxAnnounced, "announced"},
		{TxRequested, "requested"},
		{TxReceived, "received"},
		{TxPooled, "pooled"},
		{TxIncluded, "included"},
		{TxFinalized, "finalized"},
		{TxRejected, "rejected"},
		{TxDropped, "dropped"},
		{0, "unknown"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("TxStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
		}
	}
}

func TestGetStats(t *testing.T) {
	tr, _, _, _ := testTracker(5)
	defer tr.Stop()

	// Add 5 announced transactions.
	hashes := make([]common.Hash, 5)
	for i := range hashes {
		hashes[i] = makeHash(byte(i + 1))
		tr.NotifyAnnounced("peerA", []common.Hash{hashes[i]}, []byte{0}, []uint32{100})
		waitStep(t, tr)
	}

	stats := tr.GetStats()
	if stats.Total != 5 {
		t.Fatalf("expected Total=5, got %d", stats.Total)
	}
	if stats.Capacity != 5 {
		t.Fatalf("expected Capacity=5, got %d", stats.Capacity)
	}
	if stats.Evicted.Total != 0 {
		t.Fatalf("expected no evictions yet, got %d", stats.Evicted.Total)
	}

	// Add 6th → evicts oldest (announced state).
	tr.NotifyAnnounced("peerA", []common.Hash{makeHash(6)}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	stats = tr.GetStats()
	if stats.Total != 5 {
		t.Fatalf("expected Total=5 after eviction, got %d", stats.Total)
	}
	if stats.Evicted.Announced != 1 {
		t.Fatalf("expected Evicted.Announced=1, got %d", stats.Evicted.Announced)
	}
	if stats.Evicted.Total != 1 {
		t.Fatalf("expected Evicted.Total=1, got %d", stats.Evicted.Total)
	}

	// Per-state eviction: capacity-1 tracker, pool a tx, then evict it.
	tr2, _, _, _ := testTracker(1)
	defer tr2.Stop()

	h := makeHash(0x10)
	poolAndWait(t, tr2, []common.Hash{h})

	// Adding a second tx evicts the pooled one.
	tr2.NotifyAnnounced("peerA", []common.Hash{makeHash(0x11)}, []byte{0}, []uint32{100})
	waitStep(t, tr2)

	stats2 := tr2.GetStats()
	if stats2.Evicted.Pooled != 1 {
		t.Fatalf("expected Evicted.Pooled=1, got %d", stats2.Evicted.Pooled)
	}
}

func TestDropReason(t *testing.T) {
	tr, _, _, pool := testTracker(0)
	defer tr.Stop()

	eventCh := make(chan TxTrackerEvent, 64)
	sub := tr.SubscribeEvents(eventCh)
	defer sub.Unsubscribe()

	// Pool and drop with a reason.
	hash1 := makeHash(1)
	hash2 := makeHash(2)
	poolAndWait(t, tr, []common.Hash{hash1, hash2})

	pool.sendRemovedWithReasons([]common.Hash{hash1, hash2}, []string{"underpriced"})
	waitStep(t, tr)

	// hash1 should have the reason, hash2 should have empty reason (Reasons shorter than Hashes).
	info1 := tr.Get(hash1)
	if info1.DropReason != "underpriced" {
		t.Fatalf("expected DropReason=%q, got %q", "underpriced", info1.DropReason)
	}
	info2 := tr.Get(hash2)
	if info2.DropReason != "" {
		t.Fatalf("expected empty DropReason for hash2, got %q", info2.DropReason)
	}

	// Verify event feed carries the drop reason.
	events := drainEvents(eventCh)
	var dropEvents []TxTrackerEvent
	for _, ev := range events {
		if ev.NewStatus == TxDropped {
			dropEvents = append(dropEvents, ev)
		}
	}
	if len(dropEvents) != 2 {
		t.Fatalf("expected 2 drop events, got %d", len(dropEvents))
	}
	// Find the event for hash1.
	for _, ev := range dropEvents {
		if ev.TxHash == hash1 && ev.DropReason != "underpriced" {
			t.Fatalf("expected drop event DropReason=%q for hash1, got %q", "underpriced", ev.DropReason)
		}
		if ev.TxHash == hash2 && ev.DropReason != "" {
			t.Fatalf("expected empty DropReason in event for hash2, got %q", ev.DropReason)
		}
	}
}

func TestPeerStatsIncludedFinalized(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	tx := makeTx(50)
	hash := tx.Hash()

	// peerA announces, peerB delivers.
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)
	tr.NotifyReceived("peerB", []*types.Transaction{tx})
	waitStep(t, tr)
	tr.NotifyPooled([]common.Hash{hash})
	waitStep(t, tr)

	// Include in block.
	header := makeHeader(10, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	psB := tr.GetPeerStats("peerB")
	if psB.Included != 1 {
		t.Fatalf("peerB Included: expected 1, got %d", psB.Included)
	}
	// Announcer should not get inclusion credit.
	psA := tr.GetPeerStats("peerA")
	if psA.Included != 0 {
		t.Fatalf("peerA Included: expected 0, got %d", psA.Included)
	}

	// Finalize.
	chain.finalBlock = makeHeader(100, common.Hash{0xaa})
	header2 := makeHeader(11, header.Hash())
	chain.sendChainEvent(core.ChainEvent{Header: header2})
	waitStep(t, tr)

	psB = tr.GetPeerStats("peerB")
	if psB.Finalized != 1 {
		t.Fatalf("peerB Finalized: expected 1, got %d", psB.Finalized)
	}
	psA = tr.GetPeerStats("peerA")
	if psA.Finalized != 0 {
		t.Fatalf("peerA Finalized: expected 0, got %d", psA.Finalized)
	}
}

func TestReorgPeerStats(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	tx := makeTx(60)
	hash := tx.Hash()

	// peerA delivers and tx gets included.
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	waitStep(t, tr)
	tr.NotifyPooled([]common.Hash{hash})
	waitStep(t, tr)

	header := makeHeader(10, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	ps := tr.GetPeerStats("peerA")
	if ps.Included != 1 {
		t.Fatalf("peerA Included before reorg: expected 1, got %d", ps.Included)
	}

	// Reorg: new block 10 with different parent.
	reorgHeader := makeHeader(10, common.Hash{0xde, 0xad})
	chain.sendChainEvent(core.ChainEvent{Header: reorgHeader})
	waitStep(t, tr)

	ps = tr.GetPeerStats("peerA")
	if ps.Included != 0 {
		t.Fatalf("peerA Included after reorg: expected 0, got %d", ps.Included)
	}
}

func TestGetAllPeerStats(t *testing.T) {
	tr, _, _, _ := testTracker(0)
	defer tr.Stop()

	tx := makeTx(70)
	hash := tx.Hash()

	// peerA and peerB both announce.
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)
	tr.NotifyAnnounced("peerB", []common.Hash{hash}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	// peerA delivers.
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	waitStep(t, tr)

	all := tr.GetAllPeerStats()
	if len(all) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(all))
	}
	if all["peerA"].Announced != 1 || all["peerA"].Delivered != 1 {
		t.Fatalf("peerA stats unexpected: %+v", all["peerA"])
	}
	if all["peerB"].Announced != 1 || all["peerB"].Delivered != 0 {
		t.Fatalf("peerB stats unexpected: %+v", all["peerB"])
	}

	// Cross-check against individual calls.
	if all["peerA"] != tr.GetPeerStats("peerA") {
		t.Fatal("GetAllPeerStats peerA != GetPeerStats peerA")
	}
	if all["peerB"] != tr.GetPeerStats("peerB") {
		t.Fatal("GetAllPeerStats peerB != GetPeerStats peerB")
	}

	// Drop peerB and verify it's gone.
	tr.NotifyPeerDrop("peerB")
	waitStep(t, tr)

	all = tr.GetAllPeerStats()
	if _, ok := all["peerB"]; ok {
		t.Fatal("peerB should be removed after NotifyPeerDrop")
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 peer after drop, got %d", len(all))
	}
}

func TestRemoveAfterInclusion(t *testing.T) {
	tr, _, chain, pool := testTracker(0)
	defer tr.Stop()

	tx := makeTx(80)
	hash := tx.Hash()

	// Pool and include.
	poolAndWait(t, tr, []common.Hash{hash})
	header := makeHeader(20, common.Hash{})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	if s := tr.Status(hash); s != TxIncluded {
		t.Fatalf("expected TxIncluded, got %v", s)
	}

	// Pool removal event after inclusion should be ignored.
	pool.sendRemoved(hash)
	waitStep(t, tr)

	if s := tr.Status(hash); s != TxIncluded {
		t.Fatalf("expected status to remain TxIncluded after removal event, got %v", s)
	}
}

func TestChainOnlySkipFinalized(t *testing.T) {
	tr, _, chain, _ := testTracker(0)
	defer tr.Stop()

	// Pre-seed finalized block at 50.
	chain.finalBlock = makeHeader(50, common.Hash{})
	// Trigger checkFinalization so the tracker picks up lastFinalNum.
	dummyHeader := makeHeader(51, common.Hash{0x01})
	chain.sendChainEvent(core.ChainEvent{Header: dummyHeader})
	waitStep(t, tr)

	// Now send a chain event with a new tx in block 40 (below finalized).
	tx := makeTx(90)
	hash := tx.Hash()
	header40 := makeHeader(40, common.Hash{0x02})
	chain.sendChainEvent(core.ChainEvent{
		Header:       header40,
		Transactions: []*types.Transaction{tx},
	})
	waitStep(t, tr)

	// The tx should NOT be tracked because its block is already finalized.
	if s := tr.Status(hash); s != 0 {
		t.Fatalf("expected tx in finalized block to be skipped, got status %v", s)
	}
}

func TestShutdownGetStatsAndAllPeerStats(t *testing.T) {
	tr, _, _, _ := testTracker(0)

	// Add a peer so there's state to query.
	tr.NotifyAnnounced("peerA", []common.Hash{makeHash(1)}, []byte{0}, []uint32{100})
	waitStep(t, tr)

	tr.Stop()

	// GetStats should return zero value after shutdown.
	stats := tr.GetStats()
	if stats.Total != 0 || stats.Capacity != 0 {
		t.Fatalf("expected zero TrackerStats after Stop, got %+v", stats)
	}

	// GetAllPeerStats should return nil after shutdown.
	all := tr.GetAllPeerStats()
	if all != nil {
		t.Fatalf("expected nil AllPeerStats after Stop, got %+v", all)
	}
}
