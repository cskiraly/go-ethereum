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
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/trie"
)

// mockChain implements the Chain interface for testing.
//
// Blocks are stored by hash to exercise the reorg-safe lookup path in
// tracker.handleChainHead (which calls GetBlock(hash, number)). A separate
// canonicalByNum index maps each height to its canonical block hash, used
// by GetCanonicalHash (the finalization path's orphan check).
type mockChain struct {
	mu             sync.Mutex
	headFeed       event.Feed
	blocksByHash   map[common.Hash]*types.Block
	canonicalByNum map[uint64]common.Hash
	finalNum       uint64
}

func newMockChain() *mockChain {
	return &mockChain{
		blocksByHash:   make(map[common.Hash]*types.Block),
		canonicalByNum: make(map[uint64]common.Hash),
	}
}

func (c *mockChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return c.headFeed.Subscribe(ch)
}

func (c *mockChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.blocksByHash[hash]
}

func (c *mockChain) GetCanonicalHash(number uint64) common.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.canonicalByNum[number]
}

func (c *mockChain) CurrentFinalBlock() *types.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalNum == 0 {
		return nil
	}
	return &types.Header{Number: new(big.Int).SetUint64(c.finalNum)}
}

// addBlock adds a canonical block at the given height. If a canonical
// block already exists at num-1, its hash is used as the new block's
// parent so existing tests get proper chain linkage automatically.
// (Pre-A4, mockChain didn't bother with parent hashes; the tracker
// now uses ParentHash for reorg detection so the linkage matters.)
func (c *mockChain) addBlock(num uint64, txs []*types.Transaction) *types.Block {
	c.mu.Lock()
	var parentHash common.Hash
	if num > 0 {
		if h, ok := c.canonicalByNum[num-1]; ok {
			parentHash = h
		}
	}
	c.mu.Unlock()
	return c.addBlockAtHeightWithParent(num, num, parentHash, txs, true)
}

// addBlockAtHeightWithParent is the underlying constructor used by
// addBlock and addBlockAtHeight; it lets tests set a specific parent
// hash (or none, by passing common.Hash{}).
func (c *mockChain) addBlockAtHeightWithParent(num, salt uint64, parent common.Hash, txs []*types.Transaction, canonical bool) *types.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	header := &types.Header{
		Number:     new(big.Int).SetUint64(num),
		ParentHash: parent,
		Extra:      big.NewInt(int64(salt)).Bytes(),
	}
	block := types.NewBlock(header, &types.Body{Transactions: txs}, nil, trie.NewListHasher())
	c.blocksByHash[block.Hash()] = block
	if canonical {
		c.canonicalByNum[num] = block.Hash()
	}
	return block
}

// addBlockWithTime adds a canonical block at the given height carrying the
// given header timestamp, so tests can exercise the pre-slot credit gate
// (which compares a tx's pool-acceptance wall-clock against block.Time()).
func (c *mockChain) addBlockWithTime(num, blockTime uint64, txs []*types.Transaction) *types.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	header := &types.Header{
		Number:     new(big.Int).SetUint64(num),
		ParentHash: c.canonicalByNum[num-1],
		Time:       blockTime,
	}
	block := types.NewBlock(header, &types.Body{Transactions: txs}, nil, trie.NewListHasher())
	c.blocksByHash[block.Hash()] = block
	c.canonicalByNum[num] = block.Hash()
	return block
}

// addBlockAtHeight adds a block at the given height. The salt parameter
// ensures distinct block hashes for two blocks at the same height. If
// canonical is true, the block becomes the canonical block for that height.
func (c *mockChain) addBlockAtHeight(num, salt uint64, txs []*types.Transaction, canonical bool) *types.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	header := &types.Header{
		Number: new(big.Int).SetUint64(num),
		Extra:  big.NewInt(int64(salt)).Bytes(),
	}
	block := types.NewBlock(header, &types.Body{Transactions: txs}, nil, trie.NewListHasher())
	c.blocksByHash[block.Hash()] = block
	if canonical {
		c.canonicalByNum[num] = block.Hash()
	}
	return block
}

// addChild builds a block at parent.Num+1 with ParentHash = parent.Hash().
// `salt` distinguishes siblings (e.g. two competing forks at the same
// height). canonical controls whether the canonicalByNum index points
// at this block.
func (c *mockChain) addChild(parent *types.Block, salt uint64, txs []*types.Transaction, canonical bool) *types.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	header := &types.Header{
		Number:     new(big.Int).SetUint64(parent.NumberU64() + 1),
		ParentHash: parent.Hash(),
		Extra:      big.NewInt(int64(salt)).Bytes(),
	}
	block := types.NewBlock(header, &types.Body{Transactions: txs}, nil, trie.NewListHasher())
	c.blocksByHash[block.Hash()] = block
	if canonical {
		c.canonicalByNum[block.NumberU64()] = block.Hash()
	}
	return block
}

func (c *mockChain) setFinalBlock(num uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finalNum = num
}

// sendHead emits a chain head event for the canonical block at the given height.
func (c *mockChain) sendHead(num uint64) {
	c.mu.Lock()
	hash := c.canonicalByNum[num]
	block := c.blocksByHash[hash]
	c.mu.Unlock()
	if block == nil {
		panic("sendHead: no canonical block at height")
	}
	c.headFeed.Send(core.ChainHeadEvent{Header: block.Header()})
}

// sendHeadBlock emits a chain head event for the given block (may be
// non-canonical). Used for reorg tests.
func (c *mockChain) sendHeadBlock(block *types.Block) {
	c.headFeed.Send(core.ChainHeadEvent{Header: block.Header()})
}

func hashTxs(txs []*types.Transaction) []common.Hash {
	hashes := make([]common.Hash, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash()
	}
	return hashes
}

func makeTx(nonce uint64) *types.Transaction {
	return types.NewTx(&types.LegacyTx{Nonce: nonce, GasPrice: big.NewInt(1), Gas: 21000})
}

// mockConsumer captures every StatsConsumer invocation so tests can
// assert on the signals the tracker emits.
type mockConsumer struct {
	mu      sync.Mutex
	signals []signal
	// Per-peer counts for the kind-specific hooks.
	announced map[string]int
	delivered map[string]int
	accepted  map[string]int
	rejected  map[string]int
	dropped   map[string]int
}

type signal struct {
	inclusions, finalized map[string]int
}

func (c *mockConsumer) NotifyBlock(inclusions, finalized map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Deep-copy so tests inspecting older signals aren't tripped up by
	// later iterations mutating the same map (they don't today, but
	// this keeps the assertion model simple).
	in := make(map[string]int, len(inclusions))
	for k, v := range inclusions {
		in[k] = v
	}
	fn := make(map[string]int, len(finalized))
	for k, v := range finalized {
		fn[k] = v
	}
	c.signals = append(c.signals, signal{in, fn})
}

func (c *mockConsumer) NotifyAnnounced(peer string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.announced == nil {
		c.announced = make(map[string]int)
	}
	c.announced[peer] += count
}

func (c *mockConsumer) NotifyDelivered(peer string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.delivered == nil {
		c.delivered = make(map[string]int)
	}
	c.delivered[peer] += count
}

func (c *mockConsumer) NotifyAccepted(peer string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.accepted == nil {
		c.accepted = make(map[string]int)
	}
	c.accepted[peer] += count
}

func (c *mockConsumer) NotifyRejected(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rejected == nil {
		c.rejected = make(map[string]int)
	}
	c.rejected[peer]++
}

func (c *mockConsumer) NotifyDropped(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropped == nil {
		c.dropped = make(map[string]int)
	}
	c.dropped[peer]++
}

func (c *mockConsumer) last() signal {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.signals) == 0 {
		return signal{}
	}
	return c.signals[len(c.signals)-1]
}

// waitStep blocks until the tracker has processed one event.
func waitStep(t *testing.T, tr *Tracker) {
	t.Helper()
	select {
	case <-tr.step:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for tracker step")
	}
}

// TestNotifyAcceptedRecordsMapping verifies the tx-lifecycle surface:
// NotifyAccepted records tx→peer mappings in insertion order, with
// first-deliverer-wins semantics on duplicates.
func TestNotifyAcceptedRecordsMapping(t *testing.T) {
	tr := New()

	txs := []*types.Transaction{makeTx(1), makeTx(2), makeTx(3)}
	hashes := hashTxs(txs)
	tr.NotifyAccepted("peerA", hashes)

	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.txs) != 3 {
		t.Fatalf("expected 3 tracked txs, got %d", len(tr.txs))
	}
	if len(tr.order) != 3 {
		t.Fatalf("expected order length 3, got %d", len(tr.order))
	}
	for i, h := range hashes {
		ti := tr.txs[h]
		if ti == nil {
			t.Fatalf("tx %d: expected TxInfo for %x, got nil", i, h)
		}
		if ti.Deliverer != "peerA" {
			t.Fatalf("tx %d: expected deliverer=peerA, got %q", i, ti.Deliverer)
		}
		if tr.order[i] != h {
			t.Fatalf("order[%d] mismatch", i)
		}
	}
}

// TestNotifyAcceptedFirstDelivererWins verifies duplicate accepts
// preserve the original deliverer.
func TestNotifyAcceptedFirstDelivererWins(t *testing.T) {
	tr := New()
	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})
	tr.NotifyAccepted("peerB", []common.Hash{tx.Hash()})

	tr.mu.Lock()
	defer tr.mu.Unlock()
	ti := tr.txs[tx.Hash()]
	if ti == nil {
		t.Fatalf("expected TxInfo for hash, got nil")
	}
	if ti.Deliverer != "peerA" {
		t.Fatalf("expected first deliverer peerA to win, got %q", ti.Deliverer)
	}
	if len(tr.order) != 1 {
		t.Fatalf("expected single order entry, got %d", len(tr.order))
	}
}

// TestHandleChainHeadEmitsInclusions verifies the tracker emits a
// correct per-peer inclusion map to its consumer when a head block
// contains tracked transactions.
func TestHandleChainHeadEmitsInclusions(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx1, tx2 := makeTx(1), makeTx(2)
	tr.NotifyAccepted("peerA", []common.Hash{tx1.Hash()})
	tr.NotifyAccepted("peerB", []common.Hash{tx2.Hash()})

	chain.addBlock(1, []*types.Transaction{tx1, tx2})
	chain.sendHead(1)
	waitStep(t, tr)

	sig := consumer.last()
	if sig.inclusions["peerA"] != 1 {
		t.Errorf("peerA inclusions: got %d, want 1", sig.inclusions["peerA"])
	}
	if sig.inclusions["peerB"] != 1 {
		t.Errorf("peerB inclusions: got %d, want 1", sig.inclusions["peerB"])
	}
	if len(sig.finalized) != 0 {
		t.Errorf("expected empty finalized map, got %v", sig.finalized)
	}
}

// TestHandleChainHeadEmptyBlock verifies an empty head block emits an
// empty inclusion map (so peerstats can decay all known peers).
func TestHandleChainHeadEmptyBlock(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	chain.addBlock(1, nil)
	chain.sendHead(1)
	waitStep(t, tr)

	sig := consumer.last()
	if len(sig.inclusions) != 0 {
		t.Errorf("expected empty inclusions, got %v", sig.inclusions)
	}
}

// TestHandleChainHeadEmitsFinalization verifies that when finalization
// advances, the consumer receives per-peer finalization credits
// accumulated over the newly-finalized range.
func TestHandleChainHeadEmitsFinalization(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	// Include in block 1, not yet finalized.
	chain.addBlock(1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)

	if credits := consumer.last().finalized["peerA"]; credits != 0 {
		t.Fatalf("expected no finalization credits before finalization, got %d", credits)
	}

	// Finalize block 1; next head triggers the finalization scan.
	chain.setFinalBlock(1)
	chain.addBlock(2, nil)
	chain.sendHead(2)
	waitStep(t, tr)

	if credits := consumer.last().finalized["peerA"]; credits != 1 {
		t.Fatalf("expected 1 finalization credit, got %d", credits)
	}
}

// TestReorgSafety verifies the tracker resolves the head block by HASH
// so a head event pointing at a sibling block does not emit inclusions
// from the canonical block at the same height.
func TestReorgSafety(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	// Two blocks at height 1: canonical A contains tx; sibling B does not.
	blockA := chain.addBlockAtHeight(1, 1, []*types.Transaction{tx}, true)
	blockB := chain.addBlockAtHeight(1, 2, nil, false)
	if blockA.Hash() == blockB.Hash() {
		t.Fatal("sibling blocks ended up with the same hash")
	}

	// Head announces sibling B — emit must contain no peerA inclusions.
	chain.sendHeadBlock(blockB)
	waitStep(t, tr)
	if incl := consumer.last().inclusions["peerA"]; incl != 0 {
		t.Fatalf("sibling-B head should emit 0 peerA inclusions, got %d", incl)
	}

	// Head announces canonical A — emit must contain 1 peerA inclusion.
	chain.sendHeadBlock(blockA)
	waitStep(t, tr)
	if incl := consumer.last().inclusions["peerA"]; incl != 1 {
		t.Fatalf("canonical-A head should emit 1 peerA inclusion, got %d", incl)
	}
}

// TestReorgEmitsObsChainReorged builds a 1-block fork and verifies the
// tracker emits ObsChainReorged + a StateChange back to StatusPooled
// for a tx that was Included in the orphaned block. After the reorg,
// when the tx reappears on the new chain, it transitions back to
// StatusIncluded.
func TestReorgEmitsObsChainReorged(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	stateCh := make(chan StateChange, 8)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	// Build a chain: genesis-equivalent block 1, then fork at block 2.
	block1 := chain.addBlock(1, nil)
	// Block 2A contains the tx; canonical for now.
	block2A := chain.addChild(block1, 1, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block2A)
	waitStep(t, tr)

	// Confirm the tx reached StatusIncluded.
	if info := tr.GetTx(tx.Hash()); info == nil || info.Status != StatusIncluded {
		t.Fatalf("expected StatusIncluded after first head, got %+v", info)
	}

	// Reorg: block 2B is a sibling (same height, different hash, no tx).
	// Send 2B as the new head — its parent is also block1, so the tracker
	// must reorg block 2A out.
	block2B := chain.addChild(block1, 2, nil, false)
	chain.sendHeadBlock(block2B)
	waitStep(t, tr)

	// Drain state changes; we want to see Included→Pooled triggered by
	// ObsChainReorged.
	deadline := time.After(500 * time.Millisecond)
	sawReorg := false
loop:
	for {
		select {
		case ev := <-stateCh:
			if ev.TxHash == tx.Hash() && ev.NewStatus == StatusPooled && ev.Trigger == ObsChainReorged {
				sawReorg = true
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	if !sawReorg {
		t.Fatal("did not observe a Pooled state change with Trigger=ObsChainReorged")
	}

	info := tr.GetTx(tx.Hash())
	if info == nil {
		t.Fatal("expected TxInfo for tx after reorg")
	}
	if info.Status != StatusPooled {
		t.Errorf("Status: got %v, want StatusPooled", info.Status)
	}
	if info.BlockNum != 0 || info.BlockHash != (common.Hash{}) {
		t.Errorf("expected block context cleared after reorg, got %d/%x", info.BlockNum, info.BlockHash)
	}

	// Re-include on the new chain at block 3 (child of 2B). Should
	// transition back to StatusIncluded.
	block3 := chain.addChild(block2B, 3, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block3)
	waitStep(t, tr)

	info = tr.GetTx(tx.Hash())
	if info == nil || info.Status != StatusIncluded {
		t.Fatalf("expected StatusIncluded after re-inclusion, got %+v", info)
	}
}

// TestForwardChainNoReorg verifies the tracker doesn't emit
// ObsChainReorged on normal forward chain progression where each new
// head's ParentHash matches the previous head's hash.
func TestForwardChainNoReorg(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 16)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	block1 := chain.addBlock(1, nil)
	block2 := chain.addChild(block1, 1, []*types.Transaction{tx}, true)
	block3 := chain.addChild(block2, 1, nil, true)

	chain.sendHeadBlock(block1)
	waitStep(t, tr)
	chain.sendHeadBlock(block2)
	waitStep(t, tr)
	chain.sendHeadBlock(block3)
	waitStep(t, tr)

	// Drain a short window and ensure no ObsChainReorged appears.
	deadline := time.After(100 * time.Millisecond)
	for {
		select {
		case obs := <-obsCh:
			if obs.Kind == ObsChainReorged {
				t.Fatalf("unexpected ObsChainReorged on forward chain: %+v", obs)
			}
		case <-deadline:
			return
		}
	}
}

// TestHandleChainHeadNilConsumer verifies the tracker tolerates a nil
// consumer (useful for tests that only exercise tx-lifecycle behavior).
func TestHandleChainHeadNilConsumer(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	chain.addBlock(1, nil)
	chain.sendHead(1)
	waitStep(t, tr) // should not panic
}

// drainObs / drainState consume events from a subscription into a slice
// until the feed is quiet for `idle`. The counts are asserted by callers.
func drainObs(t *testing.T, sub event.Subscription, ch <-chan Observation, idle time.Duration) []Observation {
	t.Helper()
	var got []Observation
	for {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case err := <-sub.Err():
			if err != nil {
				t.Fatalf("subscription error: %v", err)
			}
			return got
		case <-time.After(idle):
			return got
		}
	}
}

func drainState(t *testing.T, sub event.Subscription, ch <-chan StateChange, idle time.Duration) []StateChange {
	t.Helper()
	var got []StateChange
	for {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case err := <-sub.Err():
			if err != nil {
				t.Fatalf("subscription error: %v", err)
			}
			return got
		case <-time.After(idle):
			return got
		}
	}
}

// TestAnnounceEmitsObservationAndStateChange verifies the two-level model:
// every NotifyAnnounced call produces exactly one Observation, but only
// the first call for a given hash produces a StateChange.
func TestAnnounceEmitsObservationAndStateChange(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 16)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 16)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	tx := makeTx(1)
	tr.NotifyAnnounced("peerA", []common.Hash{tx.Hash()}, nil, nil)
	tr.NotifyAnnounced("peerB", []common.Hash{tx.Hash()}, nil, nil) // duplicate: obs yes, state no
	tr.NotifyAnnounced("peerA", []common.Hash{makeTx(2).Hash()}, nil, nil)

	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	if len(obs) != 3 {
		t.Fatalf("expected 3 observations, got %d (%+v)", len(obs), obs)
	}
	for i, o := range obs {
		if o.Kind != ObsAnnouncedInbound {
			t.Errorf("obs[%d]: expected kind=ObsAnnouncedInbound, got %v", i, o.Kind)
		}
	}
	if obs[1].Peer != "peerB" {
		t.Errorf("obs[1]: expected Peer=peerB, got %q", obs[1].Peer)
	}

	states := drainState(t, stateSub, stateCh, 50*time.Millisecond)
	if len(states) != 2 {
		t.Fatalf("expected 2 state changes (one per distinct hash), got %d (%+v)", len(states), states)
	}
	for _, s := range states {
		if s.OldStatus != StatusUnknown || s.NewStatus != StatusAnnounced {
			t.Errorf("unexpected transition: %v → %v", s.OldStatus, s.NewStatus)
		}
		if s.Trigger != ObsAnnouncedInbound {
			t.Errorf("expected Trigger=ObsAnnouncedInbound, got %v", s.Trigger)
		}
	}

	// peerB must still be recorded as an announcer of the first tx.
	info := tr.GetTx(tx.Hash())
	if info == nil {
		t.Fatal("expected TxInfo for tx")
	}
	if len(info.Announcers) != 2 {
		t.Fatalf("expected 2 announcers, got %v", info.Announcers)
	}
}

// TestNotifyAnnouncedStashesMeta verifies that the eth/68 announcement
// metadata (Types, Sizes) is captured on TxInfo on the first inbound
// announcement, that conflicting subsequent announcements never
// overwrite the recorded values, and that fillTxBody still runs to
// populate body-derived fields once an actual body arrives — the
// "body filled" sentinel having moved from TxSize to Gas.
func TestNotifyAnnouncedStashesMeta(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 8)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	tx := makeTx(7) // legacy tx, real tx.Type() == 0
	hash := tx.Hash()

	// First announcement: peer claims type=2 (DynFee), size=42.
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, []byte{2}, []uint32{42})

	info := tr.GetTx(hash)
	if info == nil {
		t.Fatal("expected TxInfo for announced hash")
	}
	if info.TxType != 2 {
		t.Errorf("expected TxType=2 from announcement, got %d", info.TxType)
	}
	if info.TxSize != 42 {
		t.Errorf("expected TxSize=42 from announcement, got %d", info.TxSize)
	}
	if info.Gas != 0 {
		t.Errorf("expected Gas=0 (body not yet filled), got %d", info.Gas)
	}

	// The first observation must carry the type so a streaming consumer
	// doesn't have to re-fetch GetTx.
	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	if len(obs) != 1 || obs[0].TxType != 2 {
		t.Fatalf("expected one observation with TxType=2, got %+v", obs)
	}

	// Second announcement with conflicting metadata: first-wins.
	tr.NotifyAnnounced("peerB", []common.Hash{hash}, []byte{3}, []uint32{99})
	info = tr.GetTx(hash)
	if info.TxType != 2 || info.TxSize != 42 {
		t.Errorf("re-announce overwrote first-wins meta: got TxType=%d TxSize=%d",
			info.TxType, info.TxSize)
	}

	// Body arrival: fillTxBody must still run because Gas is the new
	// body-fill sentinel. TxType / TxSize get refreshed to the
	// authoritative body values (here a Legacy tx → type 0).
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	info = tr.GetTx(hash)
	if info.Gas != 21000 {
		t.Errorf("expected Gas=21000 from body fill, got %d", info.Gas)
	}
	if info.TxType != tx.Type() {
		t.Errorf("expected TxType=%d (from body), got %d", tx.Type(), info.TxType)
	}
	if info.TxSize != uint32(tx.Size()) {
		t.Errorf("expected TxSize=%d (from body), got %d", tx.Size(), info.TxSize)
	}
}

// TestNotifyAnnouncedNilMeta verifies the legacy nil/nil call form
// stays a no-op for metadata: no fields stashed at announce time,
// body fill works unchanged once the body arrives.
func TestNotifyAnnouncedNilMeta(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(8)
	hash := tx.Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)

	info := tr.GetTx(hash)
	if info.TxType != 0 || info.TxSize != 0 || info.Gas != 0 {
		t.Errorf("nil meta should leave body fields zero, got TxType=%d TxSize=%d Gas=%d",
			info.TxType, info.TxSize, info.Gas)
	}

	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	info = tr.GetTx(hash)
	if info.Gas != 21000 {
		t.Errorf("body fill failed after nil-meta announce: Gas=%d", info.Gas)
	}
}

// TestPureObservationsDoNotTransition verifies the observation kinds
// that carry no state-machine transition (outbound announce, inbound
// request, outbound delivery) produce Observations only.
func TestPureObservationsDoNotTransition(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 16)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 16)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	hashes := []common.Hash{makeTx(1).Hash()}
	tr.NotifyAnnouncedOutbound("peerA", hashes)
	tr.NotifyRequestedInbound("peerB", hashes)
	tr.NotifyDeliveredOutbound("peerC", hashes)

	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	if len(obs) != 3 {
		t.Fatalf("expected 3 observations, got %d", len(obs))
	}
	wantKinds := []ObsKind{ObsAnnouncedOutbound, ObsRequestedInbound, ObsDeliveredOutbound}
	for i, k := range wantKinds {
		if obs[i].Kind != k {
			t.Errorf("obs[%d]: expected kind=%v, got %v", i, k, obs[i].Kind)
		}
	}

	if states := drainState(t, stateSub, stateCh, 50*time.Millisecond); len(states) != 0 {
		t.Errorf("expected no state changes for pure observation kinds, got %+v", states)
	}
}

// TestFullLifecycleEmitsBothLevels walks a single tx through the forward
// path Announced→Requested→Received→Pooled→Included→Finalized and
// asserts each step produces one Observation and one StateChange with
// matching context.
func TestFullLifecycleEmitsBothLevels(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 32)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 32)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyFetchRequested("peerA", []common.Hash{hash})
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	tr.NotifyAccepted("peerA", []common.Hash{hash})

	chain.addBlock(1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)

	chain.setFinalBlock(1)
	chain.addBlock(2, nil)
	chain.sendHead(2)
	waitStep(t, tr)

	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	wantObsKinds := []ObsKind{
		ObsAnnouncedInbound,
		ObsRequestedOutbound,
		ObsDeliveredInbound,
		ObsPoolAccepted,
		ObsChainIncluded,
		ObsChainFinalized,
	}
	if len(obs) != len(wantObsKinds) {
		t.Fatalf("expected %d observations, got %d (%+v)", len(wantObsKinds), len(obs), obs)
	}
	for i, k := range wantObsKinds {
		if obs[i].Kind != k {
			t.Errorf("obs[%d]: expected kind=%v, got %v", i, k, obs[i].Kind)
		}
		if obs[i].TxHash != hash {
			t.Errorf("obs[%d]: unexpected tx hash", i)
		}
	}
	if obs[4].BlockNum != 1 || obs[5].BlockNum != 1 {
		t.Errorf("expected BlockNum=1 on inclusion + finalization observations, got %d / %d",
			obs[4].BlockNum, obs[5].BlockNum)
	}

	states := drainState(t, stateSub, stateCh, 50*time.Millisecond)
	want := []struct {
		old, new TxStatus
		trigger  ObsKind
	}{
		{StatusUnknown, StatusAnnounced, ObsAnnouncedInbound},
		{StatusAnnounced, StatusRequested, ObsRequestedOutbound},
		{StatusRequested, StatusReceived, ObsDeliveredInbound},
		{StatusReceived, StatusPooled, ObsPoolAccepted},
		{StatusPooled, StatusIncluded, ObsChainIncluded},
		{StatusIncluded, StatusFinalized, ObsChainFinalized},
	}
	if len(states) != len(want) {
		t.Fatalf("expected %d state changes, got %d (%+v)", len(want), len(states), states)
	}
	for i, s := range states {
		if s.OldStatus != want[i].old || s.NewStatus != want[i].new {
			t.Errorf("state[%d]: got %v→%v, want %v→%v",
				i, s.OldStatus, s.NewStatus, want[i].old, want[i].new)
		}
		if s.Trigger != want[i].trigger {
			t.Errorf("state[%d]: expected Trigger=%v, got %v", i, want[i].trigger, s.Trigger)
		}
	}
	if states[4].BlockNum != 1 || states[5].BlockNum != 1 {
		t.Errorf("expected BlockNum=1 on inclusion + finalization states, got %d / %d",
			states[4].BlockNum, states[5].BlockNum)
	}
}

// TestRejectedCarriesReason verifies pool rejection produces both an
// Observation and a StateChange with the rejection reason propagated.
func TestRejectedCarriesReason(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 4)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 4)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	hash := makeTx(1).Hash()
	tr.NotifyRejected("peerA", hash, errors.New("underpriced"))

	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	if len(obs) != 1 || obs[0].Kind != ObsPoolRejected {
		t.Fatalf("expected 1 ObsPoolRejected, got %+v", obs)
	}
	if obs[0].Reason != "underpriced" {
		t.Errorf("expected Reason=underpriced, got %q", obs[0].Reason)
	}

	states := drainState(t, stateSub, stateCh, 50*time.Millisecond)
	if len(states) != 1 {
		t.Fatalf("expected 1 state change, got %d", len(states))
	}
	if states[0].NewStatus != StatusRejected {
		t.Errorf("expected StatusRejected, got %v", states[0].NewStatus)
	}
	if states[0].Reason != "underpriced" {
		t.Errorf("expected Reason=underpriced, got %q", states[0].Reason)
	}
	if states[0].Peer != "peerA" {
		t.Errorf("expected Peer=peerA, got %q", states[0].Peer)
	}
}

// TestDroppedCarriesReason verifies eviction after acceptance emits both
// an Observation and a StateChange, with the drop reason propagated.
func TestDroppedCarriesReason(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 16)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 16)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	hash := makeTx(1).Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	// Route through Pooled — the strict guard on ObsPoolEvicted only
	// transitions out of StatusPooled. Drops directly from Announced
	// are suppressed (see TestPoolEvictAfterIncludeIsNoop).
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalReplaced)

	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	if len(obs) != 3 {
		t.Fatalf("expected 3 observations, got %d (%+v)", len(obs), obs)
	}
	if obs[0].Kind != ObsAnnouncedInbound ||
		obs[1].Kind != ObsPoolAccepted ||
		obs[2].Kind != ObsPoolEvicted {
		t.Errorf("unexpected observation kinds: %v, %v, %v",
			obs[0].Kind, obs[1].Kind, obs[2].Kind)
	}
	if obs[2].Reason != "replaced" {
		t.Errorf("expected Reason=replaced on evict, got %q", obs[2].Reason)
	}

	states := drainState(t, stateSub, stateCh, 50*time.Millisecond)
	if len(states) != 3 {
		t.Fatalf("expected 3 state changes (Announced + Pooled + Dropped), got %d (%+v)", len(states), states)
	}
	if states[0].NewStatus != StatusAnnounced ||
		states[1].NewStatus != StatusPooled ||
		states[2].NewStatus != StatusDropped {
		t.Errorf("unexpected transition sequence: %v → %v → %v",
			states[0].NewStatus, states[1].NewStatus, states[2].NewStatus)
	}
	if states[2].Reason != "replaced" {
		t.Errorf("expected Reason=replaced, got %q", states[2].Reason)
	}
}

// TestPoolEvictAfterIncludeIsNoop verifies that an inclusion-driven
// pool removal (the txpool's RemovedTxsEvent emitted via
// demoteUnexecutables once the tx is mined) does NOT transition the
// status away from StatusIncluded. The Observation still fires (the
// fact is recorded), but no StateChange is emitted and DropReason is
// not written to the TxInfo.
func TestPoolEvictAfterIncludeIsNoop(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 16)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 16)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	tx := makeTx(1)
	hash := tx.Hash()
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	chain.addBlock(1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)

	if info := tr.GetTx(hash); info == nil || info.Status != StatusIncluded {
		t.Fatalf("expected StatusIncluded after chain head, got %+v", info)
	}

	// Drain observations and state changes from the lifecycle so far so
	// the post-eviction drains start empty.
	drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	drainState(t, stateSub, stateCh, 50*time.Millisecond)

	// The pool's demoteUnexecutables emits a removed event with reason
	// "nonce expired" once the tx is mined. The tracker must record the
	// raw observation but NOT transition Included → Dropped.
	tr.NotifyDropped(hash, core.RemovalNonceExpired)

	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	if len(obs) != 1 || obs[0].Kind != ObsPoolEvicted {
		t.Fatalf("expected one ObsPoolEvicted observation, got %+v", obs)
	}
	states := drainState(t, stateSub, stateCh, 50*time.Millisecond)
	if len(states) != 0 {
		t.Fatalf("expected no StateChange (already Included), got %+v", states)
	}

	info := tr.GetTx(hash)
	if info.Status != StatusIncluded {
		t.Errorf("status should stay Included, got %v", info.Status)
	}
	if info.DropReason != "" {
		t.Errorf("DropReason should be empty for inclusion-driven removal, got %q", info.DropReason)
	}
}

// TestPoolEvictBeforeChainHeadEventuallyIncluded verifies the
// inverted-ordering case: the pool RemovedTxsEvent reaches the
// tracker before the ChainHeadEvent. The transient Pooled → Dropped
// is unavoidable, but the next chain head must rescue the status to
// Included via the existing ObsChainIncluded rule.
func TestPoolEvictBeforeChainHeadEventuallyIncluded(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()
	tr.NotifyAccepted("peerA", []common.Hash{hash})

	// Pool removal arrives first → Pooled → Dropped (transient).
	tr.NotifyDropped(hash, core.RemovalNonceExpired)
	if info := tr.GetTx(hash); info.Status != StatusDropped {
		t.Fatalf("expected transient StatusDropped, got %v", info.Status)
	}

	// Chain head arrives next. The existing ObsChainIncluded rule
	// permits Dropped → Included.
	chain.addBlock(1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)

	info := tr.GetTx(hash)
	if info.Status != StatusIncluded {
		t.Errorf("status should rescue to Included, got %v", info.Status)
	}
}

// TestStatsConsumerObservationHooks verifies that the per-peer hooks
// added on the StatsConsumer interface fire from the tracker as
// expected: NotifyAnnounced/Delivered/Accepted with batched counts,
// NotifyRejected per single hash, and NotifyDropped credited to the
// tx's recorded Deliverer (only when the eviction actually fires the
// state transition; inclusion-driven removals don't credit a drop).
func TestStatsConsumerObservationHooks(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx1, tx2 := makeTx(1), makeTx(2)
	h1, h2 := tx1.Hash(), tx2.Hash()

	// Inbound announcement of two hashes from peerA.
	tr.NotifyAnnounced("peerA", []common.Hash{h1, h2}, nil, nil)
	// Delivery of both bodies from peerA.
	tr.NotifyReceived("peerA", []*types.Transaction{tx1, tx2})
	// Pool accepts both.
	tr.NotifyAccepted("peerA", []common.Hash{h1, h2})
	// Reject a third hash directly (no prior pool entry).
	tr.NotifyRejected("peerB", makeTx(3).Hash(), errors.New("underpriced"))
	// Genuine drop of tx1 (currently Pooled with peerA as deliverer).
	tr.NotifyDropped(h1, core.RemovalReplaced)

	// Inclusion-driven removal: tx2 reaches Included via the chain
	// then the pool emits a removal. The drop transition is suppressed
	// (TestPoolEvictAfterIncludeIsNoop), so the consumer must NOT see
	// a credit for this one.
	chain.addBlock(1, []*types.Transaction{tx2})
	chain.sendHead(1)
	waitStep(t, tr)
	tr.NotifyDropped(h2, core.RemovalNonceExpired)

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if got := consumer.announced["peerA"]; got != 2 {
		t.Errorf("announced[peerA]: got %d, want 2", got)
	}
	if got := consumer.delivered["peerA"]; got != 2 {
		t.Errorf("delivered[peerA]: got %d, want 2", got)
	}
	if got := consumer.accepted["peerA"]; got != 2 {
		t.Errorf("accepted[peerA]: got %d, want 2", got)
	}
	if got := consumer.rejected["peerB"]; got != 1 {
		t.Errorf("rejected[peerB]: got %d, want 1", got)
	}
	// peerA gets the dropped credit for tx1 (genuine drop, deliverer
	// captured), but NOT for tx2 (inclusion-driven removal — the
	// transition was suppressed).
	if got := consumer.dropped["peerA"]; got != 1 {
		t.Errorf("dropped[peerA]: got %d, want 1 (only tx1, not tx2)", got)
	}
}

// TestDroppedUnknownHashIsNoop verifies NotifyDropped for a tx the
// tracker has never seen emits no Observation and no StateChange.
func TestDroppedUnknownHashIsNoop(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 4)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 4)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	tr.NotifyDropped(makeTx(1).Hash(), core.RemovalReplaced)

	if got := drainObs(t, obsSub, obsCh, 50*time.Millisecond); len(got) != 0 {
		t.Fatalf("expected no observations, got %+v", got)
	}
	if got := drainState(t, stateSub, stateCh, 50*time.Millisecond); len(got) != 0 {
		t.Fatalf("expected no state changes, got %+v", got)
	}
}

// TestGetTxReturnsSnapshot verifies that GetTx returns a deep-copied
// snapshot — mutating the returned TxInfo does not change tracker state.
func TestGetTxReturnsSnapshot(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	hash := makeTx(1).Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)

	snap := tr.GetTx(hash)
	if snap == nil {
		t.Fatal("expected snapshot, got nil")
	}
	snap.Announcers = append(snap.Announcers, "attacker")
	snap.DropReason = "tampered"

	again := tr.GetTx(hash)
	if containsString(again.Announcers, "attacker") {
		t.Error("internal state mutated via returned snapshot (Announcers)")
	}
	if again.DropReason == "tampered" {
		t.Error("internal state mutated via returned snapshot (DropReason)")
	}
}

// TestPerStageTimestampsPopulated verifies that as a tx walks the
// lifecycle, each per-stage timestamp on TxInfo is set on the
// corresponding forward transition, the timestamps are
// non-decreasing, and stages the tx never reached remain zero.
func TestPerStageTimestampsPopulated(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyFetchRequested("peerA", []common.Hash{hash})
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	tr.NotifyAccepted("peerA", []common.Hash{hash})

	chain.addBlock(1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)

	chain.setFinalBlock(1)
	chain.addBlock(2, nil)
	chain.sendHead(2)
	waitStep(t, tr)

	info := tr.GetTx(hash)
	if info == nil {
		t.Fatal("expected TxInfo for tx")
	}
	stages := []struct {
		name string
		ts   time.Time
	}{
		{"FirstSeen", info.FirstSeen},
		{"Requested", info.Requested},
		{"Received", info.Received},
		{"Pooled", info.Pooled},
		{"Included", info.Included},
		{"Finalized", info.Finalized},
	}
	for _, s := range stages {
		if s.ts.IsZero() {
			t.Errorf("expected %s to be set, got zero", s.name)
		}
	}
	for i := 1; i < len(stages); i++ {
		if stages[i].ts.Before(stages[i-1].ts) {
			t.Errorf("%s (%v) is before %s (%v)",
				stages[i].name, stages[i].ts,
				stages[i-1].name, stages[i-1].ts)
		}
	}
	// Dropped must remain zero — this tx never hit pool eviction.
	if !info.Dropped.IsZero() {
		t.Errorf("expected Dropped to be zero, got %v", info.Dropped)
	}
}

// TestDroppedTimestampPopulated verifies the Dropped stage timestamp
// is set on pool eviction and that no other stage past Pooled is
// populated.
func TestDroppedTimestampPopulated(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	hash := makeTx(1).Hash()
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalReplaced)

	info := tr.GetTx(hash)
	if info == nil {
		t.Fatal("expected TxInfo for tx")
	}
	if info.Pooled.IsZero() {
		t.Error("expected Pooled to be set")
	}
	if info.Dropped.IsZero() {
		t.Error("expected Dropped to be set")
	}
	if info.Dropped.Before(info.Pooled) {
		t.Errorf("Dropped (%v) before Pooled (%v)", info.Dropped, info.Pooled)
	}
	if !info.Included.IsZero() || !info.Finalized.IsZero() {
		t.Errorf("expected Included/Finalized zero, got %v/%v", info.Included, info.Finalized)
	}
}

// TestPassiveAnnounceAfterTerminalNoTransition verifies that a peer
// announcing a previously-dropped tx does NOT reset its status (the
// pre-redefinition behaviour was StatusDropped → StatusAnnounced +
// Returns++; that's wrong by the new rule). The announce only bumps
// the AnnouncedAfterTerminal counter; Returns stays unchanged.
func TestPassiveAnnounceAfterTerminalNoTransition(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	// First lifecycle: announce → pool → drop.
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalCapacity)
	info := tr.GetTx(hash)
	if info == nil || info.Status != StatusDropped {
		t.Fatalf("expected StatusDropped after first cycle, got %+v", info)
	}

	// Re-announce from another peer: counter-only, status stays Dropped,
	// Returns stays 0.
	tr.NotifyAnnounced("peerB", []common.Hash{hash}, nil, nil)
	info = tr.GetTx(hash)
	if info.Status != StatusDropped {
		t.Errorf("status after passive re-announce: got %v, want StatusDropped", info.Status)
	}
	if info.AnnouncedAfterTerminal != 1 {
		t.Errorf("AnnouncedAfterTerminal: got %d, want 1", info.AnnouncedAfterTerminal)
	}
	if info.Returns != 0 {
		t.Errorf("Returns: got %d, want 0 (passive announce shouldn't bump it)", info.Returns)
	}

	// A second peer announce: counter goes to 2, status still Dropped.
	tr.NotifyAnnounced("peerC", []common.Hash{hash}, nil, nil)
	info = tr.GetTx(hash)
	if info.AnnouncedAfterTerminal != 2 {
		t.Errorf("AnnouncedAfterTerminal: got %d, want 2", info.AnnouncedAfterTerminal)
	}
	if info.Status != StatusDropped {
		t.Errorf("status: got %v, want StatusDropped", info.Status)
	}
	if info.Returns != 0 {
		t.Errorf("Returns: got %d, want 0", info.Returns)
	}
}

// TestRequestAfterTerminalRestarts verifies an active step (we sent
// GetPooledTransactions for a previously-dropped tx) DOES restart the
// lifecycle: status moves Dropped → Requested, Returns and
// RequestedAfterTerminal both bump.
func TestRequestAfterTerminalRestarts(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalCapacity)
	if got := tr.GetTx(hash).Status; got != StatusDropped {
		t.Fatalf("setup: expected StatusDropped, got %v", got)
	}

	tr.NotifyFetchRequested("peerB", []common.Hash{hash})
	info := tr.GetTx(hash)
	if info.Status != StatusRequested {
		t.Errorf("status: got %v, want StatusRequested", info.Status)
	}
	if info.RequestedAfterTerminal != 1 {
		t.Errorf("RequestedAfterTerminal: got %d, want 1", info.RequestedAfterTerminal)
	}
	if info.Returns != 1 {
		t.Errorf("Returns: got %d, want 1", info.Returns)
	}
}

// TestPoolAcceptAfterTerminalRestarts verifies pool re-acceptance of a
// previously-dropped tx restarts the lifecycle: status moves Dropped →
// Pooled, Returns and PooledAfterTerminal both bump.
func TestPoolAcceptAfterTerminalRestarts(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalCapacity)
	if got := tr.GetTx(hash).Status; got != StatusDropped {
		t.Fatalf("setup: expected StatusDropped, got %v", got)
	}

	tr.NotifyAccepted("peerB", []common.Hash{hash})
	info := tr.GetTx(hash)
	if info.Status != StatusPooled {
		t.Errorf("status: got %v, want StatusPooled", info.Status)
	}
	if info.PooledAfterTerminal != 1 {
		t.Errorf("PooledAfterTerminal: got %d, want 1", info.PooledAfterTerminal)
	}
	if info.Returns != 1 {
		t.Errorf("Returns: got %d, want 1", info.Returns)
	}
}

// TestReorgIncrementsReorgRestarts verifies a chain reorg that pulls a
// tx out of inclusion (5 → 4) bumps both Returns and ReorgRestarts.
func TestReorgIncrementsReorgRestarts(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	block1 := chain.addBlock(1, nil)
	block2A := chain.addChild(block1, 1, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block2A)
	waitStep(t, tr)

	info := tr.GetTx(tx.Hash())
	if info == nil || info.Status != StatusIncluded {
		t.Fatalf("setup: expected StatusIncluded, got %+v", info)
	}
	if info.ReorgRestarts != 0 || info.Returns != 0 {
		t.Errorf("pre-reorg counters: ReorgRestarts=%d Returns=%d, want 0/0", info.ReorgRestarts, info.Returns)
	}

	// Reorg via a sibling block 2B that does not contain the tx.
	block2B := chain.addChild(block1, 2, nil, false)
	chain.sendHeadBlock(block2B)
	waitStep(t, tr)

	info = tr.GetTx(tx.Hash())
	if info.Status != StatusPooled {
		t.Errorf("status after reorg: got %v, want StatusPooled", info.Status)
	}
	if info.ReorgRestarts != 1 {
		t.Errorf("ReorgRestarts: got %d, want 1", info.ReorgRestarts)
	}
	if info.Returns != 1 {
		t.Errorf("Returns: got %d, want 1 (reorg counts as a return)", info.Returns)
	}
}

// TestPostTerminalCountersSaturate verifies the saturating uint8 cap.
// 300 passive announces against a Dropped tx leaves the counter at 255,
// not wrapped to 44.
func TestPostTerminalCountersSaturate(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalCapacity)

	for i := 0; i < 300; i++ {
		tr.NotifyAnnounced("peerSpam", []common.Hash{hash}, nil, nil)
	}
	info := tr.GetTx(hash)
	if info.AnnouncedAfterTerminal != 255 {
		t.Errorf("AnnouncedAfterTerminal: got %d, want 255 (saturating)", info.AnnouncedAfterTerminal)
	}
	if info.Returns != 0 {
		t.Errorf("Returns must stay 0 across 300 passive announces, got %d", info.Returns)
	}
}

// TestEvictionHistogramAttribution verifies that FIFO eviction
// increments the per-status histogram at the entry's last-known
// status, and that EvictedByStatus returns a copy (callers cannot
// mutate tracker state through it).
func TestEvictionHistogramAttribution(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()
	// Tiny cap so a handful of Notify* calls trigger eviction.
	tr.setMaxTracked(2)

	// 3 txs in StatusAnnounced; the oldest is FIFO-evicted.
	a, b, c := makeTx(1).Hash(), makeTx(2).Hash(), makeTx(3).Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{a, b, c}, nil, nil)

	hist := tr.EvictedByStatus()
	if hist[StatusAnnounced] != 1 {
		t.Errorf("expected 1 announced eviction, got %d", hist[StatusAnnounced])
	}
	for s := range hist {
		if TxStatus(s) == StatusAnnounced {
			continue
		}
		if hist[s] != 0 {
			t.Errorf("expected 0 for %s, got %d", TxStatus(s), hist[s])
		}
	}

	// Push two pooled txs in. The previously-announced entries (the
	// most recent two from the first call) get evicted because pooled
	// pushes the population back over the cap.
	d, e := makeTx(4), makeTx(5)
	tr.NotifyLocalSubmitted([]*types.Transaction{d, e})

	hist = tr.EvictedByStatus()
	// Total evictions: 5 distinct hashes through a cap-of-2 → 3 evictions.
	// All three evicted entries were StatusAnnounced (none advanced).
	if hist[StatusAnnounced] != 3 {
		t.Errorf("expected 3 announced evictions, got %d", hist[StatusAnnounced])
	}
	if hist[StatusPooled] != 0 {
		t.Errorf("no pooled evictions yet, got %d", hist[StatusPooled])
	}

	// Snapshot must be a copy: mutating it doesn't leak.
	hist[StatusPooled] = 9999
	again := tr.EvictedByStatus()
	if again[StatusPooled] != 0 {
		t.Errorf("EvictedByStatus snapshot leaked into tracker: got %d", again[StatusPooled])
	}
}

// TestNotifyLocalSubmitted verifies the local-submission producer:
// body fields populate, ti.Local is set, status transitions straight
// to StatusPooled, Deliverer remains empty (no peer credited), and
// the StateChange's Trigger is ObsPoolAccepted with empty Peer.
func TestNotifyLocalSubmitted(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	obsCh := make(chan Observation, 4)
	obsSub := tr.SubscribeObservations(obsCh)
	defer obsSub.Unsubscribe()

	stateCh := make(chan StateChange, 4)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	tx := makeTx(7)
	tr.NotifyLocalSubmitted([]*types.Transaction{tx})

	obs := drainObs(t, obsSub, obsCh, 50*time.Millisecond)
	if len(obs) != 1 || obs[0].Kind != ObsPoolAccepted {
		t.Fatalf("expected one ObsPoolAccepted observation, got %+v", obs)
	}
	if obs[0].Peer != "" {
		t.Errorf("expected empty Peer on local-submission obs, got %q", obs[0].Peer)
	}

	states := drainState(t, stateSub, stateCh, 50*time.Millisecond)
	if len(states) != 1 {
		t.Fatalf("expected one StateChange, got %d", len(states))
	}
	if states[0].OldStatus != StatusUnknown || states[0].NewStatus != StatusPooled {
		t.Errorf("unexpected transition: %v -> %v", states[0].OldStatus, states[0].NewStatus)
	}
	if states[0].Trigger != ObsPoolAccepted {
		t.Errorf("expected Trigger=ObsPoolAccepted, got %v", states[0].Trigger)
	}
	if states[0].Peer != "" {
		t.Errorf("expected empty Peer on state change, got %q", states[0].Peer)
	}

	info := tr.GetTx(tx.Hash())
	if info == nil {
		t.Fatal("expected TxInfo for local tx")
	}
	if !info.Local {
		t.Error("expected Local=true on local-submission TxInfo")
	}
	if info.TxSize == 0 {
		t.Error("expected body fields populated (TxSize non-zero)")
	}
	if info.Deliverer != "" {
		t.Errorf("expected empty Deliverer on local-submission, got %q", info.Deliverer)
	}
	if info.Pooled.IsZero() {
		t.Error("expected Pooled timestamp to be set")
	}
}

// TestNotifyReceivedFillsBodyFields verifies that NotifyReceived
// populates the body-derived fields on TxInfo on first observation,
// that subsequent calls leave the fields untouched (idempotent), and
// that GetTx returns a deep-copied snapshot for the *big.Int and
// *common.Address pointers.
func TestNotifyReceivedFillsBodyFields(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	to := common.HexToAddress("0xabababababababababababababababababababab")
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    7,
		GasPrice: big.NewInt(2),
		Gas:      30000,
		To:       &to,
		Value:    big.NewInt(123),
	})
	hash := tx.Hash()

	tr.NotifyReceived("peerA", []*types.Transaction{tx})

	info := tr.GetTx(hash)
	if info == nil {
		t.Fatal("expected TxInfo for tx")
	}
	if info.TxType != tx.Type() {
		t.Errorf("TxType: got %d, want %d", info.TxType, tx.Type())
	}
	if info.TxSize == 0 {
		t.Error("TxSize should be non-zero after NotifyReceived")
	}
	if info.Nonce != 7 {
		t.Errorf("Nonce: got %d, want 7", info.Nonce)
	}
	if info.Gas != 30000 {
		t.Errorf("Gas: got %d, want 30000", info.Gas)
	}
	if info.GasFeeCap == nil || info.GasFeeCap.ToInt().Cmp(big.NewInt(2)) != 0 {
		t.Errorf("GasFeeCap: got %v, want 2", info.GasFeeCap)
	}
	if info.GasTipCap == nil || info.GasTipCap.ToInt().Cmp(big.NewInt(2)) != 0 {
		t.Errorf("GasTipCap: got %v, want 2", info.GasTipCap)
	}
	if info.Value == nil || info.Value.ToInt().Cmp(big.NewInt(123)) != 0 {
		t.Errorf("Value: got %v, want 123", info.Value)
	}
	if info.To == nil || *info.To != to {
		t.Errorf("To: got %v, want %v", info.To, to)
	}

	// Mutate the snapshot — must not leak back into tracker state.
	// hexutil.Big aliases big.Int, so the underlying SetInt64 is
	// reachable through the type conversion.
	(*big.Int)(info.Value).SetInt64(99999)
	(*big.Int)(info.GasFeeCap).SetInt64(99999)
	(*big.Int)(info.GasTipCap).SetInt64(99999)
	*info.To = common.HexToAddress("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead")

	again := tr.GetTx(hash)
	if again.Value.ToInt().Cmp(big.NewInt(123)) != 0 {
		t.Errorf("Value mutated via snapshot: got %v", again.Value)
	}
	if again.GasFeeCap.ToInt().Cmp(big.NewInt(2)) != 0 {
		t.Errorf("GasFeeCap mutated via snapshot: got %v", again.GasFeeCap)
	}
	if *again.To != to {
		t.Errorf("To mutated via snapshot: got %v", *again.To)
	}
}

// TestNotifyReceivedIdempotentBodyFill verifies that calling
// NotifyReceived twice for the same hash does not overwrite fields
// already populated.
func TestNotifyReceivedIdempotentBodyFill(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyReceived("peerA", []*types.Transaction{tx})

	info1 := tr.GetTx(tx.Hash())
	if info1.TxSize == 0 {
		t.Fatal("expected body fields populated after first NotifyReceived")
	}

	// Second call is a no-op for body fields (no observable change to
	// the struct beyond LastChange staying put — body fields are
	// "first writer wins").
	tr.NotifyReceived("peerA", []*types.Transaction{tx})
	info2 := tr.GetTx(tx.Hash())
	if info2.TxSize != info1.TxSize {
		t.Errorf("TxSize changed across NotifyReceived calls: %d → %d",
			info1.TxSize, info2.TxSize)
	}
}

// TestSlowObsSubscriberDoesNotStallTracker verifies that a slow
// Observation subscriber only loses events (drop counter ticks) — it
// never blocks producers. Shared lock-release-before-consumer pattern
// plus the buffered obsCh guarantees forward progress.
func TestSlowObsSubscriberDoesNotStallTracker(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	ch := make(chan Observation) // unbuffered, never read
	sub := tr.SubscribeObservations(ch)
	defer sub.Unsubscribe()

	done := make(chan struct{})
	go func() {
		for i := 0; i < emitBuffer+128; i++ {
			tr.NotifyAnnounced("peerA", []common.Hash{makeTx(uint64(i)).Hash()}, nil, nil)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tracker stalled while obs subscriber blocked")
	}
	if tr.ObsDropped() == 0 {
		t.Error("expected at least one dropped observation due to blocked subscriber")
	}
}

// TestSlowStateSubscriberDoesNotStallTracker is the state-feed twin of
// TestSlowObsSubscriberDoesNotStallTracker.
func TestSlowStateSubscriberDoesNotStallTracker(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	ch := make(chan StateChange) // unbuffered, never read
	sub := tr.SubscribeStateChanges(ch)
	defer sub.Unsubscribe()

	done := make(chan struct{})
	go func() {
		for i := 0; i < emitBuffer+128; i++ {
			tr.NotifyAnnounced("peerA", []common.Hash{makeTx(uint64(i)).Hash()}, nil, nil)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tracker stalled while state subscriber blocked")
	}
	if tr.StateDropped() == 0 {
		t.Error("expected at least one dropped state change due to blocked subscriber")
	}
}

// TestPreSlotGate verifies the tracker withholds inclusion credit from a
// deliverer whose body we accepted at or after the including block's slot
// time — a re-broadcast of an already-mined tx earns no peer credit — while
// the inclusion still flows through the state machine so the lens feed sees
// it. Mirrors the pre-slot gate on peerdrop-latency's emitter tracker.
func TestPreSlotGate(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx := makeTx(1)
	// Accept the body now, so ti.Pooled ≈ current wall-clock.
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	// Mine it in a block whose slot time is in the distant past: the tx
	// was pooled at or after the slot, so the gate must suppress credit.
	chain.addBlockWithTime(1, 1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)

	if incl := consumer.last().inclusions["peerA"]; incl != 0 {
		t.Fatalf("pre-slot delivery should earn no inclusion credit, got peerA=%d", incl)
	}
	tr.mu.Lock()
	ti := tr.txs[tx.Hash()]
	tr.mu.Unlock()
	if ti == nil {
		t.Fatal("tx should still be tracked after inclusion")
	}
	if ti.IncludedDeliverer != "" {
		t.Errorf("pre-slot delivery should not freeze IncludedDeliverer, got %q", ti.IncludedDeliverer)
	}
	if ti.Status != StatusIncluded {
		t.Errorf("inclusion event should still fire (feed visibility), got status %v", ti.Status)
	}
}

// TestIncludedDelivererFreezesOnInclusion verifies the
// IncludedDeliverer field is stamped at the per-block inclusion
// walk and used as the source for finalization credit, so a late
// mutation of ti.Deliverer cannot redirect the finalization
// credit to a different peer.
func TestIncludedDelivererFreezesOnInclusion(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	// Include in block 1: inclusion credit goes to peerA, and
	// IncludedDeliverer is stamped.
	chain.addBlock(1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)
	if incl := consumer.last().inclusions["peerA"]; incl != 1 {
		t.Fatalf("inclusion credit: got peerA=%d, want 1", incl)
	}
	tr.mu.Lock()
	ti := tr.txs[tx.Hash()]
	if ti == nil || ti.IncludedDeliverer != "peerA" {
		got := ""
		if ti != nil {
			got = ti.IncludedDeliverer
		}
		tr.mu.Unlock()
		t.Fatalf("after inclusion: IncludedDeliverer=%q, want peerA", got)
	}
	tr.mu.Unlock()

	// Simulate a post-inclusion mutation of ti.Deliverer (mirrors
	// what a buggy pool re-acceptance would produce). The
	// applyPerEventBookkeeping branch only sets Deliverer when the
	// field is empty, so we mutate directly under the lock.
	tr.mu.Lock()
	tr.txs[tx.Hash()].Deliverer = "peerM"
	tr.mu.Unlock()

	// Finalize block 1 via the next head event; finalization credit
	// must follow the frozen IncludedDeliverer (peerA), NOT the
	// live Deliverer (peerM).
	chain.setFinalBlock(1)
	chain.addBlock(2, nil)
	chain.sendHead(2)
	waitStep(t, tr)
	fin := consumer.last().finalized
	if got := fin["peerA"]; got != 1 {
		t.Errorf("finalization credit peerA: got %d, want 1", got)
	}
	if got := fin["peerM"]; got != 0 {
		t.Errorf("finalization credit peerM: got %d, want 0 (must not steal credit)", got)
	}
}

// TestIncludedDelivererPrivateTxNoCredit verifies a tx that is
// not in our hot map at chain-head time leaves IncludedDeliverer
// empty, so a later "deliverer" set via a peer-late-relay path
// cannot earn finalization credit. Models the private-tx /
// post-inclusion-replay attack.
func TestIncludedDelivererPrivateTxNoCredit(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx := makeTx(1)
	// NO prior NotifyAccepted: tx is "private" from the tracker's POV.
	chain.addBlock(1, []*types.Transaction{tx})
	chain.sendHead(1)
	waitStep(t, tr)
	if incl := consumer.last().inclusions["peerM"]; incl != 0 {
		t.Fatalf("inclusions before any peer notify: got peerM=%d, want 0", incl)
	}

	// Now a malicious node M relays the body. The bookkeeping for
	// ObsPoolAccepted creates a TxInfo with Deliverer = M. If a
	// buggy pool ever accepted an already-mined tx, this is the
	// state we'd land in.
	tr.NotifyAccepted("peerM", []common.Hash{tx.Hash()})

	tr.mu.Lock()
	ti := tr.txs[tx.Hash()]
	if ti == nil {
		tr.mu.Unlock()
		t.Fatal("expected TxInfo created by NotifyAccepted")
	}
	if ti.Deliverer != "peerM" {
		t.Errorf("Deliverer: got %q, want peerM", ti.Deliverer)
	}
	if ti.IncludedDeliverer != "" {
		t.Errorf("IncludedDeliverer must stay empty for chain-only-discovered txs, got %q", ti.IncludedDeliverer)
	}
	tr.mu.Unlock()

	// Drive finalization. Since IncludedDeliverer is empty, no
	// finalization credit must flow to M.
	chain.setFinalBlock(1)
	chain.addBlock(2, nil)
	chain.sendHead(2)
	waitStep(t, tr)
	if got := consumer.last().finalized["peerM"]; got != 0 {
		t.Errorf("private-tx finalization credit peerM: got %d, want 0", got)
	}
}

// TestIncludedDelivererReorgKeepsHonestCredit verifies that on
// `5 -> 4 -> 5` (included, reorged-out, re-included), the
// IncludedDeliverer stays bound to the original honest deliverer
// across the reorg.
func TestIncludedDelivererReorgKeepsHonestCredit(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	// Block 1A on canonical chain: include tx.
	block1 := chain.addBlock(1, nil)
	block2A := chain.addChild(block1, 1, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block2A)
	waitStep(t, tr)
	tr.mu.Lock()
	if got := tr.txs[tx.Hash()].IncludedDeliverer; got != "peerA" {
		tr.mu.Unlock()
		t.Fatalf("after first inclusion: IncludedDeliverer=%q, want peerA", got)
	}
	tr.mu.Unlock()

	// Reorg: switch canonical to a sibling at height 2 that does
	// NOT contain tx. This drops tx back to Pooled (5 -> 4 via
	// ObsChainReorged).
	block2B := chain.addChild(block1, 2, nil, true)
	chain.sendHeadBlock(block2B)
	waitStep(t, tr)

	// Re-include tx in a new block 3 on the new canonical chain.
	block3 := chain.addChild(block2B, 1, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block3)
	waitStep(t, tr)

	// IncludedDeliverer must still point to peerA — the reorg did
	// NOT clear honest-credit attribution.
	tr.mu.Lock()
	got := tr.txs[tx.Hash()].IncludedDeliverer
	tr.mu.Unlock()
	if got != "peerA" {
		t.Fatalf("after reorg + re-inclusion: IncludedDeliverer=%q, want peerA", got)
	}
}

// Bit layout for the per-cycle bitmap: bit (status-1) marks each
// TxStatus visited within a cycle. Defined here as constants so the
// tests below read declaratively and a change in TxStatus enum order
// fails its test fixture first rather than silently miscounting.
const (
	bitAnnounced uint8 = 1 << (StatusAnnounced - 1)
	bitRequested uint8 = 1 << (StatusRequested - 1)
	bitReceived  uint8 = 1 << (StatusReceived - 1)
	bitPooled    uint8 = 1 << (StatusPooled - 1)
	bitIncluded  uint8 = 1 << (StatusIncluded - 1)
	bitFinalized uint8 = 1 << (StatusFinalized - 1)
	bitRejected  uint8 = 1 << (StatusRejected - 1)
	bitDropped   uint8 = 1 << (StatusDropped - 1)
)

// TestCycleBitmapHappyPath verifies the bitmap accumulates one bit
// per state visited in a single cycle (no re-entries, no reorgs).
// The tx walks Announced → Pooled → Included → Finalized, so the
// final bitmap should have those four bits set and CycleIdx 0.
func TestCycleBitmapHappyPath(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	if got, want := tr.GetTx(hash).CycleBitmap, bitAnnounced; got != want {
		t.Errorf("after Announced: got 0x%02x, want 0x%02x", got, want)
	}
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	if got, want := tr.GetTx(hash).CycleBitmap, bitAnnounced|bitPooled; got != want {
		t.Errorf("after Pooled: got 0x%02x, want 0x%02x", got, want)
	}

	block1 := chain.addBlock(1, nil)
	block2 := chain.addChild(block1, 1, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block2)
	waitStep(t, tr)
	if got, want := tr.GetTx(hash).CycleBitmap, bitAnnounced|bitPooled|bitIncluded; got != want {
		t.Errorf("after Included: got 0x%02x, want 0x%02x", got, want)
	}

	// Finalize block2 — the finalization scan runs on the next head
	// event, so we have to drive a successor block to make the
	// transition happen.
	chain.setFinalBlock(block2.NumberU64())
	block3 := chain.addChild(block2, 1, nil, true)
	chain.sendHeadBlock(block3)
	waitStep(t, tr)
	info := tr.GetTx(hash)
	if got, want := info.CycleBitmap, bitAnnounced|bitPooled|bitIncluded|bitFinalized; got != want {
		t.Errorf("after Finalized: got 0x%02x, want 0x%02x", got, want)
	}
	if info.Returns != 0 {
		t.Errorf("Returns: got %d, want 0 (no re-entry on happy path)", info.Returns)
	}
}

// TestCycleBitmapResetsOnReentry verifies the bitmap is scoped per
// cycle: when a dropped tx is actively re-requested (which bumps
// Returns), the bitmap resets to just the new state's bit and the
// prior cycle's visits are NOT carried over.
func TestCycleBitmapResetsOnReentry(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalCapacity)
	if got, want := tr.GetTx(hash).CycleBitmap, bitAnnounced|bitPooled|bitDropped; got != want {
		t.Fatalf("setup bitmap (cycle 0): got 0x%02x, want 0x%02x", got, want)
	}

	tr.NotifyFetchRequested("peerB", []common.Hash{hash})
	info := tr.GetTx(hash)
	if info.Returns != 1 {
		t.Fatalf("Returns: got %d, want 1", info.Returns)
	}
	if got, want := info.CycleBitmap, bitRequested; got != want {
		t.Errorf("after re-entry: got 0x%02x, want 0x%02x (bitmap must reset to just new state)", got, want)
	}
}

// TestCycleBitmapResetsOnReorg verifies a chain reorg (Included →
// Pooled) starts a new cycle and the bitmap resets to just the
// Pooled bit. Re-inclusion in the new cycle ORs the Included bit in
// without dragging cycle-0 visits along.
func TestCycleBitmapResetsOnReorg(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	hash := tx.Hash()

	tr.NotifyAccepted("peerA", []common.Hash{hash})
	block1 := chain.addBlock(1, nil)
	block2A := chain.addChild(block1, 1, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block2A)
	waitStep(t, tr)
	if got, want := tr.GetTx(hash).CycleBitmap, bitPooled|bitIncluded; got != want {
		t.Fatalf("setup bitmap (cycle 0): got 0x%02x, want 0x%02x", got, want)
	}

	// Reorg out via sibling block that does NOT contain the tx.
	block2B := chain.addChild(block1, 2, nil, false)
	chain.sendHeadBlock(block2B)
	waitStep(t, tr)
	info := tr.GetTx(hash)
	if info.Returns != 1 {
		t.Fatalf("Returns: got %d, want 1 (reorg counts as return)", info.Returns)
	}
	if got, want := info.CycleBitmap, bitPooled; got != want {
		t.Errorf("after reorg: got 0x%02x, want 0x%02x (must be just bitPooled)", got, want)
	}

	// Re-include in a new block on the new canonical chain — the
	// bitmap should now carry Pooled+Included for cycle 1, NOT
	// cycle 0's Included bit re-added on top.
	block3 := chain.addChild(block2B, 1, []*types.Transaction{tx}, true)
	chain.sendHeadBlock(block3)
	waitStep(t, tr)
	if got, want := tr.GetTx(hash).CycleBitmap, bitPooled|bitIncluded; got != want {
		t.Errorf("after re-inclusion (cycle 1): got 0x%02x, want 0x%02x", got, want)
	}
}

// TestStateChangeCarriesCycleFields verifies the emitted StateChange
// stream carries CycleIdx and CycleBitmap synchronously with the
// underlying transition — a subscriber doesn't have to fetch TxInfo
// to know per-cycle coverage.
func TestStateChangeCarriesCycleFields(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	ch := make(chan StateChange, 16)
	sub := tr.SubscribeStateChanges(ch)
	defer sub.Unsubscribe()

	tx := makeTx(1)
	hash := tx.Hash()
	tr.NotifyAnnounced("peerA", []common.Hash{hash}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{hash})
	tr.NotifyDropped(hash, core.RemovalCapacity)
	tr.NotifyFetchRequested("peerB", []common.Hash{hash})

	// Drain expected transitions: Announced, Pooled, Dropped, Requested.
	want := []struct {
		status TxStatus
		idx    uint8
		bitmap uint8
	}{
		{StatusAnnounced, 0, bitAnnounced},
		{StatusPooled, 0, bitAnnounced | bitPooled},
		{StatusDropped, 0, bitAnnounced | bitPooled | bitDropped},
		{StatusRequested, 1, bitRequested},
	}
	for i, w := range want {
		select {
		case sc := <-ch:
			if sc.NewStatus != w.status {
				t.Errorf("change[%d] NewStatus: got %v, want %v", i, sc.NewStatus, w.status)
			}
			if sc.CycleIdx != w.idx {
				t.Errorf("change[%d] CycleIdx: got %d, want %d", i, sc.CycleIdx, w.idx)
			}
			if sc.CycleBitmap != w.bitmap {
				t.Errorf("change[%d] CycleBitmap: got 0x%02x, want 0x%02x", i, sc.CycleBitmap, w.bitmap)
			}
		case <-time.After(time.Second):
			t.Fatalf("change[%d] (%v): timed out waiting for StateChange", i, w.status)
		}
	}
}

// TestHandleChainHeadChainOnlyEmitsStateChange verifies that a tx
// the tracker has NEVER seen before — first observed via chain
// inclusion (Flashbots / MEV-Boost / direct-to-builder path) — still
// produces a 0→Included StateChange, so subscribers can observe and
// classify chain-only flow.
func TestHandleChainHeadChainOnlyEmitsStateChange(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	// Prime the tracker with one observed tx so the outer gate
	// (len(t.txs) > 0) is satisfied; otherwise the block-tx loop
	// is skipped entirely during cold start.
	primer := makeTx(1)
	tr.NotifyAccepted("peerA", []common.Hash{primer.Hash()})

	stateCh := make(chan StateChange, 8)
	stateSub := tr.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	chainOnly := makeTx(99) // never NotifyAnnounced/Accepted/Received
	chain.addBlock(1, []*types.Transaction{primer, chainOnly})
	chain.sendHead(1)
	waitStep(t, tr)

	// Drain events; expect to see chainOnly transitioning Unknown→Included.
	deadline := time.After(500 * time.Millisecond)
	var sawChainOnly *StateChange
loop:
	for {
		select {
		case ev := <-stateCh:
			if ev.TxHash == chainOnly.Hash() && ev.NewStatus == StatusIncluded {
				cp := ev
				sawChainOnly = &cp
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	if sawChainOnly == nil {
		t.Fatal("no StateChange observed for chain-only tx — chain-inclusion path should emit one")
	}
	if sawChainOnly.OldStatus != StatusUnknown {
		t.Errorf("OldStatus: got %d, want StatusUnknown (0)", sawChainOnly.OldStatus)
	}
	// TxInfo should now exist with all pre-block stages zero — that's
	// the signature of a chain-only tx for downstream consumers.
	ti := tr.GetTx(chainOnly.Hash())
	if ti == nil {
		t.Fatal("expected GetTx to return TxInfo for chain-only tx after inclusion")
	}
	if !ti.Pooled.IsZero() {
		t.Errorf("chain-only tx has unexpected Pooled timestamp: %v", ti.Pooled)
	}
	if !ti.Requested.IsZero() {
		t.Errorf("chain-only tx has unexpected Requested timestamp: %v", ti.Requested)
	}
	if !ti.Received.IsZero() {
		t.Errorf("chain-only tx has unexpected Received timestamp: %v", ti.Received)
	}
	if len(ti.Announcers) != 0 {
		t.Errorf("chain-only tx has unexpected Announcers: %v", ti.Announcers)
	}
}
