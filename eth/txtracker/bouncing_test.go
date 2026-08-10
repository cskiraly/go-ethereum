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
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// signedTx mints a signed legacy tx for the given sender key + nonce.
// Used by the bouncing tests so TxInfo.From is populated by fillTxBody.
func signedTx(t *testing.T, key *ecdsa.PrivateKey, nonce uint64) *types.Transaction {
	t.Helper()
	tx := types.NewTx(&types.LegacyTx{Nonce: nonce, GasPrice: big.NewInt(1), Gas: 21000})
	signed, err := types.SignTx(tx, types.HomesteadSigner{}, key)
	if err != nil {
		t.Fatalf("SignTx: %v", err)
	}
	return signed
}

// driveBouncing drives a tx through Received → Accepted → Dropped(reason)
// and returns the hash. Used by every overflow-reason insertion test.
func driveBouncing(tr *Tracker, peer string, tx *types.Transaction, reason core.RemovalReason) common.Hash {
	tr.NotifyReceived(peer, []*types.Transaction{tx})
	tr.NotifyAccepted(peer, []common.Hash{tx.Hash()})
	tr.NotifyDropped(tx.Hash(), reason)
	return tx.Hash()
}

// TestBouncingInsertOnRateLimitedDrop verifies a "rate limited" eviction
// inserts a bouncing entry and IsBouncing returns true.
func TestBouncingInsertOnRateLimitedDrop(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)

	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true after rate-limited drop", hash)
	}
}

// TestBouncingInsertOnCapacityDrop verifies "capacity" inserts.
func TestBouncingInsertOnCapacityDrop(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalCapacity)

	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true after capacity drop", hash)
	}
}

// TestBouncingInsertOnReplacedDrop verifies "replaced" inserts.
func TestBouncingInsertOnReplacedDrop(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalReplaced)

	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true after replaced drop", hash)
	}
}

// TestBouncingInsertOnUnderpricedDrop verifies a drop-time "underpriced"
// reason inserts a bouncing entry. The fetcher's submission-time
// underpriced LRU only catches pool.Add → ErrUnderpriced; an
// already-accepted tx that the pool demotes/drops after a base-fee
// jump goes through this path instead, so without this entry every
// peer re-announce wastes a fetch + re-submit round-trip.
func TestBouncingInsertOnUnderpricedDrop(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalUnderpriced)

	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true after underpriced drop", hash)
	}
}

// TestBouncingNotInsertedForOtherReasons verifies drops with non-overflow
// reasons leave the bouncing map untouched. Listed reasons all
// re-validate on pool.add() retry (expired, nonce expired, invalid)
// so they self-protect without a fetcher-side cache.
func TestBouncingNotInsertedForOtherReasons(t *testing.T) {
	cases := []core.RemovalReason{core.RemovalExpired, core.RemovalNonceExpired, core.RemovalUnderfunded, core.RemovalGasLimitExceeded, core.RemovalInvalid, core.RemovalUnknown}
	for _, reason := range cases {
		t.Run(reason.String(), func(t *testing.T) {
			tr := New()
			chain := newMockChain()
			tr.Start(chain, nil)
			defer tr.Stop()

			key, _ := crypto.GenerateKey()
			tx := signedTx(t, key, 1)
			hash := driveBouncing(tr, "peerA", tx, reason)

			if tr.IsBouncing(hash, "") {
				t.Fatalf("IsBouncing(%v) = true after %v drop; want false", hash, reason)
			}
		})
	}
}

// TestBouncingClearedBySenderInclusion verifies including any tx from a
// sender clears all of that sender's bouncing entries. A different
// sender's inclusion must not clear the entry.
func TestBouncingClearedBySenderInclusion(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	keyA, _ := crypto.GenerateKey()
	keyB, _ := crypto.GenerateKey()
	txA1 := signedTx(t, keyA, 1)
	txA2 := signedTx(t, keyA, 2)
	txB := signedTx(t, keyB, 1)

	hashA1 := driveBouncing(tr, "peerA", txA1, core.RemovalRateLimited)
	hashA2 := driveBouncing(tr, "peerA", txA2, core.RemovalRateLimited)
	hashB := driveBouncing(tr, "peerB", txB, core.RemovalRateLimited)

	for _, h := range []common.Hash{hashA1, hashA2, hashB} {
		if !tr.IsBouncing(h, "") {
			t.Fatalf("IsBouncing(%v) = false; want true after rate-limited drop", h)
		}
	}

	// Include a tx from sender B only — bouncing entries are keyed by
	// sender, so the included tx need not be the same hash as the
	// dropped one. Use a fresh tx from B so the inclusion path runs
	// fillTxBody and recovers the sender from the body.
	txBIncluded := signedTx(t, keyB, 99)
	chain.addBlockAtHeight(1, 0, []*types.Transaction{txBIncluded}, true)
	chain.sendHead(1)
	waitStep(t, tr)

	if tr.IsBouncing(hashB, "") {
		t.Errorf("IsBouncing(B) = true after B's inclusion; want false")
	}
	if !tr.IsBouncing(hashA1, "") {
		t.Errorf("IsBouncing(A1) = false after B's inclusion; want true (different sender)")
	}
	if !tr.IsBouncing(hashA2, "") {
		t.Errorf("IsBouncing(A2) = false after B's inclusion; want true (different sender)")
	}

	// Now include a tx from A. Both A entries should clear.
	txAIncluded := signedTx(t, keyA, 99)
	chain.addBlockAtHeightWithParent(2, 0, chain.canonicalByNum[1], []*types.Transaction{txAIncluded}, true)
	chain.sendHead(2)
	waitStep(t, tr)

	if tr.IsBouncing(hashA1, "") {
		t.Errorf("IsBouncing(A1) = true after A's inclusion; want false")
	}
	if tr.IsBouncing(hashA2, "") {
		t.Errorf("IsBouncing(A2) = true after A's inclusion; want false")
	}
}

// TestBouncingSingleTxClearedByTTL verifies that a sender with no other
// Pooled tx at drop time gets a 5-minute TTL — and that once the TTL
// expires, IsBouncing returns false even without a sender-inclusion
// event. Uses direct map manipulation to fast-forward the TTL because
// the tracker reads time via time.Now(); a fake clock would require
// wider plumbing for one test.
func TestBouncingSingleTxClearedByTTL(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)

	// Sanity: entry exists with a non-zero `until` because the sender
	// had no other Pooled tx at drop time.
	v, ok := tr.bouncing.Load(hash)
	if !ok {
		t.Fatalf("bouncing entry missing right after drop")
	}
	entry := v.(*bouncingEntry)
	if entry.until.IsZero() {
		t.Fatalf("single-tx entry got no TTL; pooledBySender accounting is wrong")
	}

	// Force expiry by rewriting until to the past.
	tr.bouncing.Store(hash, &bouncingEntry{sender: entry.sender, until: time.Now().Add(-time.Second)})

	if tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = true after TTL expiry; want false", hash)
	}
	// IsBouncing also evicts the expired entry.
	if _, stillThere := tr.bouncing.Load(hash); stillThere {
		t.Errorf("expired entry not removed from bouncing map by IsBouncing")
	}
}

// TestBouncingMultiTxNotClearedByTime verifies that when the sender has
// other Pooled txs at drop time the bouncing entry has no TTL — only
// sender-inclusion or FIFO eviction can clear it.
func TestBouncingMultiTxNotClearedByTime(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx1 := signedTx(t, key, 1)
	tx2 := signedTx(t, key, 2)

	// Pool both txs from the same sender, then drop only tx1. tx2
	// remains Pooled, so tx1's bouncing entry should be tagged
	// multi-tx (until == zero).
	tr.NotifyReceived("peerA", []*types.Transaction{tx1, tx2})
	tr.NotifyAccepted("peerA", []common.Hash{tx1.Hash(), tx2.Hash()})
	tr.NotifyDropped(tx1.Hash(), core.RemovalRateLimited)

	v, ok := tr.bouncing.Load(tx1.Hash())
	if !ok {
		t.Fatalf("bouncing entry missing for tx1")
	}
	entry := v.(*bouncingEntry)
	if !entry.until.IsZero() {
		t.Errorf("multi-tx entry got a TTL; until=%v, want zero", entry.until)
	}
}

// stubPoolFloor reports a fixed effective-tip floor regardless of
// baseFee. Used by the fee-gate tests below.
type stubPoolFloor struct{ floor uint64 }

func (s stubPoolFloor) MinTip(uint64) uint64 { return s.floor }

// signedTxWithGasPrice mints a signed legacy tx with a custom gas price,
// so the test can place the tx above or below a stub floor.
func signedTxWithGasPrice(t *testing.T, key *ecdsa.PrivateKey, nonce uint64, gasPrice uint64) *types.Transaction {
	t.Helper()
	tx := types.NewTx(&types.LegacyTx{Nonce: nonce, GasPrice: new(big.Int).SetUint64(gasPrice), Gas: 21000})
	signed, err := types.SignTx(tx, types.HomesteadSigner{}, key)
	if err != nil {
		t.Fatalf("SignTx: %v", err)
	}
	return signed
}

// TestBouncingFeeGateBlocksClear verifies that when the pool floor sits
// above the tx's effective tip, sender-inclusion does NOT clear the
// bouncing entry — refetching would just bounce off as underpriced.
func TestBouncingFeeGateBlocksClear(t *testing.T) {
	tr := New()
	tr.SetPoolFloor(stubPoolFloor{floor: 100}) // tx must beat 100 wei tip
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTxWithGasPrice(t, key, 1, 50) // tip = 50 < floor 100
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)
	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true after rate-limited drop", hash)
	}

	// Include another tx from the same sender — sender slot freed, but
	// the gate should keep the entry blocked because tx fee < floor.
	included := signedTxWithGasPrice(t, key, 99, 50)
	chain.addBlockAtHeight(1, 0, []*types.Transaction{included}, true)
	chain.sendHead(1)
	waitStep(t, tr)

	if !tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = false after include with floor>fee; want still true (fee gate)", hash)
	}
}

// TestBouncingFeeGateAllowsClear verifies that when the tx's effective
// tip clears the floor, sender-inclusion clears the bouncing entry as
// in stage 1.
func TestBouncingFeeGateAllowsClear(t *testing.T) {
	tr := New()
	tr.SetPoolFloor(stubPoolFloor{floor: 100})
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTxWithGasPrice(t, key, 1, 200) // tip = 200 > floor 100
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)
	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true after rate-limited drop", hash)
	}

	included := signedTxWithGasPrice(t, key, 99, 200)
	chain.addBlockAtHeight(1, 0, []*types.Transaction{included}, true)
	chain.sendHead(1)
	waitStep(t, tr)

	if tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = true after include with fee>floor; want false", hash)
	}
}

// TestBouncingMetrics covers the three diagnostic meters:
//   - bouncing/cleared increments on each entry removed by sender
//     inclusion (gate passed or absent).
//   - bouncing/gate_blocked increments on each entry the fee gate
//     kept blocked.
//   - bouncing/size tracks the live map size.
//
// Run as a single test so the gauge baseline is captured once and
// asserted across all three scenarios.
func TestBouncingMetrics(t *testing.T) {
	tr := New()
	tr.SetPoolFloor(stubPoolFloor{floor: 100})
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	clearedBefore := bouncingClearedMeter.Snapshot().Count()
	blockedBefore := bouncingGateBlockedMeter.Snapshot().Count()
	sizeBefore := bouncingSizeGauge.Snapshot().Value()

	keyHigh, _ := crypto.GenerateKey()
	keyLow, _ := crypto.GenerateKey()
	txHigh := signedTxWithGasPrice(t, keyHigh, 1, 200) // tip 200 > floor 100
	txLow := signedTxWithGasPrice(t, keyLow, 1, 50)    // tip 50 < floor 100
	hashHigh := driveBouncing(tr, "peerA", txHigh, core.RemovalRateLimited)
	hashLow := driveBouncing(tr, "peerB", txLow, core.RemovalRateLimited)

	if got := bouncingSizeGauge.Snapshot().Value() - sizeBefore; got != 2 {
		t.Errorf("size delta after 2 inserts = %d, want 2", got)
	}

	// Include something from each sender. txHigh's entry should clear
	// (gate passed), txLow's should stay blocked (gate fired).
	includedHigh := signedTxWithGasPrice(t, keyHigh, 99, 200)
	includedLow := signedTxWithGasPrice(t, keyLow, 99, 50)
	chain.addBlockAtHeight(1, 0, []*types.Transaction{includedHigh, includedLow}, true)
	chain.sendHead(1)
	waitStep(t, tr)

	if tr.IsBouncing(hashHigh, "") {
		t.Errorf("hashHigh: expected cleared, still bouncing")
	}
	if !tr.IsBouncing(hashLow, "") {
		t.Errorf("hashLow: expected gate-blocked, cleared instead")
	}

	if delta := bouncingClearedMeter.Snapshot().Count() - clearedBefore; delta != 1 {
		t.Errorf("cleared meter delta = %d, want 1", delta)
	}
	if delta := bouncingGateBlockedMeter.Snapshot().Count() - blockedBefore; delta != 1 {
		t.Errorf("gate_blocked meter delta = %d, want 1", delta)
	}
	if got := bouncingSizeGauge.Snapshot().Value() - sizeBefore; got != 1 {
		t.Errorf("size delta after one clear = %d, want 1 (one cleared, one still blocked)", got)
	}
}

// TestBouncingFeeGateNoPoolFloor verifies that with no PoolFloor wired
// (default), sender-inclusion clears the entry regardless of fee
// (stage-1 behaviour preserved).
func TestBouncingFeeGateNoPoolFloor(t *testing.T) {
	tr := New() // no SetPoolFloor — fee gate disabled
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTxWithGasPrice(t, key, 1, 50)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)

	included := signedTxWithGasPrice(t, key, 99, 50)
	chain.addBlockAtHeight(1, 0, []*types.Transaction{included}, true)
	chain.sendHead(1)
	waitStep(t, tr)

	if tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = true with no PoolFloor; want false (stage-1 clear)", hash)
	}
}

// driveRejection drives a tx through Received → Rejected(reason)
// without ever pooling it (mirrors the submission-failure path:
// fetcher delivers body, pool.Add returns an error, fetcher's
// onRejected callback fires NotifyRejected). Returns the hash.
func driveRejection(tr *Tracker, peer string, tx *types.Transaction, err error) common.Hash {
	tr.NotifyReceived(peer, []*types.Transaction{tx})
	tr.NotifyRejected(peer, tx.Hash(), err)
	return tx.Hash()
}

// TestBouncingInsertOnRejectionReasons verifies that submission-time
// rejections with bounce-loop reasons (future-replace-pending, txpool
// full, authority reserved, insufficient-funds) insert into the
// bouncing map.
func TestBouncingInsertOnRejectionReasons(t *testing.T) {
	cases := []error{
		txpool.ErrFutureReplacePending,
		txpool.ErrTxPoolOverflow,
		txpool.ErrAuthorityReserved,
		core.ErrInsufficientFunds,
		// Wrapped form, as %w call sites produce.
		fmt.Errorf("%w: ctx", txpool.ErrFutureReplacePending),
		// Mirrors the detail wrapping in core/txpool/validation.go.
		fmt.Errorf("%w: balance 1234, tx cost 5678, overshot 4444", core.ErrInsufficientFunds),
	}
	for _, reason := range cases {
		t.Run(reason.Error(), func(t *testing.T) {
			tr := New()
			chain := newMockChain()
			tr.Start(chain, nil)
			defer tr.Stop()

			key, _ := crypto.GenerateKey()
			tx := signedTx(t, key, 1)
			hash := driveRejection(tr, "peerA", tx, reason)

			if !tr.IsBouncing(hash, "") {
				t.Errorf("IsBouncing(%v) = false after %v rejection; want true", hash, reason)
			}
		})
	}
}

// TestBouncingNotInsertedForOtherRejections verifies that other
// submission errors (typically fee-related, already covered by the
// fetcher's underpriced LRU) are not added to the bouncing map.
func TestBouncingNotInsertedForOtherRejections(t *testing.T) {
	cases := []error{
		txpool.ErrUnderpriced,
		txpool.ErrReplaceUnderpriced,
		txpool.ErrTxGasPriceTooLow,
		txpool.ErrAlreadyKnown,
		errors.New("invalid transaction"),
		nil,
	}
	for _, reason := range cases {
		t.Run(fmt.Sprint(reason), func(t *testing.T) {
			tr := New()
			chain := newMockChain()
			tr.Start(chain, nil)
			defer tr.Stop()

			key, _ := crypto.GenerateKey()
			tx := signedTx(t, key, 1)
			hash := driveRejection(tr, "peerA", tx, reason)

			if tr.IsBouncing(hash, "") {
				t.Errorf("IsBouncing(%v) = true after %v rejection; want false", hash, reason)
			}
		})
	}
}

// TestBouncingRejectionInsertedMeter verifies the inserted/reject
// meter increments on rejection-driven inserts and the inserted/drop
// meter is unaffected.
func TestBouncingRejectionInsertedMeter(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	rejBefore := bouncingInsertedRejectMeter.Snapshot().Count()
	dropBefore := bouncingInsertedDropMeter.Snapshot().Count()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	driveRejection(tr, "peerA", tx, txpool.ErrTxPoolOverflow)

	if got := bouncingInsertedRejectMeter.Snapshot().Count() - rejBefore; got != 1 {
		t.Errorf("inserted/reject delta = %d, want 1", got)
	}
	if got := bouncingInsertedDropMeter.Snapshot().Count() - dropBefore; got != 0 {
		t.Errorf("inserted/drop delta = %d, want 0 (rejection should not bump drop meter)", got)
	}
}

// TestBouncingRejectionClearedBySenderInclusion verifies that
// rejection-driven entries clear via the same sender-inclusion sweep
// as drop-driven ones — the unblock condition is identical.
func TestBouncingRejectionClearedBySenderInclusion(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveRejection(tr, "peerA", tx, txpool.ErrFutureReplacePending)
	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true after future-replace rejection", hash)
	}

	included := signedTx(t, key, 99)
	chain.addBlockAtHeight(1, 0, []*types.Transaction{included}, true)
	chain.sendHead(1)
	waitStep(t, tr)

	if tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = true after sender-inclusion; want false", hash)
	}
}

// TestBouncingClearedByTrackerEviction verifies that when the underlying
// TxInfo falls out of the tracker's hot map (FIFO), the bouncing entry
// is also removed. Otherwise IsBouncing would keep returning true on a
// hash the tracker no longer remembers.
func TestBouncingClearedByTrackerEviction(t *testing.T) {
	tr := New()
	tr.setMaxTracked(2)
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx1 := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx1, core.RemovalRateLimited)
	if !tr.IsBouncing(hash, "") {
		t.Fatalf("IsBouncing(%v) = false; want true", hash)
	}

	// Push past the cap with two unrelated txs from a fresh key so
	// tx1 is the oldest entry and gets FIFO-evicted.
	otherKey, _ := crypto.GenerateKey()
	tr.NotifyReceived("peerA", []*types.Transaction{
		signedTx(t, otherKey, 1),
		signedTx(t, otherKey, 2),
	})

	if tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = true after FIFO eviction; want false", hash)
	}
}

// TestBouncingBlockedConsumerSignal verifies the StatsConsumer hook
// fires from both bouncing-suppression paths (announce-side IsBouncing
// and body-side FilterInboundBodies) with the correct counts.
func TestBouncingBlockedConsumerSignal(t *testing.T) {
	tr := New()
	chain := newMockChain()
	consumer := &mockConsumer{}
	tr.Start(chain, consumer)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)

	tr.IsBouncing(hash, "peerX")
	tr.IsBouncing(hash, "peerX")
	tr.FilterInboundBodies("peerY", []*types.Transaction{tx})

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if got := consumer.bouncingBlocked["peerX"]; got != 2 {
		t.Errorf("bouncingBlocked[peerX] = %d; want 2", got)
	}
	if got := consumer.bouncingBlocked["peerY"]; got != 1 {
		t.Errorf("bouncingBlocked[peerY] = %d; want 1", got)
	}
}

// TestBouncingEntriesSnapshotCap verifies the snapshot caps at
// bouncingEntriesMaxResponse entries, prioritising the highest-block
// long tail so the lens always sees the most-active hashes regardless
// of how big the bouncing map gets server-side.
func TestBouncingEntriesSnapshotCap(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	// Inject more entries than the cap directly into the bouncing map
	// (the production path would FIFO-bound the map well above the
	// cap; we go just over to keep the test fast).
	const extra = 5
	const n = bouncingEntriesMaxResponse + extra
	for i := 0; i < n; i++ {
		var h common.Hash
		// Distinct hashes — last byte = i mod 256, third-from-last = i/256.
		h[31] = byte(i)
		h[30] = byte(i >> 8)
		entry := &bouncingEntry{
			sender: common.Address{byte(i)},
		}
		// Higher block counts for the LAST `extra` entries so we can
		// assert those survive the truncation.
		if i >= bouncingEntriesMaxResponse {
			entry.announcesBlocked.Store(uint64(1_000_000 + i))
		} else {
			entry.announcesBlocked.Store(1)
		}
		tr.bouncing.Store(h, entry)
		tr.bouncingCount.Add(1)
	}

	snap := tr.BouncingEntries()
	if got := len(snap); got != bouncingEntriesMaxResponse {
		t.Fatalf("snapshot len = %d; want %d", got, bouncingEntriesMaxResponse)
	}
	// Assert the high-block entries (the ones above the cap by index)
	// all made it into the snapshot.
	kept := make(map[common.Hash]bool, len(snap))
	for _, s := range snap {
		kept[s.Hash] = true
	}
	for i := bouncingEntriesMaxResponse; i < n; i++ {
		var h common.Hash
		h[31] = byte(i)
		h[30] = byte(i >> 8)
		if !kept[h] {
			t.Errorf("high-block entry %d (hash %x) missing from capped snapshot", i, h[:4])
		}
	}
}

// TestBouncingEntriesSnapshot verifies the snapshot RPC helper returns
// one entry per live bouncing hash, sorted by hash, with the saved-
// bounce counters and distinct-peers set populated.
func TestBouncingEntriesSnapshot(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	keyA, _ := crypto.GenerateKey()
	keyB, _ := crypto.GenerateKey()
	txA := signedTx(t, keyA, 1)
	txB := signedTx(t, keyB, 1)
	hashA := driveBouncing(tr, "peerA", txA, core.RemovalRateLimited)
	hashB := driveBouncing(tr, "peerA", txB, core.RemovalCapacity)

	// Exercise both counters from a couple of peers on entry A only.
	tr.IsBouncing(hashA, "peerX")
	tr.IsBouncing(hashA, "peerY")
	tr.FilterInboundBodies("peerZ", []*types.Transaction{txA})

	snap := tr.BouncingEntries()
	if len(snap) != 2 {
		t.Fatalf("snapshot len = %d; want 2", len(snap))
	}
	// Sorted by hash — find entry A.
	var infoA *BouncingEntryInfo
	for i := range snap {
		if snap[i].Hash == hashA {
			infoA = &snap[i]
		}
	}
	if infoA == nil {
		t.Fatalf("hashA missing from snapshot; got %+v", snap)
	}
	if got := infoA.AnnouncesBlocked; got != 2 {
		t.Errorf("A.AnnouncesBlocked = %d; want 2", got)
	}
	if got := infoA.BodiesBlocked; got != 1 {
		t.Errorf("A.BodiesBlocked = %d; want 1", got)
	}
	if got := infoA.PeersCount; got != 3 {
		t.Errorf("A.PeersCount = %d; want 3", got)
	}
	// Full peer list isn't in the snapshot — fetched per-hash on demand.
	wantPeers := []string{"peerX", "peerY", "peerZ"}
	gotPeers := tr.BouncingPeers(hashA)
	if !reflect.DeepEqual(gotPeers, wantPeers) {
		t.Errorf("BouncingPeers(A) = %v; want %v", gotPeers, wantPeers)
	}
	// Unknown hash returns nil, not an empty slice.
	if got := tr.BouncingPeers(common.Hash{0xff}); got != nil {
		t.Errorf("BouncingPeers(unknown) = %v; want nil", got)
	}

	// Confirm hash ordering is byte-lexicographic.
	for i := 1; i < len(snap); i++ {
		if bytes.Compare(snap[i-1].Hash.Bytes(), snap[i].Hash.Bytes()) >= 0 {
			t.Fatalf("snapshot not sorted by hash at index %d", i)
		}
	}
	_ = hashB
}

// TestGetTxBouncingFlag verifies the TxInfo.Bouncing flag tracks the
// bouncing map's live state: true while the entry exists, false (and
// JSON-omitted via omitempty) once the entry clears.
func TestGetTxBouncingFlag(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)

	info := tr.GetTx(hash)
	if info == nil {
		t.Fatalf("GetTx(bouncing-hash) = nil")
	}
	if !info.Bouncing {
		t.Errorf("Bouncing = false; want true while entry live")
	}

	// Force-clear the entry (mimic sender-inclusion).
	tr.deleteBouncing(hash, clearSenderInclusion)

	info = tr.GetTx(hash)
	if info == nil {
		t.Fatalf("GetTx after clear returned nil")
	}
	if info.Bouncing {
		t.Errorf("Bouncing = true after clear; want false")
	}
}

// TestFilterInboundBodiesPartitions verifies the body-intercept splits
// the incoming slice correctly and accounts blocked txs on their
// bouncing entry without bumping the non-bouncing txs.
func TestFilterInboundBodiesPartitions(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	keyA, _ := crypto.GenerateKey()
	keyB, _ := crypto.GenerateKey()
	txA := signedTx(t, keyA, 1)
	txB := signedTx(t, keyB, 1)

	// Drive A to bouncing; B stays clean.
	bouncingHash := driveBouncing(tr, "peerA", txA, core.RemovalRateLimited)

	toEnqueue, blocked := tr.FilterInboundBodies("peerC", []*types.Transaction{txA, txB})
	if len(toEnqueue) != 1 || toEnqueue[0].Hash() != txB.Hash() {
		t.Fatalf("toEnqueue=%v; want [txB]", toEnqueue)
	}
	if len(blocked) != 1 || blocked[0].Hash() != bouncingHash {
		t.Fatalf("blocked=%v; want [bouncingHash]", blocked)
	}

	v, _ := tr.bouncing.Load(bouncingHash)
	entry := v.(*bouncingEntry)
	if got := entry.bodiesBlocked.Load(); got != 1 {
		t.Errorf("bodiesBlocked = %d; want 1", got)
	}
	if _, ok := entry.peers["peerC"]; !ok {
		t.Errorf("distinct-peers set missing peerC; got %v", entry.peers)
	}
}

// TestFilterInboundBodiesWhatifPassesThrough verifies that with the
// master toggle OFF the filter returns the full slice in toEnqueue
// (matching the IsBouncing whatif semantics) and does not touch the
// per-entry counter.
func TestFilterInboundBodiesWhatifPassesThrough(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)

	tr.SetBouncingFlag("main", false)
	defer tr.SetBouncingFlag("main", true)

	toEnqueue, blocked := tr.FilterInboundBodies("peerB", []*types.Transaction{tx})
	if len(toEnqueue) != 1 || toEnqueue[0].Hash() != hash {
		t.Fatalf("toEnqueue=%v; want [tx]", toEnqueue)
	}
	if len(blocked) != 0 {
		t.Fatalf("blocked=%v; want empty under whatif", blocked)
	}
	v, _ := tr.bouncing.Load(hash)
	entry := v.(*bouncingEntry)
	if got := entry.bodiesBlocked.Load(); got != 0 {
		t.Errorf("whatif path bumped bodiesBlocked: got %d, want 0", got)
	}
}

// TestIsBouncingBumpsAnnouncesBlocked verifies that IsBouncing returning
// true increments the entry's announcesBlocked counter and adds the
// supplied peer to the distinct-peers set. The whatif path (main
// toggle OFF) must NOT bump the counter — only suppressions that
// actually took effect should count.
func TestIsBouncingBumpsAnnouncesBlocked(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)

	// Three live suppressions across two distinct peers.
	for i := 0; i < 2; i++ {
		if !tr.IsBouncing(hash, "peerA") {
			t.Fatalf("IsBouncing(peerA) = false; want true")
		}
	}
	if !tr.IsBouncing(hash, "peerB") {
		t.Fatalf("IsBouncing(peerB) = false; want true")
	}

	v, ok := tr.bouncing.Load(hash)
	if !ok {
		t.Fatalf("bouncing entry missing after suppressions")
	}
	entry := v.(*bouncingEntry)
	if got := entry.announcesBlocked.Load(); got != 3 {
		t.Errorf("announcesBlocked = %d; want 3", got)
	}
	if got := len(entry.peers); got != 2 {
		t.Errorf("distinct peers = %d; want 2", got)
	}

	// Main toggle OFF → IsBouncing returns false via the whatif path
	// and must NOT bump the counter.
	tr.SetBouncingFlag("main", false)
	defer tr.SetBouncingFlag("main", true)
	before := entry.announcesBlocked.Load()
	if tr.IsBouncing(hash, "peerC") {
		t.Fatalf("IsBouncing with main=false returned true; want false")
	}
	if got := entry.announcesBlocked.Load(); got != before {
		t.Errorf("announcesBlocked bumped on whatif path: %d → %d", before, got)
	}
	if _, seen := entry.peers["peerC"]; seen {
		t.Errorf("whatif path added peerC to distinct-peers set; should not")
	}
}

// TestBouncingEntryRecordHit verifies the per-entry distinct-peers set
// dedupes within the cap and silently ignores hits past the cap. Also
// asserts the atomic counter fields start at zero so callers that
// later increment them can rely on the freshly-constructed state.
func TestBouncingEntryRecordHit(t *testing.T) {
	e := &bouncingEntry{}

	if got := e.announcesBlocked.Load(); got != 0 {
		t.Errorf("freshly-constructed announcesBlocked = %d; want 0", got)
	}
	if got := e.bodiesBlocked.Load(); got != 0 {
		t.Errorf("freshly-constructed bodiesBlocked = %d; want 0", got)
	}

	// Empty peer is a no-op.
	e.recordHit("")
	if len(e.peers) != 0 {
		t.Errorf("empty peer recorded; len(peers)=%d", len(e.peers))
	}

	// Three distinct peers, one duplicate.
	e.recordHit("peerA")
	e.recordHit("peerB")
	e.recordHit("peerA")
	e.recordHit("peerC")
	if got := len(e.peers); got != 3 {
		t.Errorf("after 3 distinct + 1 dup recordHit, len(peers)=%d; want 3", got)
	}

	// Saturate the cap and assert further hits are silently dropped.
	for i := 0; len(e.peers) < bouncingMaxPeersPerEntry; i++ {
		e.recordHit("peer" + string(rune('0'+i%10)) + "-" + string(rune('a'+i/10)))
	}
	if got := len(e.peers); got != bouncingMaxPeersPerEntry {
		t.Fatalf("cap not reached: len(peers)=%d; want %d", got, bouncingMaxPeersPerEntry)
	}
	e.recordHit("over-the-cap")
	if got := len(e.peers); got != bouncingMaxPeersPerEntry {
		t.Errorf("recordHit past cap grew set: len(peers)=%d; want %d", got, bouncingMaxPeersPerEntry)
	}
}
