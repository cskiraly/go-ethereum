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
	"errors"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
)

// bouncingSource distinguishes the path that produced a bouncing
// entry, so the inserted/* counters break it down without growing
// the entry struct.
type bouncingSource uint8

const (
	bouncingFromDrop   bouncingSource = iota // post-acceptance pool eviction
	bouncingFromReject                       // submission-time rejection
)

// PoolFloor reports the effective-tip floor a new tx must clear to be
// admitted to the local pool at a given base fee. Implemented by
// core/txpool/legacypool.LegacyPool.MinTip; supplied via SetPoolFloor.
// When unset, the bouncing-clear path falls back to stage-1 behaviour
// (sender-inclusion clears regardless of fee).
type PoolFloor interface {
	MinTip(baseFee uint64) uint64
}

// bouncingEntry records a hash that the local pool just evicted for an
// overflow-style reason ("rate limited", "capacity", "replaced"). The
// fetcher consults the tracker via IsBouncing to suppress re-fetch of
// the hash while the entry is live, since a re-fetch would just bounce
// off the pool again.
//
// Lifecycle:
//   - Inserted from notifyHashReason on transition to StatusDropped
//     (see Tracker.markBouncing).
//   - Cleared per-sender from handleChainHead when any of the sender's
//     txs lands on chain. The fee gate (stage 2) gates that clear:
//     when a PoolFloor is wired, the entry only clears if its
//     effective tip at current base fee meets the floor — otherwise
//     re-fetching would just bounce off as underpriced.
//   - For single-tx-at-drop entries (sender had no other Pooled tx
//     when the eviction transitioned), additionally cleared by TTL
//     expiry. Multi-tx entries have until=zero and rely on
//     sender-inclusion.
//   - Cleared on FIFO eviction of the underlying TxInfo.
type bouncingEntry struct {
	sender    common.Address
	until     time.Time // zero = no TTL (multi-tx); nonzero = TTL expiry (single-tx)
	gasFeeCap *big.Int  // stored for the stage-2 fee gate; nil means skip the gate
	gasTipCap *big.Int

	// Per-entry counters bumped by the protection paths. Sum-since-mark
	// metrics, surfaced via the bouncingEntries RPC; cleared when the
	// entry leaves the map (sender-inclusion, TTL, FIFO eviction).
	announcesBlocked atomic.Uint64 // IsBouncing-true on the fetcher announce path
	bodiesBlocked    atomic.Uint64 // pushed-body intercept (Transactions / PooledTransactions)

	// Distinct peers that have hit this entry on either path. Capped at
	// bouncingMaxPeersPerEntry to bound memory under a noisy storm; the
	// peer counters above continue to climb past the cap.
	peersMu sync.Mutex
	peers   map[string]struct{}
}

// bouncingMaxPeersPerEntry caps the size of the per-entry peers set so
// a single hash being hammered by every peer on the network can't blow
// memory. Realistic storms surface 10-30 distinct peers; 64 leaves
// headroom without going unbounded.
const bouncingMaxPeersPerEntry = 64

// recordHit appends peer to the entry's distinct-peers set if there is
// room and peer is non-empty. Lock-protected; called from the IsBouncing
// announce path and the body-intercept path. Counter bumps remain on
// the call sites so callers can choose which counter (announcesBlocked vs
// bodiesBlocked) to mark.
func (e *bouncingEntry) recordHit(peer string) {
	if peer == "" {
		return
	}
	e.peersMu.Lock()
	defer e.peersMu.Unlock()
	if e.peers == nil {
		e.peers = make(map[string]struct{}, 4)
	}
	if _, ok := e.peers[peer]; ok {
		return
	}
	if len(e.peers) >= bouncingMaxPeersPerEntry {
		return
	}
	e.peers[peer] = struct{}{}
}

// bouncingSingleTxTTL caps the cooldown for entries inserted when the
// sender had no other pending tx in the local view. Mirrors the fetcher's
// underpriced LRU TTL. Multi-tx entries have no TTL — they wait for
// sender-inclusion to fire.
const bouncingSingleTxTTL = 5 * time.Minute

// bouncingDropProtected reports whether a pool-eviction reason should
// trigger anti-bounce protection. The set is the overflow-style reasons
// where peer re-announces would otherwise loop a hash through fetch →
// pool admit → eviction repeatedly:
//
//   - "rate limited" — legacypool truncatePending account-fairness
//     and cap passes (core/txpool/legacypool/legacypool.go).
//   - "capacity"     — legacypool global cap; blobpool overflow.
//   - "replaced"     — old hash superseded; the new hash is fine, the
//     OLD hash should not be re-fetched until the sender lands a tx
//     (which moves the slot forward).
//   - "underpriced"  — base-fee jumped above the tx's fee cap and the
//     pool demoted/dropped it. The fetcher's submission-time
//     underpriced LRU does NOT catch this case (it only fires when
//     pool.Add rejects, not when an already-accepted tx is evicted),
//     so without this entry every re-announce wastes a fetch +
//     re-submit round-trip until the LRU is populated by the
//     resulting ErrUnderpriced. The fee-gate (stage 2) is the
//     natural clear path here: when the sender lands a new tx, we
//     re-evaluate effective tip vs the current floor and clear
//     only if fees have recovered.
//
// Other reasons ("expired", "nonce expired", "underfunded",
// "gas limit exceeded", "invalid") are re-validated by pool.add() on
// retry, so they self-protect without a fetcher-side cache.
func bouncingDropProtected(reason core.RemovalReason) bool {
	switch reason {
	case core.RemovalRateLimited, core.RemovalCapacity, core.RemovalReplaced, core.RemovalUnderpriced:
		return true
	}
	return false
}

// bouncingRejectionProtected matches submission-time rejection reasons
// (NotifyRejected → ObsPoolRejected) that have the same bounce-loop
// shape as post-acceptance overflow drops: a refetch from another peer
// would hit the exact same wall until the sender's earlier txs land
// or pool capacity recovers. The handler forwards the pool.Add error
// itself, so matching is typed via errors.Is — a wording change in
// the error text cannot silently alter protection.
//
// Covered errors:
//   - txpool.ErrFutureReplacePending    — gapped tx tried to displace
//     a pending one in a full pool. Clears when sender lands earlier
//     nonces or pool drains.
//   - txpool.ErrTxPoolOverflow          — pool is full and Discard
//     couldn't make room. Clears on pool drain or sender churn.
//   - txpool.ErrAuthorityReserved       — tx authorization conflicts
//     with an in-flight tx from the authorizing address. Clears when
//     the conflicting tx leaves the pool.
//   - core.ErrInsufficientFunds         — sender's balance is too low
//     for `gas * gasPrice + value`. Recoverable when the sender's
//     balance rises (either they top up and resubmit, or some other
//     activity from them lands and sender-inclusion clears their
//     entries). Without this, every peer re-announce wastes
//     a fetch + pool-rejection round; image03-style traffic showed
//     dozens of cycles per affected hash before this addition.
//     core/txpool/validation.go wraps the sentinel with %w when it
//     adds balance/cost detail, so errors.Is sees through the text.
func bouncingRejectionProtected(err error) bool {
	return errors.Is(err, txpool.ErrFutureReplacePending) ||
		errors.Is(err, txpool.ErrTxPoolOverflow) ||
		errors.Is(err, txpool.ErrAuthorityReserved) ||
		errors.Is(err, core.ErrInsufficientFunds)
}

// IsBouncing reports whether the fetcher should suppress a re-fetch for
// hash. Cheap, lock-free read path — the announce hot path does not
// touch t.mu. Returns false for hashes the tracker has never seen, and
// for entries whose single-tx TTL has expired.
//
// The peer arg lets the entry's per-entry stats record which peer
// triggered the suppression (distinct-peers set + announcesBlocked
// counter). Empty peer is allowed and skips the per-peer accounting.
func (t *Tracker) IsBouncing(hash common.Hash, peer string) bool {
	v, ok := t.bouncing.Load(hash)
	if !ok {
		return false
	}
	entry := v.(*bouncingEntry)
	if !entry.until.IsZero() && time.Now().After(entry.until) {
		t.deleteBouncing(hash, clearTTL)
		return false
	}
	entry.announcesBlocked.Add(1)
	entry.recordHit(peer)
	if t.consumer != nil {
		t.consumer.NotifyBouncingBlocked(peer, 1)
	}
	return true
}

// FilterInboundBodies splits the incoming tx slice into a "to-enqueue"
// subset (safe to hand to the txpool) and a "blocked" subset (hashes
// the tracker has flagged bouncing). For each blocked tx the entry's
// bodiesBlocked counter is bumped and recordHit(peer) is called so the
// per-entry distinct-peers set captures which peers keep pushing the
// bouncing body.
//
// The caller is expected to invoke NotifyReceived on the FULL txs
// slice (not just toEnqueue), so ObsDeliveredInbound and the
// ReceivedAfterTerminal counter still fire for every body the peer
// actually pushed. The only thing this method gates is whether the
// pool sees the body.
//
// Lock-free: sync.Map.Load + atomic.Add on the hot path; the
// per-entry peers set takes a brief per-entry mutex.
func (t *Tracker) FilterInboundBodies(peer string, txs []*types.Transaction) (toEnqueue, blocked []*types.Transaction) {
	if len(txs) == 0 {
		return txs, nil
	}
	toEnqueue = make([]*types.Transaction, 0, len(txs))
	for _, tx := range txs {
		hash := tx.Hash()
		v, ok := t.bouncing.Load(hash)
		if !ok {
			toEnqueue = append(toEnqueue, tx)
			continue
		}
		entry := v.(*bouncingEntry)
		if !entry.until.IsZero() && time.Now().After(entry.until) {
			t.deleteBouncing(hash, clearTTL)
			toEnqueue = append(toEnqueue, tx)
			continue
		}
		entry.bodiesBlocked.Add(1)
		entry.recordHit(peer)
		blocked = append(blocked, tx)
	}
	if t.consumer != nil && len(blocked) > 0 {
		t.consumer.NotifyBouncingBlocked(peer, len(blocked))
	}
	return toEnqueue, blocked
}

// deleteBouncing removes hash from the bouncing map and decrements the
// size gauge + atomic counter if the entry was actually present. All
// bouncing-map removals (TTL expiry on IsBouncing, sender-inclusion
// clear, FIFO eviction, fee-gate clear) route through this helper so
// the gauge, the bouncingCount counter, and the cumulative stats
// accumulator stay accurate. why labels which clear-path the removal
// came from so the stats RPC can show the per-path breakdown.
func (t *Tracker) deleteBouncing(hash common.Hash, why bouncingClearReason) {
	if v, loaded := t.bouncing.LoadAndDelete(hash); loaded {
		bouncingSizeGauge.Dec(1)
		t.bouncingCount.Add(-1)
		if e, ok := v.(*bouncingEntry); ok && t.bouncingStats != nil {
			t.bouncingStats.recordRemove(hash, e, why, time.Now())
		}
	}
}

// markBouncing inserts a bouncing entry for hash, keyed by the sender
// recovered from TxInfo.From. Must be called with t.mu held; reads
// t.pooledBySender to decide whether to apply the single-tx TTL and
// snapshots the fee fields off TxInfo for the stage-2 fee gate. The
// source argument labels the inserted/* counter without affecting
// the entry itself. reason is the raw rejection / drop reason —
// canonicalised inside the stats accumulator for per-reason
// breakdowns.
//
// No-op if From is the zero address (sender unknown — body never seen,
// shouldn't happen for a hash the pool processed but kept defensive).
func (t *Tracker) markBouncing(hash common.Hash, ti *TxInfo, now time.Time, source bouncingSource, reason string) {
	if ti.From == (common.Address{}) {
		return
	}
	until := time.Time{}
	if t.pooledBySender[ti.From] == 0 {
		until = now.Add(bouncingSingleTxTTL)
	}
	var gasFeeCap, gasTipCap *big.Int
	if ti.GasFeeCap != nil {
		gasFeeCap = (*big.Int)(ti.GasFeeCap)
	}
	if ti.GasTipCap != nil {
		gasTipCap = (*big.Int)(ti.GasTipCap)
	}
	if _, loaded := t.bouncing.Swap(hash, &bouncingEntry{
		sender:    ti.From,
		until:     until,
		gasFeeCap: gasFeeCap,
		gasTipCap: gasTipCap,
	}); !loaded {
		bouncingSizeGauge.Inc(1)
		t.bouncingCount.Add(1)
		switch source {
		case bouncingFromDrop:
			bouncingInsertedDropMeter.Mark(1)
		case bouncingFromReject:
			bouncingInsertedRejectMeter.Mark(1)
		}
		if t.bouncingStats != nil {
			t.bouncingStats.recordInsert(hash, source, reason, now)
		}
	}
}

// clearBouncingForSenders walks the bouncing map and removes entries
// whose sender is in senders, gated on the fee-floor check when a
// PoolFloor is wired. Called from handleChainHead after a block has
// been processed: any tx landing on chain frees its sender's pool
// slot, so bouncing hashes from that sender become candidates for a
// fresh fetch — provided they would actually fit the pool today.
//
// Lock-free; safe to call without t.mu.
func (t *Tracker) clearBouncingForSenders(senders map[common.Address]struct{}, baseFee uint64) {
	if len(senders) == 0 {
		return
	}
	floor := uint64(0)
	gateOn := false
	if t.poolFloor != nil {
		floor = t.poolFloor.MinTip(baseFee)
		gateOn = true
	}
	t.bouncing.Range(func(k, v any) bool {
		entry := v.(*bouncingEntry)
		if _, ok := senders[entry.sender]; !ok {
			return true
		}
		if gateOn && bouncingEffectiveTip(entry.gasFeeCap, entry.gasTipCap, baseFee) < floor {
			// Sender slot freed but tx fee is below current pool
			// floor — refetching would just bounce off as
			// underpriced. Keep the entry blocked.
			bouncingGateBlockedMeter.Mark(1)
			return true
		}
		hash, _ := k.(common.Hash)
		t.deleteBouncing(hash, clearSenderInclusion)
		bouncingClearedMeter.Mark(1)
		return true
	})
}

// bouncingEffectiveTip is the effective miner tip the entry's tx would
// pay at baseFee: min(gasTipCap, gasFeeCap - baseFee), saturated at
// 0 if gasFeeCap < baseFee. Returns 0 when fee fields are missing,
// which reads as "below any positive floor" → keep blocked.
func bouncingEffectiveTip(gasFeeCap, gasTipCap *big.Int, baseFee uint64) uint64 {
	if gasFeeCap == nil || gasTipCap == nil {
		return 0
	}
	margin := new(big.Int).Sub(gasFeeCap, new(big.Int).SetUint64(baseFee))
	if margin.Sign() < 0 {
		return 0
	}
	if margin.Cmp(gasTipCap) > 0 {
		margin = gasTipCap
	}
	if !margin.IsUint64() {
		return ^uint64(0)
	}
	return margin.Uint64()
}

// SetPoolFloor installs an optional PoolFloor consulted in the
// per-block bouncing-clear sweep. Must be called before Start; the
// field is read from the chain-head goroutine without locking. nil
// disables the fee gate (stage-1 fallback).
func (t *Tracker) SetPoolFloor(p PoolFloor) {
	t.poolFloor = p
}

// BouncingEntryInfo is the public snapshot of a single bouncing-map
// entry, returned by Tracker.BouncingEntries and surfaced via the
// txtracker_bouncingEntries RPC. The shape is intentionally minimal:
// hash + sender + fee fields for context, the two saved-bounce
// counters, and the *size* of the distinct-peers set. The full peer
// list is omitted from the snapshot to keep the per-entry payload
// small enough to scale into the hundreds of entries without
// blowing the WS frame budget; callers that need the names use
// txtracker_bouncingPeers(hash) for on-demand lookup. `Until` is
// the TTL expiry for single-tx entries (zero for multi-tx).
type BouncingEntryInfo struct {
	Hash             common.Hash    `json:"hash"`
	Sender           common.Address `json:"sender"`
	Until            time.Time      `json:"until,omitempty"`
	GasFeeCap        *hexutil.Big   `json:"gasFeeCap,omitempty"`
	GasTipCap        *hexutil.Big   `json:"gasTipCap,omitempty"`
	AnnouncesBlocked uint64         `json:"announcesBlocked"`
	BodiesBlocked    uint64         `json:"bodiesBlocked"`
	PeersCount       int            `json:"peersCount"`
}

// bouncingEntriesMaxResponse caps the number of entries the snapshot
// RPC returns. Each entry can carry up to bouncingMaxPeersPerEntry
// (64) enode IDs, so an unbounded snapshot on a node with thousands
// of bouncing hashes can easily exceed the WS frame limits (geth's
// 32 MiB cap, the lens proxy's 16 MiB). The cap selects the top
// entries by total suppressions (announces + bodies); the long tail
// — entries with low block counts — is the least useful for both
// the lens stats card and the badge map.
const bouncingEntriesMaxResponse = 500

// BouncingEntries returns a snapshot of the bouncing map. To keep the
// response within sensible WS frame limits when the bouncing map has
// thousands of entries (each carrying up to 64 peer IDs), the snapshot
// is capped at bouncingEntriesMaxResponse entries, prioritised by
// total suppressions (announces + bodies blocked). Atomic-loads the
// per-entry counters and copies the peers set under its mutex so the
// returned slice is fully independent of subsequent tracker state.
//
// Within the cap the slice is sorted by hash for stable ordering
// across polls (so the lens doesn't see entries reshuffle each tick).
//
// Lock-free with respect to t.mu; only sync.Map.Range plus brief
// per-entry mutex acquisitions.
func (t *Tracker) BouncingEntries() []BouncingEntryInfo {
	var out []BouncingEntryInfo
	t.bouncing.Range(func(k, v any) bool {
		hash, _ := k.(common.Hash)
		entry := v.(*bouncingEntry)
		info := BouncingEntryInfo{
			Hash:             hash,
			Sender:           entry.sender,
			Until:            entry.until,
			AnnouncesBlocked: entry.announcesBlocked.Load(),
			BodiesBlocked:    entry.bodiesBlocked.Load(),
		}
		if entry.gasFeeCap != nil {
			info.GasFeeCap = (*hexutil.Big)(new(big.Int).Set(entry.gasFeeCap))
		}
		if entry.gasTipCap != nil {
			info.GasTipCap = (*hexutil.Big)(new(big.Int).Set(entry.gasTipCap))
		}
		entry.peersMu.Lock()
		info.PeersCount = len(entry.peers)
		entry.peersMu.Unlock()
		out = append(out, info)
		return true
	})
	if len(out) > bouncingEntriesMaxResponse {
		sort.Slice(out, func(i, j int) bool {
			ti := out[i].AnnouncesBlocked + out[i].BodiesBlocked
			tj := out[j].AnnouncesBlocked + out[j].BodiesBlocked
			return ti > tj
		})
		out = out[:bouncingEntriesMaxResponse]
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Hash.Bytes(), out[j].Hash.Bytes()) < 0
	})
	return out
}

// BouncingPeers returns the sorted list of distinct peers recorded on
// the bouncing entry for hash. Returns nil for hashes not in the
// bouncing map (or with an empty peer set). Companion to
// BouncingEntries, which carries only the peer *count* per entry —
// callers fetch the full peer list per-hash on demand (e.g. lens
// detail panel) instead of every poll.
//
// Lock-free w.r.t. t.mu; takes the per-entry mutex briefly to copy.
func (t *Tracker) BouncingPeers(hash common.Hash) []string {
	v, ok := t.bouncing.Load(hash)
	if !ok {
		return nil
	}
	entry := v.(*bouncingEntry)
	entry.peersMu.Lock()
	defer entry.peersMu.Unlock()
	if len(entry.peers) == 0 {
		return nil
	}
	out := make([]string, 0, len(entry.peers))
	for p := range entry.peers {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// BouncingStats returns aggregate counters over the bouncing map.
// See bouncing_stats.go for the type definition. Walks the live
// sync.Map for the live-snapshot fields (LiveEntries,
// LiveAnnouncesBlocked, LiveBodiesBlocked); cumulative fields are
// atomic loads. Cheap enough for 1-5s polling from a UI.
func (t *Tracker) BouncingStats() BouncingStats {
	if t.bouncingStats == nil {
		return BouncingStats{InsertsByReason: map[string]uint64{}}
	}
	return t.bouncingStats.snapshot(t)
}

// updatePooledCount adjusts t.pooledBySender for a status transition.
// Must be called with t.mu held, after maybeTransition has set the new
// status.
func (t *Tracker) updatePooledCount(from common.Address, oldStatus, newStatus TxStatus) {
	if from == (common.Address{}) {
		return
	}
	if oldStatus == StatusPooled && newStatus != StatusPooled {
		t.pooledBySender[from]--
		if t.pooledBySender[from] <= 0 {
			delete(t.pooledBySender, from)
		}
	}
	if oldStatus != StatusPooled && newStatus == StatusPooled {
		t.pooledBySender[from]++
	}
}
