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
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// bouncingStatsHistBuckets defines the bucket count used by the
// lifetime and announces-blocked histograms. Each bucket i covers the
// log2 range [2^i, 2^(i+1)); the last bucket is the overflow.
//
//	Lifetime: 16 buckets ≈ 0..18 hours (2^15 ms ≈ 9 h, 2^16 ≈ 18 h).
//	Announces: 16 buckets ≈ 0..65 536 hits per entry (saturating).
const bouncingStatsHistBuckets = 16

// bouncingReasonLabels are the canonical category labels emitted by
// bouncingReasonCategory. Kept as a slice (not a map) so the order in
// the BouncingStats.InsertsByReason JSON is stable for clients.
var bouncingReasonLabels = []string{
	"rate_limited",
	"capacity",
	"replaced",
	"underpriced",
	"future_replace_pending",
	"txpool_full",
	"authority_reserved",
	"insufficient_funds",
}

// bouncingReasonCategory maps a raw rejection/drop reason string to a
// canonical category label used for stats aggregation. Returns the
// empty string for reasons that aren't covered by either
// bouncingDropProtected or bouncingRejectionProtected (those
// wouldn't enter the bouncing map in the first place).
func bouncingReasonCategory(reason string) string {
	switch reason {
	case "rate limited":
		return "rate_limited"
	case "capacity":
		return "capacity"
	case "replaced":
		return "replaced"
	case "underpriced":
		return "underpriced"
	}
	prefixes := []struct{ prefix, label string }{
		{"future transaction tries to replace pending", "future_replace_pending"},
		{"txpool is full", "txpool_full"},
		{"authority already reserved", "authority_reserved"},
		{"insufficient funds for gas", "insufficient_funds"},
	}
	for _, p := range prefixes {
		if strings.HasPrefix(reason, p.prefix) {
			return p.label
		}
	}
	return ""
}

// bouncingClearReason discriminates the path by which a bouncing entry
// left the map. Tracked via the per-path counters on bouncingStatsAcc
// so operators can see whether sender-inclusion (the desirable signal),
// TTL expiry, FIFO eviction (memory pressure), or fee-gate clearing is
// the dominant clear path.
type bouncingClearReason uint8

const (
	clearSenderInclusion bouncingClearReason = iota + 1
	clearTTL
	clearFIFO
	clearFeeGate
)

// bouncingStatsAcc accumulates server-side aggregates over the lifetime
// of the tracker process. All fields use atomics or are guarded by mu
// so the snapshot path (BouncingStats) can read without coordinating
// with the protection hot paths. Per-entry counters (announcesBlocked,
// bodiesBlocked) live on bouncingEntry; the cumulative totals folded
// in here capture work done by entries that have since departed,
// preserving the "since startup" semantics.
type bouncingStatsAcc struct {
	// Cumulative inserts by source. Mirror the existing
	// bouncingInsertedDropMeter / Reject; restated here so the RPC
	// snapshot is self-contained (geth metrics are a separate pipe).
	cumInsertsDrop   atomic.Uint64
	cumInsertsReject atomic.Uint64

	// Cumulative suppression activity. Live entries' per-entry
	// counters are added in at snapshot time; values here capture
	// the contributions of entries that have already departed so
	// the totals don't reset on each clear.
	cumAnnouncesBlockedDeparted atomic.Uint64
	cumBodiesBlockedDeparted    atomic.Uint64

	// Per-clear-path counters.
	clearsBySenderInclusion atomic.Uint64
	clearsByTTL             atomic.Uint64
	clearsByFIFO            atomic.Uint64
	clearsByFeeGate         atomic.Uint64

	// Per-reason insert counters. Indexed by bouncingReasonLabels
	// order; insertsByReasonOther is the catch-all.
	insertsByReason      [8]atomic.Uint64
	insertsByReasonOther atomic.Uint64

	// Histograms over departed entries. lifetimeMs[i] counts entries
	// whose lifetime fell in [2^i, 2^(i+1)) ms; announces[i] counts
	// entries whose final announcesBlocked fell in [2^i, 2^(i+1));
	// last bucket is the overflow.
	lifetimeMs [bouncingStatsHistBuckets]atomic.Uint64
	announces  [bouncingStatsHistBuckets]atomic.Uint64

	// markedAt remembers when each currently-live entry was inserted.
	// On removal we look up the start time, compute the lifetime, and
	// drop the entry. Lock-protected map keyed by hash. Bounded by
	// the live bouncing-map size.
	mu       sync.Mutex
	markedAt map[[32]byte]time.Time
}

func newBouncingStatsAcc() *bouncingStatsAcc {
	return &bouncingStatsAcc{markedAt: make(map[[32]byte]time.Time)}
}

// recordInsert is called once per successful bouncing-map insert
// (after the bouncing.Swap succeeds in markBouncing). reason is the
// raw reason string; bouncingReasonCategory canonicalises.
func (a *bouncingStatsAcc) recordInsert(hash [32]byte, source bouncingSource, reason string, now time.Time) {
	switch source {
	case bouncingFromDrop:
		a.cumInsertsDrop.Add(1)
	case bouncingFromReject:
		a.cumInsertsReject.Add(1)
	}
	cat := bouncingReasonCategory(reason)
	matched := false
	for i, label := range bouncingReasonLabels {
		if label == cat {
			a.insertsByReason[i].Add(1)
			matched = true
			break
		}
	}
	if !matched {
		a.insertsByReasonOther.Add(1)
	}
	a.mu.Lock()
	a.markedAt[hash] = now
	a.mu.Unlock()
}

// recordRemove is called by deleteBouncing whenever an entry actually
// leaves the map. The departed per-entry counters are folded into the
// "departed" cumulative totals (so live + departed = lifetime totals).
// Lifetime and announces histograms are bucketed.
func (a *bouncingStatsAcc) recordRemove(hash [32]byte, e *bouncingEntry, why bouncingClearReason, now time.Time) {
	if e == nil {
		return
	}
	switch why {
	case clearSenderInclusion:
		a.clearsBySenderInclusion.Add(1)
	case clearTTL:
		a.clearsByTTL.Add(1)
	case clearFIFO:
		a.clearsByFIFO.Add(1)
	case clearFeeGate:
		a.clearsByFeeGate.Add(1)
	}
	ann := e.announcesBlocked.Load()
	bod := e.bodiesBlocked.Load()
	a.cumAnnouncesBlockedDeparted.Add(ann)
	a.cumBodiesBlockedDeparted.Add(bod)

	a.mu.Lock()
	t0, ok := a.markedAt[hash]
	if ok {
		delete(a.markedAt, hash)
	}
	a.mu.Unlock()

	if ok {
		ms := uint64(now.Sub(t0).Milliseconds())
		idx := log2Bucket(ms)
		a.lifetimeMs[idx].Add(1)
	}
	idx := log2Bucket(ann)
	a.announces[idx].Add(1)
}

// log2Bucket returns floor(log2(v)) clamped to [0, bouncingStatsHistBuckets-1].
// log2(0) is mapped to 0 so v=0 lands in bucket 0.
func log2Bucket(v uint64) int {
	if v == 0 {
		return 0
	}
	b := bits.Len64(v) - 1
	if b >= bouncingStatsHistBuckets {
		return bouncingStatsHistBuckets - 1
	}
	return b
}

// BouncingStats is the aggregate snapshot returned by
// Tracker.BouncingStats() and the txtracker_bouncingStats JSON-RPC.
// Numeric fields are cumulative-since-startup except LiveEntries
// (instantaneous live count) and LiveAnnounces/BodiesBlocked
// (instantaneous sum across live entries). LiveAnnounces/Bodies +
// CumAnnounces/BodiesBlockedDeparted gives the lifetime totals.
type BouncingStats struct {
	// Live-snapshot fields.
	LiveEntries          int    `json:"liveEntries"`
	LiveAnnouncesBlocked uint64 `json:"liveAnnouncesBlocked"`
	LiveBodiesBlocked    uint64 `json:"liveBodiesBlocked"`

	// Cumulative since startup. CumAnnouncesBlocked /
	// CumBodiesBlocked include both live and departed entries' work,
	// so they're the "total suppressions since startup" headline.
	CumInsertsDrop      uint64 `json:"cumInsertsDrop"`
	CumInsertsReject    uint64 `json:"cumInsertsReject"`
	CumAnnouncesBlocked uint64 `json:"cumAnnouncesBlocked"`
	CumBodiesBlocked    uint64 `json:"cumBodiesBlocked"`

	// Cumulative inserts grouped by canonical reason category. The
	// keys are stable strings drawn from bouncingReasonLabels plus
	// "other" for the catch-all.
	InsertsByReason map[string]uint64 `json:"insertsByReason"`

	// Cumulative entry removals grouped by clear-path. Adds up to
	// CumInsertsDrop + CumInsertsReject minus the number of live
	// entries (i.e., total removals).
	ClearsBySenderInclusion uint64 `json:"clearsBySenderInclusion"`
	ClearsByTTL             uint64 `json:"clearsByTTL"`
	ClearsByFIFO            uint64 `json:"clearsByFIFO"`
	ClearsByFeeGate         uint64 `json:"clearsByFeeGate"`

	// Histograms over departed entries. bucket[i] counts entries
	// whose value fell in [2^i, 2^(i+1)) — milliseconds for
	// LifetimeBucketsMs, hit-count for AnnouncesBlockedBuckets. The
	// last bucket is the overflow.
	LifetimeBucketsMs       [bouncingStatsHistBuckets]uint64 `json:"lifetimeBucketsMs"`
	AnnouncesBlockedBuckets [bouncingStatsHistBuckets]uint64 `json:"announcesBlockedBuckets"`
}

// snapshot builds a BouncingStats reading every field. Live-side
// totals (liveEntries, liveAnnouncesBlocked, liveBodiesBlocked) are
// computed by walking the bouncing sync.Map — bounded by the live
// entry count, much cheaper than scanning a 262 144-cap map every
// poll because typical live sets are 100s-1000s.
func (a *bouncingStatsAcc) snapshot(t *Tracker) BouncingStats {
	var liveAnn, liveBod uint64
	var liveCount int
	t.bouncing.Range(func(_, v any) bool {
		e := v.(*bouncingEntry)
		liveAnn += e.announcesBlocked.Load()
		liveBod += e.bodiesBlocked.Load()
		liveCount++
		return true
	})

	insertsByReason := make(map[string]uint64, len(bouncingReasonLabels)+1)
	for i, label := range bouncingReasonLabels {
		if v := a.insertsByReason[i].Load(); v > 0 {
			insertsByReason[label] = v
		}
	}
	if v := a.insertsByReasonOther.Load(); v > 0 {
		insertsByReason["other"] = v
	}

	out := BouncingStats{
		LiveEntries:             liveCount,
		LiveAnnouncesBlocked:    liveAnn,
		LiveBodiesBlocked:       liveBod,
		CumInsertsDrop:          a.cumInsertsDrop.Load(),
		CumInsertsReject:        a.cumInsertsReject.Load(),
		CumAnnouncesBlocked:     liveAnn + a.cumAnnouncesBlockedDeparted.Load(),
		CumBodiesBlocked:        liveBod + a.cumBodiesBlockedDeparted.Load(),
		InsertsByReason:         insertsByReason,
		ClearsBySenderInclusion: a.clearsBySenderInclusion.Load(),
		ClearsByTTL:             a.clearsByTTL.Load(),
		ClearsByFIFO:            a.clearsByFIFO.Load(),
		ClearsByFeeGate:         a.clearsByFeeGate.Load(),
	}
	for i := range a.lifetimeMs {
		out.LifetimeBucketsMs[i] = a.lifetimeMs[i].Load()
	}
	for i := range a.announces {
		out.AnnouncesBlockedBuckets[i] = a.announces[i].Load()
	}
	return out
}
