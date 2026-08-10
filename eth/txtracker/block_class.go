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
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
)

// PeerCoverageFunc reports, for a given chain-included tx, how many of
// `total` currently-connected eth peers have this hash in their per-peer
// known-tx cache (`knowing`). Wired by the eth backend against its
// peerSet at tracker construction; nil means the peerset proxy is
// disabled (e.g. tests that don't simulate peers).
//
// Independent of the tracker's own observation hooks: a hash can land
// in a peer's knownTxs without ever firing NotifyAnnounced /
// NotifyReceived (pool dedup, fetcher drops, outbound-only paths). The
// gap between this signal and the tracker's classification surfaces
// tracker-coverage holes vs raw eth-protocol traffic.
type PeerCoverageFunc func(hash common.Hash) (total, knowing int)

// BlockClassBucket is one of four mutually-exclusive labels for a tx
// observed at chain inclusion, derived from the tracker's authoritative
// pre-block state. Mirrors the lens-side Stats "Included by submission
// path" buckets so the two views can be cross-checked.
type BlockClassBucket uint8

const (
	BlockClassPrivate BlockClassBucket = iota
	BlockClassAnnouncedOnly
	BlockClassBounced
	BlockClassPooledClean
)

// classifyBlockInclusion routes an included tx into one of the four
// buckets using only TxInfo fields already maintained by the tracker.
// A nil ti means the tracker had no record of this hash before the
// block — chain-only / private-relay path.
//
//	hadPool    : tx reached our pool at some point (Pooled.IsZero == false)
//	hadHint    : tracker saw any pre-block signal (request / receive / announce)
//	wasBounced : tx reached a terminal pool state (Dropped / Rejected)
//	             or has cycled back from one (Returns > 0)
func classifyBlockInclusion(ti *TxInfo) BlockClassBucket {
	if ti == nil {
		return BlockClassPrivate
	}
	hadPool := !ti.Pooled.IsZero()
	hadHint := !ti.Requested.IsZero() || !ti.Received.IsZero() || len(ti.Announcers) > 0
	wasBounced := !ti.Dropped.IsZero() || ti.RejectErr != "" || ti.Returns > 0
	switch {
	case hadPool && wasBounced:
		return BlockClassBounced
	case hadPool:
		return BlockClassPooledClean
	case hadHint:
		return BlockClassAnnouncedOnly
	default:
		return BlockClassPrivate
	}
}

// blockClassAcc accumulates per-tx classification counts across the
// lifetime of the tracker. All counters are atomic so the snapshot
// reader does not need to hold t.mu. Recording is hot-path-cheap
// (one inc per included tx).
type blockClassAcc struct {
	total         atomic.Uint64
	private       atomic.Uint64
	announcedOnly atomic.Uint64
	bounced       atomic.Uint64
	pooledClean   atomic.Uint64

	// Source B — peerset proxy. peersetKnown counts included txs where
	// at least one connected peer had the hash in their knownTxs cache
	// at block-import time. peersetUnknown is the complement. The Sum
	// pair lets a snapshot reader derive a mean coverage ratio
	// (peersetKnowingSum / peersetTotalSum) without per-tx float math
	// on the hot path. peersetSampled counts txs that produced a
	// non-trivial sample (total > 0); a tracker with no peers can't
	// distinguish "no peer knew" from "no peers connected".
	peersetSampled    atomic.Uint64
	peersetKnown      atomic.Uint64
	peersetUnknown    atomic.Uint64
	peersetKnowingSum atomic.Uint64
	peersetTotalSum   atomic.Uint64
}

func newBlockClassAcc() *blockClassAcc { return &blockClassAcc{} }

func (a *blockClassAcc) record(b BlockClassBucket) {
	a.total.Add(1)
	switch b {
	case BlockClassPrivate:
		a.private.Add(1)
		blockClassPrivateMeter.Mark(1)
	case BlockClassAnnouncedOnly:
		a.announcedOnly.Add(1)
		blockClassAnnouncedOnlyMeter.Mark(1)
	case BlockClassBounced:
		a.bounced.Add(1)
		blockClassBouncedMeter.Mark(1)
	case BlockClassPooledClean:
		a.pooledClean.Add(1)
		blockClassPooledCleanMeter.Mark(1)
	}
	blockClassTotalMeter.Mark(1)
}

// BlockClassStats is the cumulative-since-startup snapshot returned by
// the txtracker_blockClassStats RPC. Each counter is the lifetime number
// of included txs that fell into the corresponding bucket. Sum of the
// four buckets equals Total. The Peerset* fields carry Source B (peerset
// proxy) totals; zero across the board when no PeerCoverageFunc is
// wired.
type BlockClassStats struct {
	Total         uint64 `json:"total"`
	Private       uint64 `json:"private"`
	AnnouncedOnly uint64 `json:"announcedOnly"`
	Bounced       uint64 `json:"bounced"`
	PooledClean   uint64 `json:"pooledClean"`

	PeersetSampled    uint64 `json:"peersetSampled,omitempty"`
	PeersetKnown      uint64 `json:"peersetKnown,omitempty"`
	PeersetUnknown    uint64 `json:"peersetUnknown,omitempty"`
	PeersetKnowingSum uint64 `json:"peersetKnowingSum,omitempty"`
	PeersetTotalSum   uint64 `json:"peersetTotalSum,omitempty"`
}

// recordPeerCoverage updates Source B counters from one
// PeerCoverageFunc result. total==0 (no peers connected) is treated as
// no sample — we can't distinguish "no peer knew" from "no peers".
func (a *blockClassAcc) recordPeerCoverage(total, knowing int) {
	if total <= 0 {
		return
	}
	a.peersetSampled.Add(1)
	a.peersetTotalSum.Add(uint64(total))
	a.peersetKnowingSum.Add(uint64(knowing))
	if knowing > 0 {
		a.peersetKnown.Add(1)
		blockClassPeersetKnownMeter.Mark(1)
	} else {
		a.peersetUnknown.Add(1)
		blockClassPeersetUnknownMeter.Mark(1)
	}
	// Coverage histogram in basis points (knowing*10000/total). Bounded
	// 0..10000 so an exp-decay histogram bucket cleanly.
	blockClassPeersetCoverageBpHist.Update(int64(knowing) * 10000 / int64(total))
}

func (a *blockClassAcc) snapshot() BlockClassStats {
	return BlockClassStats{
		Total:             a.total.Load(),
		Private:           a.private.Load(),
		AnnouncedOnly:     a.announcedOnly.Load(),
		Bounced:           a.bounced.Load(),
		PooledClean:       a.pooledClean.Load(),
		PeersetSampled:    a.peersetSampled.Load(),
		PeersetKnown:      a.peersetKnown.Load(),
		PeersetUnknown:    a.peersetUnknown.Load(),
		PeersetKnowingSum: a.peersetKnowingSum.Load(),
		PeersetTotalSum:   a.peersetTotalSum.Load(),
	}
}

// BlockClassStats returns the cumulative-since-startup classification
// of chain-included txs. Lock-free.
func (t *Tracker) BlockClassStats() BlockClassStats {
	return t.blockClass.snapshot()
}
