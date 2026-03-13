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
	"math"
	"time"
)

// txSummary is a compact 40-byte representation of a transaction's tracked
// lifecycle, used in the cold set for long-term retention after eviction from
// the hot set.
//
// Flags byte layout: bit 0 = local, bits 1-3 = txType (0-7),
// bits 4-7 = terminal status (TxStatus value).
type txSummary struct {
	flags       uint8 // local(1 bit) + txType(3 bits) + status(4 bits)
	returns     uint8 // cold→hot promotion count
	reorgCount  uint8 // included→pooled transitions
	nAnnouncers uint8 // distinct announcing peers

	txSize         uint16 // size in 64-byte units (max ~4MB)
	firstAnnouncer uint16 // peer intern index
	deliverer      uint16 // peer intern index

	dRequestMs   uint16 // firstSeen → requested (ms, max 65s)
	dReceiveMs   uint16 // firstSeen → received (ms, max 65s)
	dPoolMs      uint16 // firstSeen → pooled (ms, max 65s)
	dFinalizeSec uint16 // included → finalized (sec, max 18h)
	dDropSec     uint16 // firstSeen → dropped (sec, max 18h)

	dIncludeSec uint32 // firstSeen → included (sec, max ~136y)

	firstSeen int64  // unix nanos
	blockNum  uint64 // block number (0 if never included)
}

// Flags accessors.

func (s txSummary) isLocal() bool    { return s.flags&1 != 0 }
func (s txSummary) txType() uint8    { return (s.flags >> 1) & 0x7 }
func (s txSummary) status() TxStatus { return TxStatus((s.flags >> 4) & 0xF) }

func packFlags(local bool, txType uint8, status TxStatus) uint8 {
	var f uint8
	if local {
		f |= 1
	}
	f |= (txType & 0x7) << 1
	f |= uint8(status&0xF) << 4
	return f
}

// Clamping helpers for time deltas.

func clampMs(base, t time.Time) uint16 {
	if t.IsZero() || base.IsZero() {
		return 0
	}
	d := t.Sub(base).Milliseconds()
	if d <= 0 {
		return 0
	}
	if d > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(d)
}

func clampSec16(base, t time.Time) uint16 {
	if t.IsZero() || base.IsZero() {
		return 0
	}
	d := int64(t.Sub(base).Seconds())
	if d <= 0 {
		return 0
	}
	if d > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(d)
}

func clampSec32(base, t time.Time) uint32 {
	if t.IsZero() || base.IsZero() {
		return 0
	}
	d := int64(t.Sub(base).Seconds())
	if d <= 0 {
		return 0
	}
	if d > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(d)
}

// peerIntern maps peer ID strings to compact uint16 indices. Index 0 is
// reserved for unknown/empty. The table grows monotonically and is never
// shrunk. If more than 65534 unique peers are seen, overflow falls back to 0.
type peerIntern struct {
	index   map[string]uint16
	strings []string // [0] = ""
}

func newPeerIntern() *peerIntern {
	return &peerIntern{
		index:   make(map[string]uint16),
		strings: []string{""},
	}
}

// intern returns the uint16 index for a peer string. Empty string returns 0.
// If the table is full (65535 entries), returns 0 (unknown).
func (p *peerIntern) intern(peer string) uint16 {
	if peer == "" {
		return 0
	}
	if idx, ok := p.index[peer]; ok {
		return idx
	}
	if len(p.strings) >= math.MaxUint16 {
		return 0 // overflow
	}
	idx := uint16(len(p.strings))
	p.strings = append(p.strings, peer)
	p.index[peer] = idx
	return idx
}

// resolve returns the peer string for a uint16 index. Invalid indices
// return empty string.
func (p *peerIntern) resolve(idx uint16) string {
	if int(idx) >= len(p.strings) {
		return ""
	}
	return p.strings[idx]
}

// compactRecord converts a hot txRecord into a compact txSummary.
func compactRecord(rec *txRecord, peers *peerIntern) txSummary {
	s := txSummary{
		flags:       packFlags(rec.local, rec.txType, rec.status),
		returns:     rec.returns,
		reorgCount:  rec.reorgCount,
		firstSeen:   rec.firstSeen.UnixNano(),
		blockNum:    rec.blockNum,
		dRequestMs:  clampMs(rec.firstSeen, rec.requested),
		dReceiveMs:  clampMs(rec.firstSeen, rec.received),
		dPoolMs:     clampMs(rec.firstSeen, rec.pooled),
		dIncludeSec: clampSec32(rec.firstSeen, rec.included),
		dFinalizeSec: clampSec16(rec.included, rec.finalized),
		dDropSec:    clampSec16(rec.firstSeen, rec.dropped),
	}
	// Peer interning.
	if len(rec.announcers) > 0 {
		s.firstAnnouncer = peers.intern(rec.announcers[0])
		n := len(rec.announcers)
		if n > math.MaxUint8 {
			n = math.MaxUint8
		}
		s.nAnnouncers = uint8(n)
	}
	s.deliverer = peers.intern(rec.deliverer)

	// Size in 64-byte units, rounded up.
	sz := (uint32(rec.txSize) + 63) / 64
	if sz > math.MaxUint16 {
		sz = math.MaxUint16
	}
	s.txSize = uint16(sz)

	return s
}

// promoteRecord converts a cold txSummary back into a hot txRecord,
// restoring timing from deltas and peer references from interned indices.
func promoteRecord(s txSummary, peers *peerIntern) *txRecord {
	fs := time.Unix(0, s.firstSeen)
	rec := &txRecord{
		status:     s.status(),
		local:      s.isLocal(),
		txType:     s.txType(),
		txSize:     uint32(s.txSize) * 64,
		returns:    s.returns,
		reorgCount: s.reorgCount,
		firstSeen:  fs,
		blockNum:   s.blockNum,
	}
	if s.dRequestMs > 0 {
		rec.requested = fs.Add(time.Duration(s.dRequestMs) * time.Millisecond)
	}
	if s.dReceiveMs > 0 {
		rec.received = fs.Add(time.Duration(s.dReceiveMs) * time.Millisecond)
	}
	if s.dPoolMs > 0 {
		rec.pooled = fs.Add(time.Duration(s.dPoolMs) * time.Millisecond)
	}
	if s.dIncludeSec > 0 {
		rec.included = fs.Add(time.Duration(s.dIncludeSec) * time.Second)
	}
	if s.dFinalizeSec > 0 && !rec.included.IsZero() {
		rec.finalized = rec.included.Add(time.Duration(s.dFinalizeSec) * time.Second)
	}
	if s.dDropSec > 0 {
		rec.dropped = fs.Add(time.Duration(s.dDropSec) * time.Second)
	}
	// Restore peer references (partial: first announcer + deliverer only).
	if s.firstAnnouncer != 0 {
		if p := peers.resolve(s.firstAnnouncer); p != "" {
			rec.announcers = []string{p}
		}
	}
	if s.deliverer != 0 {
		rec.deliverer = peers.resolve(s.deliverer)
	}
	return rec
}

// summaryToTxInfo builds a partial TxInfo from a cold summary without
// promoting it to the hot set. Fields not preserved in the summary
// (from, nonce, gas, fees, value, to, blockHash, rejectErr, dropReason,
// full announcer list) are left at zero values.
func summaryToTxInfo(s txSummary, peers *peerIntern) *TxInfo {
	fs := time.Unix(0, s.firstSeen)
	info := &TxInfo{
		Status:    s.status(),
		Local:     s.isLocal(),
		TxType:    s.txType(),
		TxSize:    uint32(s.txSize) * 64,
		FirstSeen: fs,
		BlockNum:  s.blockNum,
	}
	if s.dRequestMs > 0 {
		info.Requested = fs.Add(time.Duration(s.dRequestMs) * time.Millisecond)
	}
	if s.dReceiveMs > 0 {
		info.Received = fs.Add(time.Duration(s.dReceiveMs) * time.Millisecond)
	}
	if s.dPoolMs > 0 {
		info.Pooled = fs.Add(time.Duration(s.dPoolMs) * time.Millisecond)
	}
	if s.dIncludeSec > 0 {
		info.Included = fs.Add(time.Duration(s.dIncludeSec) * time.Second)
	}
	if s.dFinalizeSec > 0 && !info.Included.IsZero() {
		info.Finalized = info.Included.Add(time.Duration(s.dFinalizeSec) * time.Second)
	}
	if s.dDropSec > 0 {
		info.Dropped = fs.Add(time.Duration(s.dDropSec) * time.Second)
	}
	if s.firstAnnouncer != 0 {
		if p := peers.resolve(s.firstAnnouncer); p != "" {
			info.Announcers = []string{p}
		}
	}
	if s.deliverer != 0 {
		info.Deliverer = peers.resolve(s.deliverer)
	}
	return info
}
