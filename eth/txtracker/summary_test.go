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
	"fmt"
	"math"
	"testing"
	"time"
)

func TestPeerIntern(t *testing.T) {
	p := newPeerIntern()

	// Empty string → 0.
	if idx := p.intern(""); idx != 0 {
		t.Fatalf("empty string: got %d, want 0", idx)
	}

	// First peer gets index 1.
	if idx := p.intern("peer-a"); idx != 1 {
		t.Fatalf("first peer: got %d, want 1", idx)
	}

	// Same peer → same index.
	if idx := p.intern("peer-a"); idx != 1 {
		t.Fatalf("same peer: got %d, want 1", idx)
	}

	// Different peer → different index.
	if idx := p.intern("peer-b"); idx != 2 {
		t.Fatalf("second peer: got %d, want 2", idx)
	}

	// Round-trip via resolve.
	if s := p.resolve(1); s != "peer-a" {
		t.Fatalf("resolve(1): got %q, want peer-a", s)
	}
	if s := p.resolve(2); s != "peer-b" {
		t.Fatalf("resolve(2): got %q, want peer-b", s)
	}
	if s := p.resolve(0); s != "" {
		t.Fatalf("resolve(0): got %q, want empty", s)
	}

	// Out of range → empty.
	if s := p.resolve(999); s != "" {
		t.Fatalf("resolve(999): got %q, want empty", s)
	}
}

func TestPeerInternOverflow(t *testing.T) {
	p := newPeerIntern()

	// Fill to max.
	for i := 1; i < math.MaxUint16; i++ {
		p.intern(fmt.Sprintf("peer-%d", i))
	}
	// Now at 65534 entries (indices 1..65534). Next should overflow.
	if idx := p.intern("overflow-peer"); idx != 0 {
		t.Fatalf("overflow: got %d, want 0", idx)
	}
}

func TestFlagsEncoding(t *testing.T) {
	tests := []struct {
		local  bool
		txType uint8
		status TxStatus
	}{
		{false, 0, TxAnnounced},
		{true, 2, TxPooled},
		{false, 4, TxFinalized},
		{true, 7, TxDropped},
		{false, 3, TxRejected},
	}
	for _, tt := range tests {
		f := packFlags(tt.local, tt.txType, tt.status)
		s := txSummary{flags: f}
		if s.isLocal() != tt.local {
			t.Errorf("local: got %v, want %v", s.isLocal(), tt.local)
		}
		if s.txType() != tt.txType {
			t.Errorf("txType: got %d, want %d", s.txType(), tt.txType)
		}
		if s.status() != tt.status {
			t.Errorf("status: got %d, want %d", s.status(), tt.status)
		}
	}
}

func TestClampHelpers(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Normal case.
	if got := clampMs(base, base.Add(500*time.Millisecond)); got != 500 {
		t.Fatalf("clampMs normal: got %d, want 500", got)
	}

	// Zero time → 0.
	if got := clampMs(base, time.Time{}); got != 0 {
		t.Fatalf("clampMs zero: got %d, want 0", got)
	}
	if got := clampMs(time.Time{}, base); got != 0 {
		t.Fatalf("clampMs zero base: got %d, want 0", got)
	}

	// Overflow → clamped to max.
	if got := clampMs(base, base.Add(100*time.Second)); got != math.MaxUint16 {
		t.Fatalf("clampMs overflow: got %d, want %d", got, math.MaxUint16)
	}

	// Negative delta → 0.
	if got := clampMs(base, base.Add(-1*time.Second)); got != 0 {
		t.Fatalf("clampMs negative: got %d, want 0", got)
	}

	// clampSec16 normal.
	if got := clampSec16(base, base.Add(3600*time.Second)); got != 3600 {
		t.Fatalf("clampSec16 normal: got %d, want 3600", got)
	}

	// clampSec16 overflow.
	if got := clampSec16(base, base.Add(100000*time.Second)); got != math.MaxUint16 {
		t.Fatalf("clampSec16 overflow: got %d, want %d", got, math.MaxUint16)
	}

	// clampSec32 normal.
	if got := clampSec32(base, base.Add(100000*time.Second)); got != 100000 {
		t.Fatalf("clampSec32 normal: got %d, want 100000", got)
	}
}

func TestCompactRecord(t *testing.T) {
	peers := newPeerIntern()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	rec := &txRecord{
		status:    TxIncluded,
		local:     true,
		txType:    2,
		txSize:    256,
		firstSeen: now,
		requested: now.Add(10 * time.Millisecond),
		received:  now.Add(50 * time.Millisecond),
		pooled:    now.Add(100 * time.Millisecond),
		included:  now.Add(30 * time.Second),
		finalized: now.Add(30*time.Second + 900*time.Second), // 15 min after include
		announcers: []string{"peer-a", "peer-b", "peer-c"},
		deliverer:  "peer-a",
		blockNum:   12345678,
		returns:    1,
		reorgCount: 2,
	}

	s := compactRecord(rec, peers)

	// Flags.
	if !s.isLocal() {
		t.Error("expected local=true")
	}
	if s.txType() != 2 {
		t.Errorf("txType: got %d, want 2", s.txType())
	}
	if s.status() != TxIncluded {
		t.Errorf("status: got %d, want TxIncluded", s.status())
	}

	// Timing deltas.
	if s.dRequestMs != 10 {
		t.Errorf("dRequestMs: got %d, want 10", s.dRequestMs)
	}
	if s.dReceiveMs != 50 {
		t.Errorf("dReceiveMs: got %d, want 50", s.dReceiveMs)
	}
	if s.dPoolMs != 100 {
		t.Errorf("dPoolMs: got %d, want 100", s.dPoolMs)
	}
	if s.dIncludeSec != 30 {
		t.Errorf("dIncludeSec: got %d, want 30", s.dIncludeSec)
	}
	if s.dFinalizeSec != 900 {
		t.Errorf("dFinalizeSec: got %d, want 900", s.dFinalizeSec)
	}

	// Peers.
	if s.nAnnouncers != 3 {
		t.Errorf("nAnnouncers: got %d, want 3", s.nAnnouncers)
	}
	if peers.resolve(s.firstAnnouncer) != "peer-a" {
		t.Errorf("firstAnnouncer: got %q", peers.resolve(s.firstAnnouncer))
	}
	if peers.resolve(s.deliverer) != "peer-a" {
		t.Errorf("deliverer: got %q", peers.resolve(s.deliverer))
	}

	// Other fields.
	if s.blockNum != 12345678 {
		t.Errorf("blockNum: got %d", s.blockNum)
	}
	if s.txSize != 4 { // 256/64 = 4
		t.Errorf("txSize: got %d, want 4", s.txSize)
	}
	if s.returns != 1 {
		t.Errorf("returns: got %d, want 1", s.returns)
	}
	if s.reorgCount != 2 {
		t.Errorf("reorgCount: got %d, want 2", s.reorgCount)
	}
}

func TestCompactRecordClamping(t *testing.T) {
	peers := newPeerIntern()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	rec := &txRecord{
		status:    TxPooled,
		firstSeen: now,
		requested: now.Add(100 * time.Second), // > 65s, clamped to 65535ms
		txSize:    5000000,                     // > 4MB, clamped
	}
	s := compactRecord(rec, peers)

	if s.dRequestMs != math.MaxUint16 {
		t.Errorf("clamped dRequestMs: got %d, want %d", s.dRequestMs, math.MaxUint16)
	}
	if s.txSize != math.MaxUint16 {
		t.Errorf("clamped txSize: got %d, want %d", s.txSize, math.MaxUint16)
	}
}

func TestPromoteRecord(t *testing.T) {
	peers := newPeerIntern()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	s := txSummary{
		flags:          packFlags(true, 3, TxFinalized),
		returns:        2,
		reorgCount:     1,
		nAnnouncers:    5,
		txSize:         10,
		firstAnnouncer: peers.intern("peer-x"),
		deliverer:      peers.intern("peer-y"),
		dRequestMs:     15,
		dReceiveMs:     45,
		dPoolMs:        80,
		dIncludeSec:    60,
		dFinalizeSec:   900,
		dDropSec:       0,
		firstSeen:      now.UnixNano(),
		blockNum:       99999,
	}

	rec := promoteRecord(s, peers)

	if rec.status != TxFinalized {
		t.Errorf("status: got %d", rec.status)
	}
	if !rec.local {
		t.Error("expected local=true")
	}
	if rec.txType != 3 {
		t.Errorf("txType: got %d", rec.txType)
	}
	if rec.txSize != 640 { // 10 * 64
		t.Errorf("txSize: got %d, want 640", rec.txSize)
	}
	if rec.returns != 2 {
		t.Errorf("returns: got %d", rec.returns)
	}
	if rec.reorgCount != 1 {
		t.Errorf("reorgCount: got %d", rec.reorgCount)
	}
	if !rec.firstSeen.Equal(now) {
		t.Errorf("firstSeen: got %v, want %v", rec.firstSeen, now)
	}
	if rec.blockNum != 99999 {
		t.Errorf("blockNum: got %d", rec.blockNum)
	}

	// Timing: check within 1ms tolerance (ms deltas).
	checkTime := func(name string, got, want time.Time) {
		t.Helper()
		if got.Sub(want).Abs() > time.Millisecond {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}
	checkTime("requested", rec.requested, now.Add(15*time.Millisecond))
	checkTime("received", rec.received, now.Add(45*time.Millisecond))
	checkTime("pooled", rec.pooled, now.Add(80*time.Millisecond))

	// Second-resolution deltas: check within 1s tolerance.
	checkSec := func(name string, got, want time.Time) {
		t.Helper()
		if got.Sub(want).Abs() > time.Second {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}
	checkSec("included", rec.included, now.Add(60*time.Second))
	checkSec("finalized", rec.finalized, now.Add(60*time.Second+900*time.Second))

	// Peers (partial restoration).
	if len(rec.announcers) != 1 || rec.announcers[0] != "peer-x" {
		t.Errorf("announcers: got %v", rec.announcers)
	}
	if rec.deliverer != "peer-y" {
		t.Errorf("deliverer: got %q", rec.deliverer)
	}
}

func TestCompactPromoteRoundTrip(t *testing.T) {
	peers := newPeerIntern()
	now := time.Date(2026, 6, 15, 10, 30, 0, 0, time.UTC)

	orig := &txRecord{
		status:     TxPooled,
		local:      false,
		txType:     2,
		txSize:     512,
		firstSeen:  now,
		requested:  now.Add(5 * time.Millisecond),
		received:   now.Add(20 * time.Millisecond),
		pooled:     now.Add(25 * time.Millisecond),
		announcers: []string{"peer-1", "peer-2"},
		deliverer:  "peer-1",
		blockNum:   0,
		returns:    3,
		reorgCount: 0,
	}

	s := compactRecord(orig, peers)
	restored := promoteRecord(s, peers)

	// Key fields must survive round-trip.
	if restored.status != orig.status {
		t.Errorf("status: %d vs %d", restored.status, orig.status)
	}
	if restored.local != orig.local {
		t.Errorf("local: %v vs %v", restored.local, orig.local)
	}
	if restored.txType != orig.txType {
		t.Errorf("txType: %d vs %d", restored.txType, orig.txType)
	}
	if !restored.firstSeen.Equal(orig.firstSeen) {
		t.Errorf("firstSeen mismatch")
	}
	if restored.blockNum != orig.blockNum {
		t.Errorf("blockNum: %d vs %d", restored.blockNum, orig.blockNum)
	}
	if restored.returns != orig.returns {
		t.Errorf("returns: %d vs %d", restored.returns, orig.returns)
	}

	// Timing: ms-resolution fields should be exact, sec-resolution within 1s.
	if restored.requested.Sub(orig.requested).Abs() > time.Millisecond {
		t.Errorf("requested mismatch: %v vs %v", restored.requested, orig.requested)
	}
	if restored.received.Sub(orig.received).Abs() > time.Millisecond {
		t.Errorf("received mismatch: %v vs %v", restored.received, orig.received)
	}
	if restored.pooled.Sub(orig.pooled).Abs() > time.Millisecond {
		t.Errorf("pooled mismatch: %v vs %v", restored.pooled, orig.pooled)
	}

	// First announcer preserved (rest lost in compaction).
	if len(restored.announcers) != 1 || restored.announcers[0] != "peer-1" {
		t.Errorf("announcers: got %v, want [peer-1]", restored.announcers)
	}
	if restored.deliverer != orig.deliverer {
		t.Errorf("deliverer: %q vs %q", restored.deliverer, orig.deliverer)
	}

	// txSize: 512 → 8 units → 512 (exact round trip since 512 is multiple of 64).
	if restored.txSize != orig.txSize {
		t.Errorf("txSize: %d vs %d", restored.txSize, orig.txSize)
	}
}

func TestSummaryToTxInfo(t *testing.T) {
	peers := newPeerIntern()
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	s := txSummary{
		flags:          packFlags(false, 2, TxIncluded),
		txSize:         5,
		firstAnnouncer: peers.intern("p1"),
		deliverer:      peers.intern("p2"),
		dRequestMs:     10,
		dReceiveMs:     20,
		dPoolMs:        30,
		dIncludeSec:    120,
		dFinalizeSec:   900,
		firstSeen:      now.UnixNano(),
		blockNum:       5000000,
	}

	info := summaryToTxInfo(s, peers)

	if info.Status != TxIncluded {
		t.Errorf("Status: %d", info.Status)
	}
	if info.Local {
		t.Error("expected local=false")
	}
	if info.TxType != 2 {
		t.Errorf("TxType: %d", info.TxType)
	}
	if info.TxSize != 320 { // 5*64
		t.Errorf("TxSize: %d", info.TxSize)
	}
	if info.BlockNum != 5000000 {
		t.Errorf("BlockNum: %d", info.BlockNum)
	}
	if !info.FirstSeen.Equal(now) {
		t.Errorf("FirstSeen: %v", info.FirstSeen)
	}
	if len(info.Announcers) != 1 || info.Announcers[0] != "p1" {
		t.Errorf("Announcers: %v", info.Announcers)
	}
	if info.Deliverer != "p2" {
		t.Errorf("Deliverer: %q", info.Deliverer)
	}

	// Finalized should be included + 900s.
	expectedFinalized := now.Add(120*time.Second + 900*time.Second)
	if info.Finalized.Sub(expectedFinalized).Abs() > time.Second {
		t.Errorf("Finalized: got %v, want ~%v", info.Finalized, expectedFinalized)
	}
}
