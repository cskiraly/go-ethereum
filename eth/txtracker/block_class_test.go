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
	"testing"
	"time"
)

func TestClassifyBlockInclusion(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cases := []struct {
		name string
		ti   *TxInfo
		want BlockClassBucket
	}{
		{"nil tracker entry → private", nil, BlockClassPrivate},
		{"announce only", &TxInfo{Announcers: []string{"p1"}}, BlockClassAnnouncedOnly},
		{"request only", &TxInfo{Requested: now}, BlockClassAnnouncedOnly},
		{"receive only", &TxInfo{Received: now}, BlockClassAnnouncedOnly},
		{"pooled clean", &TxInfo{Pooled: now}, BlockClassPooledClean},
		{"pooled clean with hint", &TxInfo{Requested: now, Pooled: now}, BlockClassPooledClean},
		{"pooled then dropped → bounced", &TxInfo{Pooled: now, Dropped: now}, BlockClassBounced},
		{"pooled with reject → bounced", &TxInfo{Pooled: now, RejectErr: "underpriced"}, BlockClassBounced},
		{"pooled with returns → bounced", &TxInfo{Pooled: now, Returns: 1}, BlockClassBounced},
		{"reject-only (no pool) → announcedOnly via Announcers", &TxInfo{Announcers: []string{"p1"}, RejectErr: "x"}, BlockClassAnnouncedOnly},
		{"reject-only with no hint → private", &TxInfo{RejectErr: "x"}, BlockClassPrivate},
	}
	for _, c := range cases {
		if got := classifyBlockInclusion(c.ti); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestBlockClassAccCounts(t *testing.T) {
	a := newBlockClassAcc()
	a.record(BlockClassPrivate)
	a.record(BlockClassPrivate)
	a.record(BlockClassAnnouncedOnly)
	a.record(BlockClassBounced)
	a.record(BlockClassPooledClean)
	a.record(BlockClassPooledClean)
	a.record(BlockClassPooledClean)
	got := a.snapshot()
	want := BlockClassStats{Total: 7, Private: 2, AnnouncedOnly: 1, Bounced: 1, PooledClean: 3}
	if got != want {
		t.Fatalf("snapshot mismatch: got %+v want %+v", got, want)
	}
}

func TestBlockClassAccPeerCoverage(t *testing.T) {
	a := newBlockClassAcc()
	// No-peers samples are ignored (we can't distinguish "no peer knew"
	// from "no peers connected").
	a.recordPeerCoverage(0, 0)
	// 8 peers, 3 know the hash.
	a.recordPeerCoverage(8, 3)
	// 8 peers, 0 know.
	a.recordPeerCoverage(8, 0)
	// 4 peers, 4 know — fully covered.
	a.recordPeerCoverage(4, 4)
	got := a.snapshot()
	if got.PeersetSampled != 3 {
		t.Errorf("PeersetSampled: got %d want 3", got.PeersetSampled)
	}
	if got.PeersetKnown != 2 {
		t.Errorf("PeersetKnown: got %d want 2", got.PeersetKnown)
	}
	if got.PeersetUnknown != 1 {
		t.Errorf("PeersetUnknown: got %d want 1", got.PeersetUnknown)
	}
	if got.PeersetKnowingSum != 7 {
		t.Errorf("PeersetKnowingSum: got %d want 7", got.PeersetKnowingSum)
	}
	if got.PeersetTotalSum != 20 {
		t.Errorf("PeersetTotalSum: got %d want 20", got.PeersetTotalSum)
	}
}
