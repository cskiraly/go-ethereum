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

package fetcher

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// stubBouncingChecker is a fake BouncingChecker for unit tests. The
// bouncing set is keyed by hash; IsBouncing returns true for any hash
// in the set.
type stubBouncingChecker struct {
	bouncing map[common.Hash]bool
}

func (s *stubBouncingChecker) IsBouncing(h common.Hash, _ string) bool {
	return s.bouncing[h]
}

// drainNotify reads one txAnnounce from the fetcher's internal notify
// channel, or returns (nil, false) if no announce arrives within the
// timeout. The fetcher must not be Started — Notify pushes synchronously
// into the channel, and an unstarted fetcher leaves it unread, so the
// test reads it directly.
func drainNotify(f *TxFetcher) ([]common.Hash, bool) {
	select {
	case ann := <-f.notify:
		return ann.hashes, true
	case <-time.After(200 * time.Millisecond):
		return nil, false
	}
}

// TestAnnounceSkippedWhenBouncing verifies that hashes the BouncingChecker
// flags are filtered out of the announce-enqueue path. The remaining
// hashes are pushed onto f.notify; the suppressed ones are not.
func TestAnnounceSkippedWhenBouncing(t *testing.T) {
	f := newTestTxFetcher()
	bouncingHash := common.Hash{0xAA}
	f.SetBouncingChecker(&stubBouncingChecker{bouncing: map[common.Hash]bool{
		bouncingHash: true,
	}})

	// Three hashes: one bouncing, two not. Notify pushes through the
	// announce filter and forwards the survivors via f.notify.
	hashes := []common.Hash{bouncingHash, {0xBB}, {0xCC}}
	types := []byte{types.LegacyTxType, types.LegacyTxType, types.LegacyTxType}
	sizes := []uint32{100, 200, 300}

	before := txAnnounceBouncingMeter.Snapshot().Count()
	go func() {
		if _, err := f.Notify("peer1", types, sizes, hashes); err != nil {
			t.Errorf("Notify: %v", err)
		}
	}()

	got, ok := drainNotify(f)
	if !ok {
		t.Fatal("no announce reached f.notify; expected the two non-bouncing hashes")
	}
	if len(got) != 2 {
		t.Fatalf("got %d hashes, want 2 (bouncing one should be filtered)", len(got))
	}
	for _, h := range got {
		if h == bouncingHash {
			t.Errorf("bouncing hash %v leaked through filter", h)
		}
	}
	after := txAnnounceBouncingMeter.Snapshot().Count()
	if after-before != 1 {
		t.Errorf("txAnnounceBouncingMeter delta = %d, want 1", after-before)
	}
}

// TestAnnounceProceedsWhenNotBouncing verifies that with a checker that
// flags nothing, every hash makes it through the announce filter. Sanity
// check that the wiring doesn't accidentally suppress everything.
func TestAnnounceProceedsWhenNotBouncing(t *testing.T) {
	f := newTestTxFetcher()
	f.SetBouncingChecker(&stubBouncingChecker{bouncing: map[common.Hash]bool{}})

	hashes := []common.Hash{{0xBB}, {0xCC}}
	types := []byte{types.LegacyTxType, types.LegacyTxType}
	sizes := []uint32{200, 300}

	go func() {
		if _, err := f.Notify("peer1", types, sizes, hashes); err != nil {
			t.Errorf("Notify: %v", err)
		}
	}()

	got, ok := drainNotify(f)
	if !ok {
		t.Fatal("no announce reached f.notify")
	}
	if len(got) != 2 {
		t.Errorf("got %d hashes, want 2", len(got))
	}
}

// TestAnnounceProceedsWithoutChecker verifies that when SetBouncingChecker
// is never called (nil checker), Notify works as it did pre-feature.
// Sanity check for the optional-dependency contract.
func TestAnnounceProceedsWithoutChecker(t *testing.T) {
	f := newTestTxFetcher()
	// No SetBouncingChecker call.

	hashes := []common.Hash{{0xBB}}
	types := []byte{types.LegacyTxType}
	sizes := []uint32{200}

	go func() {
		if _, err := f.Notify("peer1", types, sizes, hashes); err != nil {
			t.Errorf("Notify: %v", err)
		}
	}()

	if got, ok := drainNotify(f); !ok || len(got) != 1 {
		t.Errorf("got %v ok=%v, want one hash through", got, ok)
	}
}
