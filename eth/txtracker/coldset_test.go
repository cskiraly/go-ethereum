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
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// makeTestHash creates a deterministic hash from an integer.
func makeTestHash(n int) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[:8], uint64(n))
	return h
}

// makeTestSummary creates a summary with a distinguishing marker.
func makeTestSummary(n int) txSummary {
	return txSummary{blockNum: uint64(n)}
}

func TestColdSetInsertGet(t *testing.T) {
	cs := newColdSet(100)

	h1 := makeTestHash(1)
	s1 := makeTestSummary(1)

	// Not found before insert.
	if _, ok := cs.get(h1); ok {
		t.Fatal("expected not found before insert")
	}

	// Insert and retrieve.
	if !cs.insert(h1, s1) {
		t.Fatal("insert returned false")
	}
	got, ok := cs.get(h1)
	if !ok {
		t.Fatal("expected found after insert")
	}
	if got.blockNum != 1 {
		t.Fatalf("wrong summary: got blockNum %d, want 1", got.blockNum)
	}
	if cs.Len() != 1 {
		t.Fatalf("wrong len: got %d, want 1", cs.Len())
	}

	// Insert several more.
	for i := 2; i <= 50; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}
	if cs.Len() != 50 {
		t.Fatalf("wrong len: got %d, want 50", cs.Len())
	}

	// Verify all retrievable.
	for i := 1; i <= 50; i++ {
		got, ok := cs.get(makeTestHash(i))
		if !ok {
			t.Fatalf("hash %d not found", i)
		}
		if got.blockNum != uint64(i) {
			t.Fatalf("hash %d: wrong blockNum %d", i, got.blockNum)
		}
	}
}

func TestColdSetDelete(t *testing.T) {
	cs := newColdSet(100)

	h1 := makeTestHash(1)
	cs.insert(h1, makeTestSummary(1))

	// Delete existing.
	s, ok := cs.delete(h1)
	if !ok {
		t.Fatal("delete returned false for existing entry")
	}
	if s.blockNum != 1 {
		t.Fatalf("wrong deleted summary: blockNum %d", s.blockNum)
	}
	if cs.Len() != 0 {
		t.Fatalf("wrong len after delete: %d", cs.Len())
	}

	// Not found after delete.
	if _, ok := cs.get(h1); ok {
		t.Fatal("found after delete")
	}

	// Delete non-existent.
	if _, ok := cs.delete(makeTestHash(999)); ok {
		t.Fatal("delete returned true for non-existent")
	}
}

func TestColdSetProbeChain(t *testing.T) {
	// Use a small table to force collisions.
	cs := newColdSet(4) // table will be 8 slots

	// Insert entries that are likely to collide (sequential hashes often
	// map to adjacent slots, but with only 8 slots collisions are frequent).
	hashes := make([]common.Hash, 4)
	for i := range hashes {
		hashes[i] = makeTestHash(i + 1)
		cs.insert(hashes[i], makeTestSummary(i+1))
	}

	// Delete a middle entry (creates tombstone in probe chain).
	cs.delete(hashes[1])

	// Remaining entries must still be found via probing past tombstone.
	for i, h := range hashes {
		if i == 1 {
			if _, ok := cs.get(h); ok {
				t.Fatal("deleted entry still found")
			}
			continue
		}
		got, ok := cs.get(h)
		if !ok {
			t.Fatalf("entry %d not found after sibling delete", i)
		}
		if got.blockNum != uint64(i+1) {
			t.Fatalf("entry %d: wrong blockNum %d", i, got.blockNum)
		}
	}
}

func TestColdSetInsertDuplicate(t *testing.T) {
	cs := newColdSet(100)

	h := makeTestHash(1)
	cs.insert(h, makeTestSummary(10))
	cs.insert(h, makeTestSummary(20)) // update

	if cs.Len() != 1 {
		t.Fatalf("duplicate insert changed len: got %d", cs.Len())
	}
	got, _ := cs.get(h)
	if got.blockNum != 20 {
		t.Fatalf("duplicate insert didn't update summary: blockNum %d", got.blockNum)
	}
}

func TestColdSetEvictFIFO(t *testing.T) {
	cs := newColdSet(5)

	// Insert 5 entries (at capacity).
	for i := 1; i <= 5; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}

	// Insert 6th → evicts oldest (hash 1).
	cs.insert(makeTestHash(6), makeTestSummary(6))
	if cs.Len() != 5 {
		t.Fatalf("wrong len after eviction: %d", cs.Len())
	}
	if _, ok := cs.get(makeTestHash(1)); ok {
		t.Fatal("oldest entry should have been evicted")
	}

	// Insert 7th → evicts hash 2.
	cs.insert(makeTestHash(7), makeTestSummary(7))
	if _, ok := cs.get(makeTestHash(2)); ok {
		t.Fatal("second oldest should have been evicted")
	}

	// Hashes 3-7 should all be present.
	for i := 3; i <= 7; i++ {
		if _, ok := cs.get(makeTestHash(i)); !ok {
			t.Fatalf("hash %d should be present", i)
		}
	}
}

func TestColdSetDeleteDoesNotBreakFIFO(t *testing.T) {
	cs := newColdSet(5)

	// Insert A, B, C.
	cs.insert(makeTestHash(1), makeTestSummary(1)) // A (oldest)
	cs.insert(makeTestHash(2), makeTestSummary(2)) // B
	cs.insert(makeTestHash(3), makeTestSummary(3)) // C (newest)

	// Delete B (simulates promotion).
	cs.delete(makeTestHash(2))

	// Evict → should remove A (oldest remaining), not C.
	hash, _, ok := cs.evictOldest()
	if !ok {
		t.Fatal("evictOldest failed")
	}
	if hash != makeTestHash(1) {
		t.Fatalf("expected hash 1 evicted, got %x", hash[:8])
	}

	// Evict again → should remove C.
	hash, _, ok = cs.evictOldest()
	if !ok {
		t.Fatal("second evictOldest failed")
	}
	if hash != makeTestHash(3) {
		t.Fatalf("expected hash 3 evicted, got %x", hash[:8])
	}

	// Set should be empty now.
	if cs.Len() != 0 {
		t.Fatalf("expected empty, got len %d", cs.Len())
	}
	if _, _, ok := cs.evictOldest(); ok {
		t.Fatal("evictOldest on empty set should return false")
	}
}

func TestColdSetReInsert(t *testing.T) {
	cs := newColdSet(5)

	// Insert A, B, C.
	cs.insert(makeTestHash(1), makeTestSummary(10)) // A
	cs.insert(makeTestHash(2), makeTestSummary(20)) // B
	cs.insert(makeTestHash(3), makeTestSummary(30)) // C

	// Delete A (promote), then re-insert A with new summary.
	cs.delete(makeTestHash(1))
	cs.insert(makeTestHash(1), makeTestSummary(11)) // A is now newest

	// Verify updated summary.
	got, ok := cs.get(makeTestHash(1))
	if !ok {
		t.Fatal("re-inserted entry not found")
	}
	if got.blockNum != 11 {
		t.Fatalf("wrong summary after re-insert: blockNum %d", got.blockNum)
	}

	// Evict order should be B, C, A (A is newest).
	hash, _, _ := cs.evictOldest()
	if hash != makeTestHash(2) {
		t.Fatalf("expected B evicted first, got %x", hash[:8])
	}
	hash, _, _ = cs.evictOldest()
	if hash != makeTestHash(3) {
		t.Fatalf("expected C evicted second, got %x", hash[:8])
	}
	hash, _, _ = cs.evictOldest()
	if hash != makeTestHash(1) {
		t.Fatalf("expected A evicted last, got %x", hash[:8])
	}
}

func TestColdSetTombstoneReclaim(t *testing.T) {
	cs := newColdSet(100)

	// Insert 50 entries, delete them all (creates 50 tombstones).
	for i := 1; i <= 50; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}
	for i := 1; i <= 50; i++ {
		cs.delete(makeTestHash(i))
	}
	// Note: rehash may have been triggered, clearing tombstones.
	// Either way, inserting new entries should work correctly.

	// Insert 50 new entries.
	for i := 51; i <= 100; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}
	if cs.Len() != 50 {
		t.Fatalf("wrong len: got %d, want 50", cs.Len())
	}

	// Verify all new entries found.
	for i := 51; i <= 100; i++ {
		if _, ok := cs.get(makeTestHash(i)); !ok {
			t.Fatalf("hash %d not found", i)
		}
	}
}

func TestColdSetRehash(t *testing.T) {
	cs := newColdSet(20)

	// Insert 20 entries.
	for i := 1; i <= 20; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}

	// Delete > 25% to trigger rehash. Table is power-of-2 >= 28,
	// so 25% is at least 7. Delete 10 to be safe.
	for i := 1; i <= 10; i++ {
		cs.delete(makeTestHash(i))
	}

	// Rehash triggers partway through deletes (when tombstones > 25%).
	// Remaining deletes after rehash may create a few new tombstones.
	if cs.tombstones > len(cs.slots)/4 {
		t.Fatalf("tombstones should be below rehash threshold, got %d/%d", cs.tombstones, len(cs.slots))
	}
	if cs.Len() != 10 {
		t.Fatalf("wrong len after rehash: got %d, want 10", cs.Len())
	}

	// Verify remaining entries and FIFO order.
	for i := 11; i <= 20; i++ {
		got, ok := cs.get(makeTestHash(i))
		if !ok {
			t.Fatalf("hash %d not found after rehash", i)
		}
		if got.blockNum != uint64(i) {
			t.Fatalf("hash %d: wrong blockNum %d after rehash", i, got.blockNum)
		}
	}

	// FIFO order: 11, 12, ..., 20.
	for i := 11; i <= 20; i++ {
		hash, _, ok := cs.evictOldest()
		if !ok {
			t.Fatalf("evictOldest failed at step %d", i)
		}
		if hash != makeTestHash(i) {
			t.Fatalf("wrong FIFO order: expected hash %d, got %x", i, hash[:8])
		}
	}
}

func TestColdSetEvictTombstoneRehash(t *testing.T) {
	cs := newColdSet(10)

	// Fill to capacity.
	for i := 1; i <= 10; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}

	// Sustained insert/evict churn — each insert evicts oldest and creates
	// a tombstone. After enough churn, rehash should be triggered.
	for i := 11; i <= 100; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}

	// After churn, tombstones should have been cleaned by rehash.
	if cs.tombstones > len(cs.slots)/4 {
		t.Fatalf("tombstone ratio too high after churn: %d/%d", cs.tombstones, len(cs.slots))
	}
	if cs.Len() != 10 {
		t.Fatalf("wrong len after churn: got %d", cs.Len())
	}

	// Latest 10 entries should be present.
	for i := 91; i <= 100; i++ {
		if _, ok := cs.get(makeTestHash(i)); !ok {
			t.Fatalf("hash %d not found after churn", i)
		}
	}
}

func TestColdSetZeroHash(t *testing.T) {
	cs := newColdSet(10)

	// Zero hash is rejected (reserved as empty sentinel).
	if cs.insert(common.Hash{}, makeTestSummary(1)) {
		t.Fatal("zero hash should be rejected")
	}
	if cs.Len() != 0 {
		t.Fatal("len should be 0 after rejected insert")
	}

	// Tombstone hash is also rejected.
	if cs.insert(tombstoneHash, makeTestSummary(1)) {
		t.Fatal("tombstone hash should be rejected")
	}
}

func TestColdSetChurn(t *testing.T) {
	cs := newColdSet(100)

	// Fill to capacity.
	for i := 1; i <= 100; i++ {
		cs.insert(makeTestHash(i), makeTestSummary(i))
	}

	// 10K cycles of: promote (delete) a random entry, insert a new one.
	nextNew := 101
	// Promote from oldest still present, insert new.
	for cycle := 0; cycle < 10000; cycle++ {
		// Evict oldest (simulates natural FIFO eviction during insert).
		// Instead, let's explicitly promote via delete + re-insert new.
		hash, _, ok := cs.evictOldest()
		if !ok {
			t.Fatalf("evictOldest failed at cycle %d", cycle)
		}
		_ = hash
		cs.insert(makeTestHash(nextNew), makeTestSummary(nextNew))
		nextNew++
	}

	// Verify integrity.
	if cs.Len() != 100 {
		t.Fatalf("wrong len after churn: got %d, want 100", cs.Len())
	}

	// All 100 current entries should be retrievable.
	found := 0
	for i := 1; i < nextNew; i++ {
		if _, ok := cs.get(makeTestHash(i)); ok {
			found++
		}
	}
	if found != 100 {
		t.Fatalf("found %d entries, expected 100", found)
	}
}
