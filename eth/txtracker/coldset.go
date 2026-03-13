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

	"github.com/ethereum/go-ethereum/common"
)

// tombstoneHash is a sentinel value for deleted slots. Probe chains treat it
// as occupied (continue probing) but insert can reuse it.
var tombstoneHash = common.Hash{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

// coldSlot is a single entry in the cold set hash table.
type coldSlot struct {
	hash    common.Hash // zero = empty, tombstoneHash = deleted
	summary txSummary
	prev    int32 // DLL prev slot index (-1 = none)
	next    int32 // DLL next slot index (-1 = none)
}

// coldSet is an open-addressing hash table with tombstone deletion and an
// intrusive doubly-linked list for FIFO eviction. It stores compact transaction
// summaries keyed by transaction hash.
//
// The table uses linear probing with the first 8 bytes of the hash as the
// probe index (transaction hashes are cryptographic, no rehash needed).
// Deletions write a tombstone sentinel to preserve probe chains. The DLL
// threads through table slots via prev/next int32 indices, providing O(1)
// FIFO eviction without a separate ring buffer.
type coldSet struct {
	slots      []coldSlot
	mask       uint64 // len(slots) - 1, for fast modulo
	len        int    // live entries
	maxLen     int    // capacity limit
	tombstones int    // tombstone count (triggers rehash when > 25% of table)
	fifoHead   int32  // oldest entry (-1 = empty list)
	fifoTail   int32  // newest entry (-1 = empty list)
}

// newColdSet creates a cold set with the given maximum number of live entries.
// The underlying table is sized at maxLen/0.75 rounded up to a power of 2.
func newColdSet(maxLen int) *coldSet {
	if maxLen <= 0 {
		maxLen = 1
	}
	// Table size: at least maxLen/0.75, rounded to power of 2.
	tableSize := 1
	for tableSize < maxLen*4/3+1 {
		tableSize <<= 1
	}
	return &coldSet{
		slots:    make([]coldSlot, tableSize),
		mask:     uint64(tableSize - 1),
		maxLen:   maxLen,
		fifoHead: -1,
		fifoTail: -1,
	}
}

// Len returns the number of live entries.
func (c *coldSet) Len() int {
	return c.len
}

// probe returns the starting slot index for the given hash.
func (c *coldSet) probe(hash common.Hash) uint64 {
	return binary.BigEndian.Uint64(hash[:8]) & c.mask
}

// isEmptySlot returns true if the slot is unused (zero hash).
func isEmptySlot(h common.Hash) bool {
	return h == (common.Hash{})
}

// isTombstone returns true if the slot is a tombstone.
func isTombstone(h common.Hash) bool {
	return h == tombstoneHash
}

// isLive returns true if the slot contains a live entry.
func isLive(h common.Hash) bool {
	return !isEmptySlot(h) && !isTombstone(h)
}

// get returns the summary for the given hash, or false if not found.
func (c *coldSet) get(hash common.Hash) (txSummary, bool) {
	if isEmptySlot(hash) || isTombstone(hash) {
		return txSummary{}, false
	}
	idx := c.probe(hash)
	for {
		slot := &c.slots[idx]
		if isEmptySlot(slot.hash) {
			return txSummary{}, false
		}
		if slot.hash == hash {
			return slot.summary, true
		}
		idx = (idx + 1) & c.mask
	}
}

// insert adds or updates an entry. Returns false if hash is zero (reserved).
// If the table exceeds maxLen after insertion, the oldest entry is evicted.
func (c *coldSet) insert(hash common.Hash, s txSummary) bool {
	if isEmptySlot(hash) || isTombstone(hash) {
		return false
	}
	idx := c.probe(hash)
	tombIdx := int64(-1) // first tombstone seen

	for {
		slot := &c.slots[idx]

		if isEmptySlot(slot.hash) {
			// Not found. Insert at tombstone if we saw one, else here.
			if tombIdx >= 0 {
				c.writeSlot(int(tombIdx), hash, s)
				c.tombstones--
			} else {
				c.writeSlot(int(idx), hash, s)
			}
			c.len++
			// Evict oldest if over capacity.
			for c.len > c.maxLen {
				c.evictOldest()
			}
			return true
		}
		if slot.hash == hash {
			// Duplicate: update summary in place, don't change FIFO position.
			slot.summary = s
			return true
		}
		if isTombstone(slot.hash) && tombIdx < 0 {
			tombIdx = int64(idx)
		}
		idx = (idx + 1) & c.mask
	}
}

// writeSlot writes an entry to the given slot index and appends it to the
// DLL tail (newest position).
func (c *coldSet) writeSlot(idx int, hash common.Hash, s txSummary) {
	slot := &c.slots[idx]
	slot.hash = hash
	slot.summary = s
	slot.prev = c.fifoTail
	slot.next = -1

	if c.fifoTail >= 0 {
		c.slots[c.fifoTail].next = int32(idx)
	} else {
		c.fifoHead = int32(idx) // first entry
	}
	c.fifoTail = int32(idx)
}

// delete removes an entry by hash. Returns the removed summary and true,
// or zero summary and false if not found. The slot is tombstoned to preserve
// probe chains. Triggers rehash if tombstone ratio exceeds 25%.
func (c *coldSet) delete(hash common.Hash) (txSummary, bool) {
	if isEmptySlot(hash) || isTombstone(hash) {
		return txSummary{}, false
	}
	idx := c.probe(hash)
	for {
		slot := &c.slots[idx]
		if isEmptySlot(slot.hash) {
			return txSummary{}, false
		}
		if slot.hash == hash {
			s := slot.summary
			c.unlinkSlot(int(idx))
			slot.hash = tombstoneHash
			slot.summary = txSummary{}
			c.len--
			c.tombstones++
			c.maybeRehash()
			return s, true
		}
		idx = (idx + 1) & c.mask
	}
}

// evictOldest removes the oldest entry (DLL head). Returns the evicted hash,
// summary, and true; or zero values and false if the set is empty.
func (c *coldSet) evictOldest() (common.Hash, txSummary, bool) {
	if c.fifoHead < 0 {
		return common.Hash{}, txSummary{}, false
	}
	idx := int(c.fifoHead)
	slot := &c.slots[idx]
	hash := slot.hash
	s := slot.summary

	c.unlinkSlot(idx)
	slot.hash = tombstoneHash
	slot.summary = txSummary{}
	c.len--
	c.tombstones++
	c.maybeRehash()

	return hash, s, true
}

// unlinkSlot removes the slot at idx from the DLL.
func (c *coldSet) unlinkSlot(idx int) {
	slot := &c.slots[idx]
	prev := slot.prev
	next := slot.next

	if prev >= 0 {
		c.slots[prev].next = next
	} else {
		c.fifoHead = next
	}
	if next >= 0 {
		c.slots[next].prev = prev
	} else {
		c.fifoTail = prev
	}
	slot.prev = -1
	slot.next = -1
}

// maybeRehash triggers a rehash if tombstone count exceeds 25% of table size.
func (c *coldSet) maybeRehash() {
	if c.tombstones > len(c.slots)/4 {
		c.rehash()
	}
}

// rehash rebuilds the table, clearing all tombstones. It walks the old DLL
// from head to tail to preserve FIFO insertion order.
func (c *coldSet) rehash() {
	oldSlots := c.slots
	oldHead := c.fifoHead

	// Allocate a fresh table of the same size.
	c.slots = make([]coldSlot, len(oldSlots))
	c.fifoHead = -1
	c.fifoTail = -1
	c.tombstones = 0
	savedLen := c.len
	c.len = 0

	// Walk old DLL in FIFO order, re-inserting each live entry.
	idx := oldHead
	for idx >= 0 {
		old := &oldSlots[idx]
		next := old.next
		if isLive(old.hash) {
			c.insert(old.hash, old.summary)
		}
		idx = next
	}
	_ = savedLen // len is restored by inserts
}
