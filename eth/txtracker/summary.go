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

// txSummary is a compact 40-byte representation of a transaction's tracked
// lifecycle, used in the cold set for long-term retention after eviction from
// the hot set.
type txSummary struct {
	flags      uint8 // local(1 bit) + txType(3 bits) + status(4 bits)
	returns    uint8 // cold→hot promotion count
	reorgCount uint8 // included→pooled transitions
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
