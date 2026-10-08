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

package miner

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

// pairwiseResultConflict is the reference: it compares task with every result
// committed after its execution started, in commit order.
func pairwiseResultConflict(task *parallelTask, committedResults []parallelCommittedResult) *parallelConflict {
	for i := task.commitsSeen; i < len(committedResults); i++ {
		committed := committedResults[i]
		if committed.sender == task.sender {
			continue
		}
		if task.contextForChain != nil && committed.chain == task.contextForChain && committed.chainIndex < task.indexInChain {
			continue
		}
		if conflict := task.result.state.Conflict(committed.state); conflict != nil {
			return &parallelConflict{previousHash: committed.hash, previousPosition: committed.position, state: conflict}
		}
	}
	return nil
}

// randomAccesses draws access sets over a small universe, so that results
// overlap often.
func randomAccesses(rng *rand.Rand, addrs []common.Address, slots []common.Hash, reads, writes int) *state.ParallelStateResult {
	acc := state.ParallelStateAccesses{
		AccountReads:  make(map[common.Address]state.ParallelAccountFields),
		AccountWrites: make(map[common.Address]state.ParallelAccountFields),
		StorageReads:  make(map[state.ParallelStorageLocation]struct{}),
		StorageWrites: make(map[state.ParallelStorageLocation]struct{}),
	}
	location := func() state.ParallelStorageLocation {
		return state.ParallelStorageLocation{Address: addrs[rng.Intn(len(addrs))], Slot: slots[rng.Intn(len(slots))]}
	}
	// Existence is rare, as in practice; otherwise it would decide most checks.
	fields := func() state.ParallelAccountFields {
		f := state.ParallelAccountFields(rng.Intn(16)) &^ state.ParallelAccountExistence
		if rng.Intn(20) == 0 {
			f |= state.ParallelAccountExistence
		}
		return f
	}
	for range rng.Intn(reads + 1) {
		acc.AccountReads[addrs[rng.Intn(len(addrs))]] |= fields()
		acc.StorageReads[location()] = struct{}{}
	}
	for range rng.Intn(writes + 1) {
		acc.AccountWrites[addrs[rng.Intn(len(addrs))]] |= fields()
		acc.StorageWrites[location()] = struct{}{}
	}
	return &state.ParallelStateResult{Accesses: acc}
}

func TestParallelWriteIndexMatchesPairwise(t *testing.T) {
	for _, size := range []struct{ addrs, slots, reads, writes int }{
		{4, 2, 2, 1},    // dense: nearly everything conflicts
		{30, 8, 3, 2},   // mixed
		{200, 50, 4, 2}, // sparse: few conflicts, long scans
	} {
		t.Run(fmt.Sprintf("addrs=%d", size.addrs), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(size.addrs)))
			addrs := make([]common.Address, size.addrs)
			for i := range addrs {
				addrs[i] = common.BytesToAddress([]byte{byte(i), byte(i >> 8), 1})
			}
			slots := make([]common.Hash, size.slots)
			for i := range slots {
				slots[i] = common.BytesToHash([]byte{byte(i), 2})
			}
			senders := addrs[:min(len(addrs), 6)]
			chains := []*speculativeContextForChain{nil, {}, {}}

			batch := &parallelBatchCommit{}
			var checked, conflicts int
			for commit := range 400 {
				// Check a few tasks against the results committed so far.
				for range 5 {
					task := &parallelTask{
						sender:          senders[rng.Intn(len(senders))],
						contextForChain: chains[rng.Intn(len(chains))],
						indexInChain:    rng.Intn(4),
						commitsSeen:     rng.Intn(commit + 1),
						result:          &parallelExecutionResult{state: randomAccesses(rng, addrs, slots, size.reads, size.writes)},
					}
					want := pairwiseResultConflict(task, batch.committed)
					got := parallelResultConflict(task, batch)
					checked++
					switch {
					case want == nil && got == nil:
					case want == nil || got == nil:
						t.Fatalf("commit %d: pairwise %v, index %v", commit, want, got)
					case want.previousPosition != got.previousPosition:
						t.Fatalf("commit %d: pairwise conflicts with position %d, index with %d", commit, want.previousPosition, got.previousPosition)
					default:
						conflicts++
					}
				}
				entry := parallelCommittedResult{
					position:   commit,
					sender:     senders[rng.Intn(len(senders))],
					state:      randomAccesses(rng, addrs, slots, size.reads, size.writes),
					chain:      chains[rng.Intn(len(chains))],
					chainIndex: rng.Intn(4),
				}
				if rng.Intn(50) == 0 {
					entry.state = nil // a serial re-execution that recorded nothing
				}
				batch.commit(entry)
			}
			t.Logf("%d checks, %d conflicts", checked, conflicts)
			if conflicts == 0 || conflicts == checked {
				t.Fatalf("degenerate test: %d of %d checks conflict", conflicts, checked)
			}
		})
	}
}
