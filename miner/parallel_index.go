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
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

// parallelWriteIndex records which committed results of a batch wrote each
// location, by their index in the batch's commit order. It lets a task be
// checked against the results committed after it executed by looking up its
// reads, instead of comparing it with each of those results.
type parallelWriteIndex struct {
	accounts map[common.Address][]parallelAccountWrite
	storage  map[state.ParallelStorageLocation][]int
}

type parallelAccountWrite struct {
	commit int // index of the writing result in the commit order
	fields state.ParallelAccountFields
}

// add indexes the writes of the result committed at index commit. Results must
// be added in commit order.
func (ix *parallelWriteIndex) add(commit int, result *state.ParallelStateResult) {
	if result == nil {
		return
	}
	if ix.accounts == nil {
		ix.accounts = make(map[common.Address][]parallelAccountWrite)
		ix.storage = make(map[state.ParallelStorageLocation][]int)
	}
	for addr, fields := range result.Accesses.AccountWrites {
		ix.accounts[addr] = append(ix.accounts[addr], parallelAccountWrite{commit: commit, fields: fields})
	}
	for location := range result.Accesses.StorageWrites {
		ix.storage[location] = append(ix.storage[location], commit)
	}
}

// commit appends a committed result to the batch and indexes its writes.
func (b *parallelBatchCommit) commit(entry parallelCommittedResult) {
	b.index.add(len(b.committed), entry.state)
	b.committed = append(b.committed, entry)
}

// parallelResultConflict returns the conflict of task with the earliest result
// committed after the task's execution started, if any.
func parallelResultConflict(task *parallelTask, batch *parallelBatchCommit) *parallelConflict {
	if task.result == nil || task.result.state == nil {
		return &parallelConflict{incomplete: true}
	}
	commit := batch.firstConflicting(task)
	if commit < 0 {
		return nil
	}
	committed := batch.committed[commit]
	conflict := task.result.state.Conflict(committed.state)
	if conflict == nil {
		panic("parallel write index disagrees with the conflict check")
	}
	return &parallelConflict{
		previousHash:     committed.hash,
		previousPosition: committed.position,
		state:            conflict,
	}
}

// firstConflicting returns the index of the earliest committed result that
// task conflicts with, or -1. It applies the rules of
// state.ParallelStateResult.Conflict to the indexed writes.
func (b *parallelBatchCommit) firstConflicting(task *parallelTask) int {
	if task.commitsSeen >= len(b.committed) {
		return -1 // nothing was committed after the task executed
	}
	var (
		accesses = task.result.state.Accesses
		first    = len(b.committed) // earliest conflicting commit found so far
	)
	// visible reports whether the writes of the result committed at index
	// commit were inputs of the task's execution rather than conflicts.
	visible := func(commit int) bool {
		committed := &b.committed[commit]
		if committed.sender == task.sender {
			return true
		}
		// Writes of an earlier link of the task's own chain were visible to
		// its execution.
		return task.contextForChain != nil && committed.chain == task.contextForChain && committed.chainIndex < task.indexInChain
	}
	// accountWrite moves first to the earliest relevant commit that wrote
	// addr with fields matching conflicts.
	accountWrite := func(addr common.Address, conflicts func(state.ParallelAccountFields) bool) {
		writes := b.index.accounts[addr]
		i := sort.Search(len(writes), func(i int) bool { return writes[i].commit >= task.commitsSeen })
		for ; i < len(writes) && writes[i].commit < first; i++ {
			if conflicts(writes[i].fields) && !visible(writes[i].commit) {
				first = writes[i].commit
				return
			}
		}
	}
	existence := func(writes state.ParallelAccountFields) bool {
		return writes&state.ParallelAccountExistence != 0
	}
	for addr, reads := range accesses.AccountReads {
		accountWrite(addr, func(writes state.ParallelAccountFields) bool {
			return existence(writes) || reads&writes != 0
		})
		if first == task.commitsSeen {
			return first
		}
	}
	for location := range accesses.StorageReads {
		writes := b.index.storage[location]
		i := sort.SearchInts(writes, task.commitsSeen)
		for ; i < len(writes) && writes[i] < first; i++ {
			if !visible(writes[i]) {
				first = writes[i]
				break
			}
		}
		accountWrite(location.Address, existence)
		if first == task.commitsSeen {
			return first
		}
	}
	for addr, writes := range accesses.AccountWrites {
		if writes != 0 {
			accountWrite(addr, existence)
		}
	}
	if first == len(b.committed) {
		return -1
	}
	return first
}
