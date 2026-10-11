// Copyright 2021 The go-ethereum Authors
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

package state

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/internal/testrand"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

func filledStateDB() *StateDB {
	state, _ := New(types.EmptyRootHash, NewDatabaseForTesting())

	// Create an account and check if the retrieved balance is correct
	addr := common.HexToAddress("0xaffeaffeaffeaffeaffeaffeaffeaffeaffeaffe")
	skey := common.HexToHash("aaa")
	sval := common.HexToHash("bbb")

	state.SetBalance(addr, uint256.NewInt(42), tracing.BalanceChangeUnspecified) // Change the account trie
	state.SetCode(addr, []byte("hello"), tracing.CodeChangeUnspecified)          // Change an external metadata
	state.SetState(addr, skey, sval)                                             // Change the storage trie
	for i := 0; i < 100; i++ {
		sk := common.BigToHash(big.NewInt(int64(i)))
		state.SetState(addr, sk, sk) // Change the storage trie
	}
	return state
}

func TestUseAfterTerminate(t *testing.T) {
	db := filledStateDB()
	prefetcher := newTriePrefetcher(db.db, db.originalRoot, "", true)
	skey := common.HexToHash("aaa")

	if err := prefetcher.prefetch(common.Hash{}, db.originalRoot, common.Address{}, nil, []common.Hash{skey}, false); err != nil {
		t.Errorf("Prefetch failed before terminate: %v", err)
	}
	prefetcher.terminate(false)

	if err := prefetcher.prefetch(common.Hash{}, db.originalRoot, common.Address{}, nil, []common.Hash{skey}, false); err == nil {
		t.Errorf("Prefetch succeeded after terminate: %v", err)
	}
	if tr := prefetcher.trie(common.Hash{}, db.originalRoot); tr == nil {
		t.Errorf("Prefetcher returned nil trie after terminate")
	}
}

func TestVerklePrefetcher(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	db := triedb.NewDatabase(disk, triedb.UBTDefaults)
	sdb := NewDatabase(db, nil)

	state, err := New(types.EmptyRootHash, sdb)
	if err != nil {
		t.Fatalf("failed to initialize state: %v", err)
	}
	// Create an account and check if the retrieved balance is correct
	addr := testrand.Address()
	skey := testrand.Hash()
	sval := testrand.Hash()

	state.SetBalance(addr, uint256.NewInt(42), tracing.BalanceChangeUnspecified) // Change the account trie
	state.SetCode(addr, []byte("hello"), tracing.CodeChangeUnspecified)          // Change an external metadata
	state.SetState(addr, skey, sval)                                             // Change the storage trie
	root, _ := state.Commit(0, true, false)

	state, _ = New(root, sdb)
	fetcher := newTriePrefetcher(sdb, root, "", false)

	// Read account
	fetcher.prefetch(common.Hash{}, root, common.Address{}, []common.Address{addr}, nil, false)

	// Read storage slot
	fetcher.prefetch(crypto.Keccak256Hash(addr.Bytes()), common.Hash{}, addr, nil, []common.Hash{skey}, false)

	fetcher.terminate(false)
	accountTrie := fetcher.trie(common.Hash{}, root)
	storageTrie := fetcher.trie(crypto.Keccak256Hash(addr.Bytes()), common.Hash{})

	rootA := accountTrie.Hash()
	rootB := storageTrie.Hash()
	if rootA != rootB {
		t.Fatal("Two different tries are retrieved")
	}
}

// TestPrefetchWrites schedules write paths ahead of the writes, concurrently
// with them, and checks that the prefetcher loaded them and that the state
// root is the one without prefetching.
func TestPrefetchWrites(t *testing.T) {
	db := NewDatabaseForTesting()
	init, _ := New(types.EmptyRootHash, db)
	var (
		addrs []common.Address
		slots []common.Hash
	)
	for i := 0; i < 64; i++ {
		addr := testrand.Address()
		addrs = append(addrs, addr)
		init.SetBalance(addr, uint256.NewInt(uint64(i+1)), tracing.BalanceChangeUnspecified)
	}
	for i := 0; i < 32; i++ {
		slot := testrand.Hash()
		slots = append(slots, slot)
		init.SetState(addrs[0], slot, testrand.Hash())
	}
	root, err := init.Commit(0, true, false)
	if err != nil {
		t.Fatal(err)
	}
	write := func(prefetch bool) (common.Hash, *triePrefetcher) {
		s, _ := New(root, db)
		s.StartPrefetcher("test", nil)
		done := make(chan struct{})
		if prefetch {
			go func() {
				defer close(done)
				s.PrefetchAccountWrites(addrs)
				s.PrefetchStorageWrites(addrs[0], slots)
				s.PrefetchStorageWrites(addrs[1], slots) // no storage: skipped
			}()
		} else {
			close(done)
		}
		for i, addr := range addrs {
			s.AddBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			if i < len(slots) {
				s.SetState(addrs[0], slots[i], common.Hash{byte(i)})
			}
			s.Finalise(true)
		}
		<-done
		p := s.prefetcher
		return s.IntermediateRoot(true), p
	}
	want, _ := write(false)
	got, p := write(true)
	if got != want {
		t.Fatalf("root %x, want %x", got, want)
	}
	acct := p.fetchers[p.trieID(common.Hash{}, root)]
	if n := len(acct.seenWriteAddr); n != len(addrs) {
		t.Errorf("account paths loaded: %d, want %d", n, len(addrs))
	}
	init2, _ := New(root, db)
	storageRoot := init2.GetStorageRoot(addrs[0])
	storage := p.fetchers[p.trieID(crypto.Keccak256Hash(addrs[0].Bytes()), storageRoot)]
	if storage == nil || len(storage.seenWriteSlot) != len(slots) {
		t.Fatalf("storage paths not loaded")
	}
	if len(p.fetchers) != 2 {
		t.Errorf("%d fetchers, want 2", len(p.fetchers))
	}
}
