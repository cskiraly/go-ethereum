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
	"errors"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
)

var (
	buildBenchMainnet = flag.String("buildbench.mainnet", "", "geth datadir to replay mainnet blocks from, instead of the synthetic workloads; it is opened read-write, so use a disposable copy")
	buildBenchBlocks  = flag.String("buildbench.blocks", "last:10", "mainnet blocks N to build: FROM-TO, N, or last:COUNT (the latest whose candidates and parent state exist)")
	buildBenchEvict   = flag.Bool("buildbench.evict", false, "mainnet: before every build, empty the trie database clean caches and drop the database files from the OS page cache (posix_fadvise, no root needed); with -buildbench.cold, state reads reach the disk")
	buildBenchCold    = flag.Bool("buildbench.cold", false, "mainnet: shrink geth's own caches (pebble block cache, pathdb clean caches) so state reads reach the OS page cache")
	buildBenchLog     = flag.Bool("buildbench.log", false, "print geth's info logs, such as opening the chain")
	buildBenchDepth   = flag.Int("buildbench.depth", 2, "mainnet candidates: the transactions of blocks N to N+depth-1, built on N-1")
)

// mainnetCases opens the chain in -buildbench.mainnet and returns one case per
// selected block. The chain is closed when the test ends.
func mainnetCases(t *testing.T) ([]benchCase, error) {
	if *buildBenchDepth < 1 {
		return nil, errors.New("-buildbench.depth must be at least 1")
	}
	if *buildBenchLog {
		log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, false)))
	}
	chain, engine, err := openBenchChain(*buildBenchMainnet)
	if err != nil {
		return nil, err
	}
	t.Cleanup(chain.Stop)

	head := chain.CurrentBlock().Number.Uint64()
	depth := uint64(*buildBenchDepth)
	if head+1 < depth {
		return nil, fmt.Errorf("chain head %d is below the candidate depth", head)
	}
	last := head + 1 - depth // the newest N whose candidates are all on the chain
	from, to, err := parseBlockRange(*buildBenchBlocks, last, chain)
	if err != nil {
		return nil, err
	}
	if to > last {
		return nil, fmt.Errorf("block %d needs candidates up to %d, past the head %d", to, to+depth-1, head)
	}
	var cases []benchCase
	for n := from; n <= to; n++ {
		cases = append(cases, benchCase{
			name: fmt.Sprintf("block-%d", n),
			open: func() (*benchEnv, error) { return newMainnetEnv(chain, engine, n, int(depth)) },
		})
	}
	return cases, nil
}

// parseBlockRange parses -buildbench.blocks. For last:COUNT it walks back from
// last over blocks whose parent state is available.
func parseBlockRange(spec string, last uint64, chain *core.BlockChain) (uint64, uint64, error) {
	if count, ok := strings.CutPrefix(spec, "last:"); ok {
		c, err := strconv.ParseUint(count, 10, 64)
		if err != nil || c == 0 {
			return 0, 0, fmt.Errorf("invalid -buildbench.blocks %q", spec)
		}
		from := last
		for from > 1 && last-from+1 < c {
			parent := chain.GetHeaderByNumber(from - 2)
			if parent == nil || !chain.HasState(parent.Root) {
				break
			}
			from--
		}
		return from, last, nil
	}
	a, b, isRange := strings.Cut(spec, "-")
	from, err := strconv.ParseUint(a, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid -buildbench.blocks %q", spec)
	}
	to := from
	if isRange {
		if to, err = strconv.ParseUint(b, 10, 64); err != nil || to < from {
			return 0, 0, fmt.Errorf("invalid -buildbench.blocks %q", spec)
		}
	}
	return from, to, nil
}

// openBenchChain opens the chain of a geth datadir with the path scheme.
func openBenchChain(datadir string) (*core.BlockChain, consensus.Engine, error) {
	dir := filepath.Join(datadir, "geth", "chaindata")
	// Like the node, open databases in the legacy format with pebble v1.
	var (
		kv  ethdb.KeyValueStore
		err error
	)
	cache := 2048
	if *buildBenchCold {
		cache = 16
	}
	if pebble.NeedsV1(dir) {
		kv, err = pebble.NewV1(dir, cache, 4096, "", false)
	} else {
		kv, err = pebble.New(dir, cache, 4096, "", false)
	}
	if err != nil {
		return nil, nil, err
	}
	db, err := rawdb.Open(kv, rawdb.OpenOptions{Ancient: filepath.Join(dir, "ancient")})
	if err != nil {
		kv.Close()
		return nil, nil, err
	}
	engine := beacon.New(ethash.NewFaker())
	cfg := core.DefaultConfig().WithStateScheme(rawdb.PathScheme)
	// The in-memory state layers of the latest blocks are journaled here at
	// shutdown. Without them the chain rewinds to the state on disk, ~128
	// blocks back, and truncates the blocks above it.
	cfg.TrieJournalDirectory = filepath.Join(datadir, "geth", "triedb")
	if *buildBenchCold {
		cfg.TrieCleanLimit, cfg.SnapshotLimit = 1, 1
	}
	chain, err := core.NewBlockChain(db, nil, engine, cfg)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return chain, engine, nil
}

// newMainnetEnv sets up building block number on its parent, with the
// transactions of blocks number to number+depth-1 as candidates. Blob
// transactions are left out: their sidecars are not kept with the chain.
func newMainnetEnv(chain *core.BlockChain, engine consensus.Engine, number uint64, depth int) (*benchEnv, error) {
	block := chain.GetBlockByNumber(number)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", number)
	}
	parent := chain.GetHeaderByHash(block.ParentHash())
	if parent == nil || !chain.HasState(parent.Root) {
		return nil, fmt.Errorf("state of block %d not available", number-1)
	}
	config := chain.Config()
	signer := types.LatestSigner(config)
	var txs []*types.Transaction
	origin := make(map[common.Hash]*types.Header)
	for i := range depth {
		b := chain.GetBlockByNumber(number + uint64(i))
		if b == nil {
			return nil, fmt.Errorf("block %d not found", number+uint64(i))
		}
		for _, tx := range b.Transactions() {
			if tx.Type() != types.BlobTxType {
				txs = append(txs, tx)
				origin[tx.Hash()] = b.Header()
			}
		}
	}
	header := block.Header()
	env := &benchEnv{
		name:        fmt.Sprintf("block-%d", number),
		description: fmt.Sprintf("mainnet %d (%d txs, %.1f Mgas), candidates from %d blocks", number, len(block.Transactions()), float64(block.GasUsed())/1e6, depth),
		depth:       float64(depth),
		gasCeil:     block.GasLimit(),
		config:      config,
		engine:      engine,
		chain:       chain,
		txs:         len(txs),
		candidates:  txs,
		origin:      origin,
		signer:      signer,
		verified:    make(map[common.Hash]error),
		check:       func(b *types.Block) error { return benchProcessBlock(chain, parent, b) },
	}
	env.params = func() *generateParams {
		withdrawals := block.Withdrawals()
		if withdrawals == nil {
			withdrawals = types.Withdrawals{}
		}
		return &generateParams{
			timestamp:   header.Time,
			forceTime:   true,
			parentHash:  parent.Hash(),
			coinbase:    header.Coinbase,
			random:      header.MixDigest,
			withdrawals: withdrawals,
			beaconRoot:  header.ParentBeaconRoot,
			slotNum:     header.SlotNumber,
		}
	}
	pool, closePool, err := newBenchPool(chain, signer, txs)
	if err != nil {
		return nil, err
	}
	env.pool = pool
	env.closers = append(env.closers, closePool)
	return env, nil
}

// evictPageCache drops the files under dir from the OS page cache. Only clean
// pages go, and only for this machine; the files are not changed.
func evictPageCache(dir string) (int64, error) {
	var evicted int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil // gone meanwhile, e.g. compacted away
		}
		defer f.Close()
		if info, err := f.Stat(); err == nil {
			evicted += info.Size()
		}
		return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	})
	return evicted, err
}
