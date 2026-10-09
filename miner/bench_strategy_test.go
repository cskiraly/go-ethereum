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

// This file is the only part of the build benchmark that depends on the
// building engines of the branch: it maps strategy names to miner settings and
// collects engine-internal statistics. Port it when moving the harness.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/miner/parallel"
)

// applyBenchStrategy configures cfg for the named strategy:
//   - "sequential"
//   - "parallel-w<N>": the branch's engine, committing results as they arrive
//   - "inorder-w<N>": the engine committing in priority order
//   - "hybrid-w<N>[-g<gas>]": in order, the committer executing transactions
//     that use at most <gas> (default 50000) itself
func applyBenchStrategy(name string, cfg *Config) error {
	if name == "sequential" {
		cfg.ParallelExecution = false
		return nil
	}
	if base, ok := strings.CutSuffix(name, "-c"); ok {
		// -c: share parent reads between the engine and the block state
		cfg.ParallelSharedReads = true
		name = base
	}
	kind, rest, ok := strings.Cut(name, "-w")
	if !ok {
		return fmt.Errorf("unknown strategy %q", name)
	}
	workersStr, gasStr, hasGas := strings.Cut(rest, "-g")
	workers, err := strconv.Atoi(workersStr)
	if err != nil || workers < 1 {
		return fmt.Errorf("invalid strategy %q", name)
	}
	cfg.ParallelExecution = true
	cfg.ParallelWorkers = workers
	switch kind {
	case "parallel":
		if hasGas {
			return fmt.Errorf("invalid strategy %q", name)
		}
	case "inorderv", "predictv":
		// as below, validating results against the version store
		if hasGas {
			return fmt.Errorf("invalid strategy %q", name)
		}
		cfg.ParallelInOrder = true
		cfg.ParallelStoreValidation = true
	case "inorder", "predict", "preexec", "preexecp":
		// predict: in order, with the writes of every candidate predicted by
		// executing it alone on the parent state (prepareBenchMiner)
		if hasGas {
			return fmt.Errorf("invalid strategy %q", name)
		}
		cfg.ParallelInOrder = true
	case "hybrid":
		cfg.ParallelInOrder = true
		cfg.ParallelInlineGas = 50_000
		if hasGas {
			if cfg.ParallelInlineGas, err = strconv.ParseUint(gasStr, 10, 64); err != nil {
				return fmt.Errorf("invalid strategy %q", name)
			}
		}
	default:
		return fmt.Errorf("unknown strategy %q", name)
	}
	return nil
}

// resetBenchStats clears engine statistics before a build.
func resetBenchStats(m *Miner) {
	m.lastParallelMetrics.Store(nil)
}

// benchStats returns the engine statistics of the last build, if any.
func benchStats(m *Miner) map[string]any {
	s := m.lastParallelMetrics.Load()
	if s == nil {
		return nil
	}
	return map[string]any{
		"batches":         s.batches,
		"planned":         s.Planned,
		"executions":      s.Executions,
		"stale":           s.Stale,
		"committed":       s.Committed,
		"dropped":         s.Dropped,
		"commitPhaseNs":   s.wall.Nanoseconds(),
		"planningNs":      s.planning.Nanoseconds(),
		"laterBatchesNs":  s.lastRun.Nanoseconds(),
		"batchLog":        s.log,
		"executionTimeNs": s.ExecutionTime.Nanoseconds(),
		"commitTimeNs":    s.CommitTime.Nanoseconds(),
		"waitTimeNs":      s.WaitTime.Nanoseconds(),
		"chained":         s.Chained,
		"longestChain":    s.LongestChain,
		"requeued":        s.Requeued,
		"validateTimeNs":  s.ValidateTime.Nanoseconds(),
		"chainWaitTimeNs": s.ChainWaitTime.Nanoseconds(),
		"inlined":         s.Inlined,
		"waits":           s.Waits,
		"seeded":          s.Seeded,
		"prefetchNs":      s.PrefetchTime.Nanoseconds(),
		"waitedNs":        s.WaitedTime.Nanoseconds(),
		"reexecuted":      s.Reexecuted,
		"inlineTimeNs":    s.InlineTime.Nanoseconds(),
		"staleBy":         s.StaleBy,
	}
}

// benchProcessBlock executes block on the state of parent and validates the
// result, as a node importing it would, without writing anything.
func benchProcessBlock(chain *core.BlockChain, parent *types.Header, block *types.Block) error {
	if err := chain.Validator().ValidateBody(block); err != nil && !errors.Is(err, core.ErrKnownBlock) {
		return err
	}
	statedb, err := chain.StateAt(parent)
	if err != nil {
		return err
	}
	res, err := chain.Processor().Process(context.Background(), block, statedb, nil, vm.Config{})
	if err != nil {
		return err
	}
	return chain.Validator().ValidateState(block, statedb, res, false)
}

var buildBenchPredictDrop = flag.Float64("buildbench.predictdrop", 0, "predict strategies: drop this fraction of the predicted accesses, to test incomplete predictions")

// prepareBenchMiner installs what a strategy needs beyond the config.
func prepareBenchMiner(env *benchEnv, m *Miner, strategy string) error {
	if strings.HasPrefix(strategy, "preexec") {
		// preexec: every candidate executed alone on the parent state, once,
		// gives both the prediction and a result the build starts from
		if env.preexecuted == nil {
			started := time.Now()
			results, err := preexecuteCandidates(env)
			if err != nil {
				return err
			}
			env.preexecuted, env.preexecNs = results, time.Since(started).Nanoseconds()
		}
		coinbase := env.params().coinbase
		m.parallelPredict = func(hash common.Hash) parallel.Prediction {
			if r := env.preexecuted[hash]; r != nil && r.Err == nil {
				return r.Prediction(coinbase)
			}
			return parallel.Prediction{}
		}
		if !strings.HasPrefix(strategy, "preexecp") {
			// preexecp: the predictions alone, without starting from the results
			m.parallelPreexecuted = func(hash common.Hash) *parallel.Result { return env.preexecuted[hash] }
		}
		return nil
	}
	if !strings.HasPrefix(strategy, "predict") {
		return nil
	}
	if env.predicted == nil {
		started := time.Now()
		predicted, err := predictCandidateWrites(env)
		if err != nil {
			return err
		}
		env.predicted, env.predictNs = predicted, time.Since(started).Nanoseconds()
	}
	m.parallelPredict = func(hash common.Hash) parallel.Prediction { return env.predicted[hash] }
	if *buildBenchPredictDrop > 0 {
		// incomplete predictions: drop a seeded fraction of every list
		rng := rand.New(rand.NewSource(*buildBenchSeed))
		drop := func(n int) []int {
			var keep []int
			for i := range n {
				if rng.Float64() >= *buildBenchPredictDrop {
					keep = append(keep, i)
				}
			}
			return keep
		}
		thinned := make(map[common.Hash]parallel.Prediction, len(env.predicted))
		for _, tx := range env.candidates {
			p := env.predicted[tx.Hash()]
			var q parallel.Prediction
			for _, i := range drop(len(p.Writes)) {
				q.Writes = append(q.Writes, p.Writes[i])
			}
			for _, i := range drop(len(p.Deltas)) {
				q.Deltas = append(q.Deltas, p.Deltas[i])
			}
			for _, i := range drop(len(p.Observes)) {
				q.Observes = append(q.Observes, p.Observes[i])
			}
			thinned[tx.Hash()] = q
		}
		m.parallelPredict = func(hash common.Hash) parallel.Prediction { return thinned[hash] }
	}
	return nil
}
