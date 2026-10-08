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
	"fmt"
	"strconv"
	"strings"
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
	case "inorder":
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
		"executionTimeNs": s.ExecutionTime.Nanoseconds(),
		"commitTimeNs":    s.CommitTime.Nanoseconds(),
		"waitTimeNs":      s.WaitTime.Nanoseconds(),
		"chained":         s.Chained,
		"longestChain":    s.LongestChain,
		"requeued":        s.Requeued,
		"validateTimeNs":  s.ValidateTime.Nanoseconds(),
		"chainWaitTimeNs": s.ChainWaitTime.Nanoseconds(),
		"inlined":         s.Inlined,
		"inlineTimeNs":    s.InlineTime.Nanoseconds(),
	}
}
