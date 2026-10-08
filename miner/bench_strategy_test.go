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

// applyBenchStrategy configures cfg for the named strategy: "sequential" or
// "parallel-w<workers>".
func applyBenchStrategy(name string, cfg *Config) error {
	switch {
	case name == "sequential":
		cfg.ParallelExecution = false
		return nil
	case strings.HasPrefix(name, "parallel-w"):
		workers, err := strconv.Atoi(strings.TrimPrefix(name, "parallel-w"))
		if err != nil || workers < 1 {
			return fmt.Errorf("invalid strategy %q", name)
		}
		cfg.ParallelExecution = true
		cfg.ParallelWorkers = workers
		return nil
	}
	return fmt.Errorf("unknown strategy %q", name)
}

// resetBenchStats clears engine statistics before a build.
func resetBenchStats(m *Miner) {
	m.lastParallelMetrics.Store(nil)
}

// benchStats returns the engine statistics of the last build, if any. The
// common keys (planned, executions, stale, committed, executionTimeNs) mean the
// same as on the improv branches; the rest are v1's own.
func benchStats(m *Miner) map[string]any {
	s := m.lastParallelMetrics.Load()
	if s == nil {
		return nil
	}
	return map[string]any{
		"batches":           s.batches,
		"planned":           s.planned,
		"executions":        s.speculative + s.retryExecutions + s.retries,
		"stale":             s.conflicts,
		"committed":         s.committed,
		"executionTimeNs":   s.executionWork.Nanoseconds(),
		"speculative":       s.speculative,
		"merged":            s.merged,
		"firstAttempt":      s.firstAttempt,
		"retriedCommitted":  s.retriedCommitted,
		"retryRounds":       s.retryRounds,
		"retryExecutions":   s.retryExecutions,
		"serialRetries":     s.retries,
		"invalid":           s.invalid,
		"incomplete":        s.incomplete,
		"executeWallNs":     s.executeWall.Nanoseconds(),
		"commitWallNs":      s.commitWall.Nanoseconds(),
		"serialRetryTimeNs": s.retryTime.Nanoseconds(),
		"planningTimeNs":    s.planningTime.Nanoseconds(),
		"storageConflicts":  s.storageConflicts,
		"balanceConflicts":  s.balanceConflicts,
		"nonceConflicts":    s.nonceConflicts,
		"codeConflicts":     s.codeConflicts,
		"existConflicts":    s.existConflicts,
	}
}
