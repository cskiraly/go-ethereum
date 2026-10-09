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
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/txpool/txorder"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/miner/parallel"
	"github.com/ethereum/go-ethereum/params"
)

// parallelPlanningGasFactor sizes a batch: candidates are planned until their
// gas limits add up to this many times the gas left. Gas limits overstate the
// gas used, and a batch that leaves gas over is followed by more batches, each
// planned and executed only after the previous one committed. On mainnet blocks
// with four blocks of candidates, a factor of 2 took 7 batches per block and
// was slower than sequential; 4 fits the block in one. Planning more costs
// executions of candidates that end up not fitting, but the workers execute in
// position order, so those come last.
const parallelPlanningGasFactor = 4

// parallelBlock presents the miners environment to the engine as a Block.
type parallelBlock struct {
	miner     *Miner
	env       *environment
	interrupt *atomic.Int32
}

type parallelBuildMetrics struct {
	parallel.Stats
	batches  int
	planning time.Duration // time spent planning batches
	lastRun  time.Duration // wall time of the batches after the first
	wall     time.Duration
	log      []parallelBatchLog
}

// parallelBatchLog describes one batch, for diagnosis.
type parallelBatchLog struct {
	Planned   int    `json:"planned"`
	Committed int    `json:"committed"`
	GasLeft   uint64 `json:"gasLeft"` // before the batch
	WallUs    int64  `json:"wallUs"`
	ExecUs    int64  `json:"execUs"` // executions summed over workers
}

func (miner *Miner) parallelWorkerCount() int {
	workers := miner.config.ParallelWorkers
	if workers <= 0 {
		workers = 8
	}
	return max(1, min(workers, runtime.GOMAXPROCS(0)))
}

func parallelExecutionSupported(env *environment) (bool, string) {
	if env.witness != nil {
		return false, "stateless-witness"
	}
	if env.state.Database().Type().Is(state.TypeUBT) {
		return false, "binary-trie-access-events"
	}
	return true, ""
}

func (miner *Miner) commitTransactionsParallel(ctx context.Context, env *environment, plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce, interrupt *atomic.Int32) (err error) {
	ctx = context.WithoutCancel(ctx)

	started := time.Now()
	parent := miner.chain.GetHeader(env.header.ParentHash, env.header.Number.Uint64()-1)
	if parent == nil {
		return fmt.Errorf("missing parent header %x", env.header.ParentHash)
	}
	engine, err := parallel.New(parallel.Config{
		Workers:   miner.parallelWorkerCount(),
		Chain:     miner.chain,
		Params:    miner.chainConfig,
		DB:        env.state.Database(),
		Root:      parent.Root,
		Header:    env.header,
		Coinbase:  env.coinbase,
		InOrder:   miner.config.ParallelInOrder,
		InlineGas: miner.config.ParallelInlineGas,
		Predict:   miner.parallelPredict,
	})
	if err != nil {
		return err
	}
	// Run pre-execution system calls
	err = engine.Seed(func(sdb vm.StateDB) error {
		evm := vm.NewEVM(core.NewEVMBlockContext(env.header, miner.chain, &env.coinbase), sdb, miner.chainConfig, vm.Config{})
		defer evm.Release()
		core.PreExecution(ctx, env.header.ParentBeaconRoot, parent, miner.chainConfig, evm, env.header.Number, env.header.Time)
		return nil
	})
	if err != nil {
		return err
	}
	block := &parallelBlock{miner: miner, env: env, interrupt: interrupt}
	metrics := new(parallelBuildMetrics)
	defer func() {
		metrics.Stats = engine.Stats()
		metrics.wall = time.Since(started)
		miner.lastParallelMetrics.Store(metrics)
		if env.benchmark != nil {
			env.benchmark.addParallelMetrics(metrics)
		}
		if err != nil && !(errors.Is(err, errBlockInterruptedByNewHead) || errors.Is(err, errBlockInterruptedByRecommit) || errors.Is(err, errBlockInterruptedByTimeout)) {
			log.Warn("Parallel block building failed", "number", env.header.Number, "err", err)
		}
		log.Info("Parallel block building summary", "number", env.header.Number, "batches", metrics.batches,
			"planned", metrics.Planned, "chained", metrics.Chained, "longestChain", metrics.LongestChain,
			"executions", metrics.Executions, "stale", metrics.Stale, "requeued", metrics.Requeued,
			"committed", metrics.Committed, "dropped", metrics.Dropped, "inlined", metrics.Inlined, "wall", metrics.wall,
			"executionTime", metrics.ExecutionTime, "validateTime", metrics.ValidateTime, "commitTime", metrics.CommitTime,
			"waitTime", metrics.WaitTime, "chainWaitTime", metrics.ChainWaitTime)
	}()
	for {
		if interrupt != nil {
			if signal := interrupt.Load(); signal != commitInterruptNone {
				return signalToErr(signal)
			}
		}
		if env.gasPool.Gas() < params.TxGas {
			return nil
		}
		if !blobTxs.Empty() && env.blobs >= miner.maxBlobsPerBlock(env.header.Time) {
			blobTxs.Clear()
		}
		planStarted := time.Now()
		tasks := miner.planParallel(engine, env, plainTxs, blobTxs)
		metrics.planning += time.Since(planStarted)
		if len(tasks) == 0 {
			return nil
		}
		metrics.batches++
		runStarted, gasLeft, before := time.Now(), env.gasPool.Gas(), engine.Stats()
		full, err := engine.Run(ctx, tasks, block)
		if metrics.batches > 1 {
			metrics.lastRun += time.Since(runStarted)
		}
		after := engine.Stats()
		metrics.log = append(metrics.log, parallelBatchLog{
			Planned:   len(tasks),
			Committed: after.Committed - before.Committed,
			GasLeft:   gasLeft,
			WallUs:    time.Since(runStarted).Microseconds(),
			ExecUs:    (after.ExecutionTime - before.ExecutionTime).Microseconds(),
		})
		if err != nil || full {
			return err
		}
	}
}

// planParallel takes about one blocks worth of candidates off the queues in
// price order, popping senders the engine has dropped.
func (miner *Miner) planParallel(engine *parallel.Engine, env *environment, plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce) []*parallel.Task {
	var (
		budget      = env.gasPool.Gas() * parallelPlanningGasFactor
		blobGas     uint64
		planned     uint64
		plannedBlob uint64
		last        = make(map[common.Address]*parallel.Task)
		tasks       []*parallel.Task
	)
	if !blobTxs.Empty() {
		blobGas = uint64(miner.maxBlobsPerBlock(env.header.Time)-env.blobs) * params.BlobTxBlobGasPerBlob
	}
	for planned < budget {
		queue, lazy, sender := nextCandidate(plainTxs, blobTxs)
		if lazy == nil {
			break
		}
		blob := queue == blobTxs
		// the dropped for a sender check only gets fired if the engine has already
		// dropped a task from that sender in a previous batch in the commit phase
		// so we can skipthe senders transactions entirely
		if engine.Dropped(sender) || (blob && lazy.BlobGas > blobGas-plannedBlob) {
			queue.Pop()
			continue
		}
		if blob {
			plannedBlob += lazy.BlobGas
		}
		planned += lazy.Gas
		task := engine.NewTask(lazy, sender, blob)
		task.Follow(last[sender])
		last[sender] = task
		tasks = append(tasks, task)
		queue.Shift()
	}
	return tasks
}

// nextCandidate returns the better paying head of the two queues.
func nextCandidate(plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce) (*txorder.TransactionsByPriceAndNonce, *txpool.LazyTransaction, common.Address) {
	plain, plainFrom, plainTip := plainTxs.PeekWithSender()
	blob, blobFrom, blobTip := blobTxs.PeekWithSender()
	switch {
	case plain == nil:
		return blobTxs, blob, blobFrom
	case blob == nil:
		return plainTxs, plain, plainFrom
	case plainTip.Lt(blobTip):
		return blobTxs, blob, blobFrom
	}
	return plainTxs, plain, plainFrom
}

func (b *parallelBlock) State() *state.StateDB {
	return b.env.state
}

func (b *parallelBlock) Check(t *parallel.Task) (parallel.Verdict, error) {
	if b.interrupt != nil {
		if signal := b.interrupt.Load(); signal != commitInterruptNone {
			return parallel.Full, signalToErr(signal)
		}
	}
	env, tx := b.env, t.Tx()
	switch {
	case tx == nil:
		return parallel.Reject, nil
	case env.gasPool.Gas() < params.TxGas:
		return parallel.Full, nil
	case env.gasPool.Gas() < t.Lazy.Gas:
		return parallel.Reject, nil
	case tx.Protected() && !b.miner.chainConfig.IsEIP155(env.header.Number):
		return parallel.Reject, nil
	case !env.txFitsSize(tx):
		return parallel.Full, nil
	case t.Blob && !b.blobFits(tx):
		return parallel.Reject, nil
	}
	return parallel.Accept, nil
}

func (b *parallelBlock) blobFits(tx *types.Transaction) bool {
	sidecar := tx.BlobTxSidecar()
	return sidecar != nil && b.env.blobs+len(sidecar.Blobs) <= b.miner.maxBlobsPerBlock(b.env.header.Time)
}

// Execute runs the transaction of t on the block state through the sequential
// path and appends it to the block.
func (b *parallelBlock) Execute(t *parallel.Task) error {
	env, tx := b.env, t.Tx()
	env.state.SetTxContext(tx.Hash(), env.tcount, uint32(env.tcount+1))
	return b.miner.commitTransaction(context.Background(), env, tx)
}

// Include applies a validated result and appends the transaction to the block.
func (b *parallelBlock) Include(t *parallel.Task, r *parallel.Result) error {
	env, tx := b.env, t.Tx()
	if err := env.gasPool.ApplyTransactionDelta(r.Gas); err != nil {
		return err
	}
	env.state.SetTxContext(tx.Hash(), env.tcount, uint32(env.tcount+1))
	r.Apply(env.state, env.coinbase)

	receipt, blockHash := r.Receipt, env.header.Hash()
	receipt.CumulativeGasUsed = env.gasPool.CumulativeUsed()
	receipt.BlockHash = blockHash
	receipt.BlockNumber = env.header.Number
	receipt.TransactionIndex = uint(env.tcount)
	receipt.Logs = env.state.GetLogs(tx.Hash(), env.header.Number.Uint64(), blockHash, env.header.Time)
	receipt.Bloom = types.CreateBloom(receipt)
	env.header.GasUsed = env.gasPool.Used()

	if t.Blob {
		sidecar := tx.BlobTxSidecar()
		tx = tx.WithoutBlobTxSidecar()
		env.sidecars = append(env.sidecars, sidecar)
		env.blobs += len(sidecar.Blobs)
		*env.header.BlobGasUsed += receipt.BlobGasUsed
	}
	env.txs = append(env.txs, tx)
	env.receipts = append(env.receipts, receipt)
	env.size += tx.Size()
	env.bal.Merge(r.AccessList)
	env.tcount++
	return nil
}
