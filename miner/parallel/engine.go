package parallel

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

type Config struct {
	Workers  int
	Chain    core.ChainContext
	Params   *params.ChainConfig
	DB       state.Database
	Root     common.Hash // parent state root
	Header   *types.Header
	Coinbase common.Address

	// InOrder commits in position order, which builds the block a sequential
	// builder would. Otherwise results commit as they arrive.
	InOrder bool
	// InlineGas, with InOrder, makes the committer execute a transaction on the
	// block state itself when it uses at most this much gas. For cheap
	// transactions that costs less than validating and applying a result.
	InlineGas uint64
	// Reader, if set, reads the parent state instead of DB.Reader(Root), such
	// as a SharedReader the block state also reads through.
	Reader state.Reader
	// StoreValidation validates results against the store instead of the
	// block state: a read is fresh if the newest version below the task is
	// still the one it loaded. Needs InOrder; stale results are re-executed on
	// the block state with tracking, so that every committed write is in the
	// store.
	StoreValidation bool
	// ApplyLoads, with a SharedReader parent, reads the parent state that
	// applying a validated result loads before applying it, with up to this
	// many concurrent reads. Zero leaves the reads to Apply, one at a time.
	ApplyLoads int
	// Predict, if set, returns the expected state access of a transaction.
	// An execution reading a key waits for the lower positions predicted to
	// write it, instead of reading a value they are about to replace; for a
	// balance changed by deltas, only if it is predicted to read it exactly.
	Predict func(common.Hash) Prediction
	// Preexecuted, if set, returns the result of a Preexecutor for the same
	// parent state and header, or nil. With InOrder, such a result stands in
	// for the first execution of its transaction unless a lower transaction is
	// expected to write what it read.
	Preexecuted func(common.Hash) *Result
}

// Stats counts what happened during a build.
type Stats struct {
	Planned      int
	Chained      int // tasks that had to wait for an earlier one from the same sender
	LongestChain int
	Executions   int
	Stale        int // results whose reads no longer held at commit time
	Requeued     int // stale tasks handed back to the workers
	Committed    int
	Dropped      int
	Inlined      int // transactions the committer executed on the block state
	Reexecuted   int // of those, executed by a worker before (stale or failed results)
	Waits        int // reads that waited for a predicted writer
	Seeded       int // tasks started from a pre-executed result

	ExecutionTime time.Duration // time summed over all executions
	ValidateTime  time.Duration // time the committer spent checking reads against the block state
	CommitTime    time.Duration // time the committer spent applying results
	WaitTime      time.Duration // time the committer spent waiting for results
	InlineTime    time.Duration // time the committer spent executing transactions itself
	WaitedTime    time.Duration // time executions spent waiting for predicted writers
	PrefetchTime  time.Duration // time summed over the goroutines loading the state seeded results read
	ChainWaitTime time.Duration // the part of WaitTime spent with a chained task as the lowest one left

	// StaleBy counts stale results by the location of the read that failed
	// first: "<address>" for account fields, "<address>/storage".
	StaleBy map[string]int
}

func (s *Stats) recordStale(k key) {
	if s.StaleBy == nil {
		s.StaleBy = make(map[string]int)
	}
	loc := k.addr.Hex()
	if k.field == storage {
		loc += "/storage"
	} else if k.field == balance {
		loc += "/balance"
	} else if k.field == minBalance {
		loc += "/minbalance"
	} else {
		loc += fmt.Sprintf("/field%d", k.field)
	}
	s.StaleBy[loc]++
}

// Engine executes candidate transactions in parallel and commits them in
// position order.
type Engine struct {
	store     *store
	committed *store // writes of committed transactions, versioned by commit order
	exec      *executor
	workers   int
	inOrder   bool
	storeVal  bool
	inlineGas uint64
	applyLoad int
	predict   func(common.Hash) Prediction
	preexec   func(common.Hash) *Result
	context   common.Hash // execution context, which pre-executed results must share
	seeded    map[*Result]bool
	next      int
	dropped   map[common.Address]bool
	stats     Stats
}

// New returns an engine building on the parent state described by cfg.
func New(cfg Config) (*Engine, error) {
	parent := cfg.Reader
	if parent == nil {
		var err error
		if parent, err = cfg.DB.Reader(cfg.Root); err != nil {
			return nil, err
		}
	}
	if !cfg.InOrder {
		// only the in-order committer executes cheap transactions itself
		cfg.InlineGas = 0
	}
	s, committed := newStore(), newStore()
	return &Engine{
		store:     s,
		committed: committed,
		exec: &executor{
			chain:     cfg.Chain,
			config:    cfg.Params,
			db:        cfg.DB,
			root:      cfg.Root,
			header:    types.CopyHeader(cfg.Header),
			coinbase:  cfg.Coinbase,
			store:     s,
			committed: committed,
			parent:    parent,
			// a sequential build reuses one EVM and its code analysis
			jumpDests: core.NewJumpDestCache(),
		},
		workers:   max(1, cfg.Workers),
		inOrder:   cfg.InOrder,
		storeVal:  cfg.StoreValidation,
		inlineGas: cfg.InlineGas,
		applyLoad: cfg.ApplyLoads,
		predict:   cfg.Predict,
		preexec:   cfg.Preexecuted,
		context:   executionContext(cfg.Params, cfg.Root, cfg.Header, cfg.Coinbase),
		dropped:   make(map[common.Address]bool),
	}, nil
}

// NewTask registers a candidate at the next position.
func (e *Engine) NewTask(lazy *txpool.LazyTransaction, sender common.Address, blob bool) *Task {
	t := &Task{
		Position: e.next,
		Sender:   sender,
		Lazy:     lazy,
		Blob:     blob,
		done:     make(chan struct{}),
	}
	if e.predict != nil {
		p := e.predict(lazy.Hash)
		for _, w := range p.Writes {
			t.predicted = append(t.predicted, w.key())
		}
		t.deltas = p.Deltas
		if len(p.Observes) > 0 {
			t.observes = make(map[common.Address]bool, len(p.Observes))
			for _, a := range p.Observes {
				t.observes[a] = true
			}
		}
	}
	if e.preexec != nil && e.inOrder {
		if r := e.preexec(lazy.Hash); r != nil && r.context == e.context {
			t.pre = r
		}
	}
	e.next++
	e.stats.Planned++
	// resolving a blob transaction reconstructs its blobs, which takes
	// milliseconds, so start right away instead of on the worker
	if blob {
		go t.resolve()
	}
	return t
}

// Dropped reports whether the senders remaining candidates are excluded.
func (e *Engine) Dropped(sender common.Address) bool {
	return e.dropped[sender]
}

// Seed runs fn on the parent state and makes its writes visible to every
// task, so executions see the changes made before the first transaction
// parent writes are written at position -1 in the store
func (e *Engine) Seed(fn func(vm.StateDB) error) error {
	writes, codes, err := e.exec.seedWrites(fn)
	if err != nil {
		return err
	}
	e.store.publish(-1, writes, codes)
	e.committed.publish(-1, writes, codes)
	return nil
}

// seedWrites runs fn on the parent state and returns its writes, additive
// balance changes made absolute, as the committer publishes them for
// transactions.
func (e *executor) seedWrites(fn func(vm.StateDB) error) (map[key]value, map[common.Hash][]byte, error) {
	reader := newVersionedStateReader(0, e.store, e.parent)
	sdb, err := state.NewWithReader(e.root, e.db, reader)
	if err != nil {
		return nil, nil, err
	}
	tracked := newTrackingStateDB(sdb, reader, e.coinbase)
	if err := fn(tracked); err != nil {
		return nil, nil, err
	}
	res, err := tracked.result()
	if err != nil {
		return nil, nil, err
	}
	writes := res.writes
	if len(res.deltas) > 0 || res.fee != nil {
		writes = maps.Clone(res.writes)
		credited := slices.Collect(maps.Keys(res.deltas))
		if res.fee != nil {
			credited = append(credited, e.coinbase)
		}
		for _, addr := range credited {
			writes[key{addr: addr, field: exists}] = boolValue(true)
			writes[key{addr: addr, field: balance}] = balanceValue(sdb.GetBalance(addr))
		}
	}
	return writes, res.codes, nil
}

// Run executes tasks and commits them into block as their results arrive. It
// returns true once block reports that it is full.
func (e *Engine) Run(ctx context.Context, tasks []*Task, block Block) (bool, error) {
	chain := make(map[*Task]int, len(tasks))
	for _, t := range tasks {
		chain[t] = 1
		if t.prev != nil {
			chain[t] = chain[t.prev] + 1
			e.stats.Chained++
		}
		e.stats.LongestChain = max(e.stats.LongestChain, chain[t])
	}
	sched := newScheduler(e.store, e.workers, e.exec.run, e.inlineGas)
	if e.predict != nil {
		e.exec.waiter = newWriteWaiter(tasks, sched)
	}
	var seeds map[*Task]*Result
	if e.preexec != nil && e.inOrder {
		if e.seeded == nil {
			e.seeded = make(map[*Result]bool)
		}
		seeds = seedable(tasks, e.exec.coinbase, e.inlineGas, e.seeded)
		e.stats.Seeded += len(seeds)
	}
	sched.start(tasks, seeds)
	if shared, ok := e.exec.parent.(*SharedReader); ok && len(seeds) > 0 {
		stop := prefetchSeeds(tasks, seeds, shared, max(1, e.workers/2), e.storeVal, &e.stats)
		defer stop()
	}
	defer func() {
		executions, executionTime := sched.stop()
		e.stats.Executions += executions
		e.stats.ExecutionTime += executionTime
		e.stats.Waits += int(sched.waits.Load())
		e.stats.WaitedTime += time.Duration(sched.waitTime.Load())
		e.exec.waiter = nil
	}()

	c := &committer{
		store:     e.store,
		committed: e.committed,
		runOn:     e.exec.runOn,
		requeue:   sched.push,
		finished:  sched.finished,
		block:     block,
		coinbase:  e.exec.coinbase,
		dropped:   e.dropped,
		stats:     &e.stats,
		inlineGas: e.inlineGas,
		storeVal:  e.storeVal,
	}
	if shared, ok := e.exec.parent.(*SharedReader); ok && e.applyLoad > 0 {
		c.shared, c.applyLoad = shared, e.applyLoad
	}
	if e.inOrder {
		return c.runInOrder(ctx, tasks)
	}
	return c.run(ctx, tasks)
}

func (e *Engine) Stats() Stats {
	return e.stats
}
