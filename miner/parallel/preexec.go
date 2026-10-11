package parallel

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

// Preexecutor executes transactions alone on a parent state, as a node could
// when they arrive. A build on the same parent and header can use such a
// result as the first execution of the transaction: it commits it if its reads
// still hold, so the transaction is executed only if it conflicts.
type Preexecutor struct {
	exec    *executor
	context common.Hash
	seed    map[key]value // the writes of Seed, under every execution
	codes   map[common.Hash][]byte
}

// Seed runs fn on the parent state, as Engine.Seed does for a build, and
// executes every later transaction on top of its writes: the block's system
// calls, such as the one storing the beacon root, which transactions can read.
// It must be called before Run.
func (p *Preexecutor) Seed(fn func(vm.StateDB) error) error {
	writes, codes, err := p.exec.seedWrites(fn)
	if err != nil {
		return err
	}
	p.seed, p.codes = writes, codes
	p.exec.env.seed = writes
	p.exec.store.publish(-1, writes, codes)
	return nil
}

// executionContext identifies what an execution depends on besides the
// state changes of other transactions and the header values it reads: the
// parent state and the rules. A pre-executed result is used only in a build
// with the same context, since validating its state reads cannot detect a
// different one; the header values it read are checked one by one (adapt).
func executionContext(config *params.ChainConfig, root common.Hash, h *types.Header) common.Hash {
	var buf []byte
	bigInt := func(v *big.Int) {
		if v == nil {
			buf = append(buf, 0)
			return
		}
		b := v.Bytes()
		buf = append(buf, byte(len(b)+1))
		buf = append(buf, b...)
	}
	// the rules and chain id in force, as opcodes like CHAINID see them
	rules := config.Rules(h.Number, h.Difficulty == nil || h.Difficulty.Sign() == 0, h.Time)
	buf = fmt.Appendf(buf, "%+v", rules)
	bigInt(config.ChainID)
	buf = append(buf, root[:]...)
	buf = append(buf, h.ParentHash[:]...)
	return crypto.Keccak256Hash(buf)
}

// blobFee returns the blob base fee of blocks with header h, nil before Cancun.
func blobFee(config *params.ChainConfig, h *types.Header) *big.Int {
	if h.ExcessBlobGas == nil {
		return nil
	}
	return eip4844.CalcBlobFee(config, h)
}

// NewPreexecutor returns a Preexecutor for blocks with the given header on the
// state of parent. The header may lie more than one block ahead of parent, for
// a block the node will propose later. reader, if set, reads parent's state
// instead of db.Reader(parent.Root).
func NewPreexecutor(chain core.ChainContext, config *params.ChainConfig, db state.Database, parent, header *types.Header, reader state.Reader) (*Preexecutor, error) {
	root := parent.Root
	if reader == nil {
		var err error
		if reader, err = db.Reader(root); err != nil {
			return nil, err
		}
	}
	return &Preexecutor{context: executionContext(config, root, header), exec: &executor{
		chain:     chain,
		config:    config,
		db:        db,
		root:      root,
		header:    types.CopyHeader(header),
		coinbase:  header.Coinbase,
		store:     newStore(),
		parent:    reader,
		jumpDests: core.NewJumpDestCache(),
		env:       &execEnv{header: types.CopyHeader(header), parent: types.CopyHeader(parent), coinbase: header.Coinbase, blobFee: blobFee(config, header)},
	}}, nil
}

// Run executes tx on the parent state. The result is valid only for the
// header and state the Preexecutor was made for. A transaction whose nonce
// lies ahead of its sender's is executed as if it did not, which predicts its
// access but gives no result a build can use.
func (p *Preexecutor) Run(tx *types.Transaction) *Result {
	return p.RunAfter(tx, nil)
}

// RunAfter executes tx on the parent state as changed by the results in
// basis, which are those of the earlier transactions of the same sender, in
// nonce order. A build can then use the result if it uses those of basis too.
func (p *Preexecutor) RunAfter(tx *types.Transaction, basis []*Result) *Result {
	store := p.exec.store
	if len(basis) > 0 {
		store = newStore()
		if p.seed != nil {
			store.publish(-1, p.seed, p.codes)
		}
		for i, b := range basis {
			writes := b.writes
			if len(b.deltas) > 0 {
				// additive changes become the balances they lead to
				writes = maps.Clone(b.writes)
				for addr, d := range b.deltas {
					k := key{addr: addr, field: balance}
					cur := new(big.Int)
					if v, _, has := store.read(k, i); has {
						cur = v.balance().ToBig()
					} else if acct, err := p.exec.parent.Account(addr); err == nil && acct != nil {
						cur = acct.Balance.ToBig()
					}
					if cur.Add(cur, d).Sign() < 0 {
						continue
					}
					next, overflow := uint256.FromBig(cur)
					if overflow {
						continue
					}
					writes[k] = balanceValue(next)
					writes[key{addr: addr, field: exists}] = boolValue(true)
				}
			}
			store.publish(i, writes, b.codes)
		}
	}
	run := func(skipNonce bool) *Result {
		t := &Task{Lazy: &txpool.LazyTransaction{Hash: tx.Hash(), Tx: tx}, done: make(chan struct{}), skipNonce: skipNonce}
		return p.exec.execute(t, newVersionedStateReader(len(basis), store, p.exec.parent))
	}
	r := run(false)
	r.context = p.context
	if errors.Is(r.Err, core.ErrNonceTooHigh) {
		if ahead := run(true); ahead.Err == nil {
			ahead.predictOnly = true
			ahead.context = p.context
			ahead.changed = ahead.changes()
			return ahead
		}
	}
	if r.Err == nil && len(basis) > 0 {
		r.basis = basis
	}
	r.changed = r.changes()
	return r
}

// Rebase re-executes, lowest first, each of results (those of txs, in block
// order) that read what a lower result changed, on top of the lower results it
// depends on and their own bases. A build that seeds those can then seed it
// too, instead of executing it after them.
func (p *Preexecutor) Rebase(txs []*types.Transaction, results []*Result) {
	index := make(map[*Result]int, len(results))
	written := make(map[key]int)
	credited := make(map[common.Address]int)
	for i, r := range results {
		if r == nil {
			continue
		}
		index[r] = i // also for later results built on this one, if it changes
		deps := make(map[int]bool)
		depend := func(j int, found bool) {
			if found && !slices.Contains(r.basis, results[j]) {
				deps[j] = true
			}
		}
		for k := range r.reads {
			j, found := written[k]
			depend(j, found)
			if k.field == balance {
				j, found = credited[k.addr]
				depend(j, found)
			}
			if k.field == storage {
				j, found = written[key{addr: k.addr, field: destructed}]
				depend(j, found)
			}
		}
		for addr := range r.minBalance {
			j, found := credited[addr]
			depend(j, found)
			j, found = written[key{addr: addr, field: balance}]
			depend(j, found)
		}
		if len(deps) > 0 && (r.Usable() || r.predictOnly) {
			// the basis: the dependencies, their bases and r's own, in order
			addBasis := func(basis []*Result) {
				for _, b := range basis {
					if j, ok := index[b]; ok {
						deps[j] = true
					}
				}
			}
			addBasis(r.basis)
			for j := range maps.Clone(deps) {
				addBasis(results[j].basis)
			}
			order := slices.Sorted(maps.Keys(deps))
			basis := make([]*Result, len(order))
			for n, j := range order {
				basis[n] = results[j]
			}
			if rebased := p.RunAfter(txs[i], basis); rebased.Usable() {
				results[i], r = rebased, rebased
			}
		}
		if !r.Usable() {
			continue
		}
		index[r] = i
		for _, k := range r.changes() {
			written[k] = i
		}
		for addr := range r.deltas {
			credited[addr] = i
		}
	}
}

// Usable reports whether a build can start from r, and later
// transactions of the sender be pre-executed on top of it.
func (r *Result) Usable() bool {
	return r.Err == nil && !r.predictOnly
}

// Prediction returns the state access of r, for Config.Predict.
func (r *Result) Prediction(coinbase common.Address) Prediction {
	var p Prediction
	for _, k := range r.changes() {
		var f byte
		switch k.field {
		case exists:
			f = 'e'
		case balance:
			f = 'b'
		case nonce:
			f = 'n'
		case code:
			f = 'c'
		case storage:
			f = 's'
		default:
			continue
		}
		p.Writes = append(p.Writes, WriteKey{Addr: k.addr, Field: f, Slot: k.slot})
	}
	for addr := range r.deltas {
		p.Deltas = append(p.Deltas, addr)
	}
	for k := range r.reads {
		if k.field == balance && k.addr != coinbase {
			p.Observes = append(p.Observes, k.addr)
		}
	}
	return p
}

// changes returns the keys r wrote with a value other than the one it read
// first: a write restoring the original value, such as a reentrancy lock
// released again, does not affect other transactions. Pre-executed results
// compute them once.
func (r *Result) changes() []key {
	if r.changed != nil {
		return r.changed
	}
	changed := make([]key, 0, len(r.writes))
	for k, v := range r.writes {
		if before, ok := r.reads[k]; ok && before == v {
			continue
		}
		changed = append(changed, k)
	}
	return changed
}

// copyForBuild returns a copy of r that a build can complete and apply
// without changing r.
func (r *Result) copyForBuild() *Result {
	cp := *r
	if r.Receipt != nil {
		receipt := *r.Receipt
		cp.Receipt = &receipt
	}
	if r.AccessList != nil {
		cp.AccessList = r.AccessList.Copy()
	}
	return &cp
}

// seedable returns, for each task, the pre-executed result to start from: one
// that executed without error and that no lower task is expected to
// invalidate, by writing what it read. A lower task's writes are those of its
// own pre-executed result, or its prediction. seeded collects the
// pre-executed results seeded, across the batches of a build.
func seedable(tasks []*Task, coinbase common.Address, inlineGas uint64, seeded map[*Result]bool) map[*Task]*Result {
	seeds := make(map[*Task]*Result)
	// the last lower writer of each key and balance changed additively, by
	// its pre-executed result, or unknown (nil) for a prediction or the fees
	written := make(map[key]*Result)
	credited := map[common.Address]*Result{coinbase: nil} // every transaction pays a fee
	conflicts := func(r *Result) bool {
		// a result executed on top of seeded results may read their writes
		ok := func(w *Result) bool { return w != nil && slices.Contains(r.basis, w) }
		for _, b := range r.basis {
			if !seeded[b] {
				return true
			}
		}
		for k := range r.reads {
			if w, found := written[k]; found && !ok(w) {
				return true
			}
			if k.field == balance {
				if w, found := credited[k.addr]; found && !ok(w) {
					return true
				}
			}
			if k.field == storage {
				if w, found := written[key{addr: k.addr, field: destructed}]; found && !ok(w) {
					return true
				}
			}
		}
		for addr := range r.minBalance {
			if w, found := credited[addr]; found && !ok(w) {
				return true
			}
			if w, found := written[key{addr: addr, field: balance}]; found && !ok(w) {
				return true
			}
		}
		return false
	}
	for _, t := range tasks {
		r := t.pre
		if r != nil && r.Err == nil && !r.predictOnly && !t.cheap(inlineGas) && !conflicts(r) {
			seeds[t] = r.copyForBuild()
			seeded[r] = true
		}
		if r != nil && r.Err == nil {
			for _, k := range r.changes() {
				written[k] = r
			}
			for addr := range r.deltas {
				credited[addr] = r
			}
			continue
		}
		for _, k := range t.predicted {
			written[k] = nil
			if k.field == exists {
				written[key{addr: k.addr, field: destructed}] = nil
			}
		}
		for _, addr := range t.deltas {
			credited[addr] = nil
		}
	}
	return seeds
}

// prefetchSeeds loads the parent state that the seeded results read and
// write into shared, lowest position first, with n goroutines. The committer
// validates and applies seeded results on the block state, which reads
// through shared: without this its loads would be cold, as no worker executed
// these transactions. With storeVal, reads are validated against the store,
// so only what applying a result loads is fetched: the keys it writes, the
// accounts its balance changes and requirements touch, and the reads that
// validation checks on the block state. The returned function stops the
// goroutines and waits for them.
func prefetchSeeds(tasks []*Task, seeds map[*Task]*Result, shared *SharedReader, n int, storeVal bool, stats *Stats) func() {
	var order []*Result
	for _, t := range tasks {
		if r := seeds[t]; r != nil {
			order = append(order, r)
		}
	}
	var (
		next atomic.Int64
		busy atomic.Int64
		quit = make(chan struct{})
		wg   sync.WaitGroup
	)
	load := func(m map[key]value) {
		for k := range m {
			switch k.field {
			case storage:
				shared.Storage(k.addr, k.slot)
			case exists, balance, nonce, code:
				shared.Account(k.addr)
			}
		}
	}
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			defer func() { busy.Add(int64(time.Since(started))) }()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(order) {
					return
				}
				select {
				case <-quit:
					return
				default:
				}
				r := order[i]
				if storeVal {
					load(r.unversionedReads())
					for addr := range r.deltas {
						shared.Account(addr)
					}
					for addr := range r.minBalance {
						shared.Account(addr)
					}
				} else {
					load(r.reads)
				}
				load(r.writes)
			}
		}()
	}
	return func() {
		close(quit)
		wg.Wait()
		stats.PrefetchTime += time.Duration(busy.Load())
	}
}

// unversionedReads returns the reads of r that store validation checks on the
// block state, as it has no load record for them (see committer.storeStale).
func (r *Result) unversionedReads() map[key]value {
	var reads map[key]value
	for k, v := range r.reads {
		if _, ok := r.loads[k]; ok {
			continue
		}
		if _, ok := r.loads[key{addr: k.addr, field: exists}]; ok && k.field != storage {
			continue
		}
		if reads == nil {
			reads = make(map[key]value)
		}
		reads[k] = v
	}
	return reads
}

// prefetchExecutions loads, for the tasks that execute in the build although
// pre-executed (results not seeded: a lower task is expected to change what
// they read, or they ran ahead of their nonce), the parent state their
// pre-executed results read, contract code included. A worker reads one key
// after another; here each task's keys are read concurrently, up to n at a
// time, two tasks at a time in position order, so that the worker finds them
// cached or joins the reads. The returned function stops it and waits.
func prefetchExecutions(tasks []*Task, seeds map[*Task]*Result, shared *SharedReader, n int, stats *Stats) func() {
	var order []*Result
	for _, t := range tasks {
		if r := t.pre; r != nil && r.Err == nil && seeds[t] == nil {
			order = append(order, r)
		}
	}
	var (
		next atomic.Int64
		busy atomic.Int64
		quit = make(chan struct{})
		wg   sync.WaitGroup
	)
	for range min(2, len(order)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			defer func() { busy.Add(int64(time.Since(started))) }()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(order) {
					return
				}
				select {
				case <-quit:
					return
				default:
				}
				var (
					accounts []common.Address
					slots    []slotKey
					seen     = make(map[common.Address]bool)
				)
				for k, v := range order[i].reads {
					if !seen[k.addr] {
						seen[k.addr] = true
						accounts = append(accounts, k.addr)
					}
					switch k.field {
					case storage:
						slots = append(slots, slotKey{k.addr, k.slot})
					case code:
						if h := v.hash(); h != types.EmptyCodeHash && h != (common.Hash{}) {
							shared.Code(k.addr, h)
						}
					}
				}
				shared.Load(accounts, slots, n)
			}
		}()
	}
	return func() {
		close(quit)
		wg.Wait()
		stats.PrefetchExecTime += time.Duration(busy.Load())
	}
}

// prefetchWritePaths schedules on the trie prefetcher of sdb, the block
// state, the trie paths of the keys that the pre-executed results of tasks
// write, in position order until their gas used exceeds gasLimit: seeded
// results are applied as they are, the others predict what their executions
// write. The account paths, which need no reads, go first in one batch; the
// storage paths need the parent storage root of their account, which n
// goroutines load through sdb's reader. The returned function stops them and
// waits.
func prefetchWritePaths(sdb *state.StateDB, tasks []*Task, seeds map[*Task]*Result, coinbase common.Address, gasLimit uint64, n int, stats *Stats) func() {
	var (
		accounts = []common.Address{coinbase}
		owners   []common.Address
		slots    = make(map[common.Address][]common.Hash)
		seen     = map[key]bool{{addr: coinbase, field: exists}: true}
		gas      uint64
	)
	add := func(k key) {
		if k.field != storage {
			k = key{addr: k.addr, field: exists}
		}
		if seen[k] {
			return
		}
		seen[k] = true
		if k.field != storage {
			accounts = append(accounts, k.addr)
			return
		}
		if slots[k.addr] == nil {
			owners = append(owners, k.addr)
		}
		slots[k.addr] = append(slots[k.addr], k.slot)
	}
	for _, t := range tasks {
		r := seeds[t]
		if r == nil {
			r = t.pre
		}
		if r == nil || r.Err != nil {
			continue
		}
		for k := range r.writes {
			add(k)
		}
		for addr := range r.deltas {
			add(key{addr: addr, field: exists})
		}
		if r.Receipt != nil {
			if gas += r.Receipt.GasUsed; gas >= gasLimit {
				break
			}
		}
	}
	// a storage write also writes the account's storage root
	for _, addr := range owners {
		add(key{addr: addr, field: exists})
	}
	sdb.PrefetchAccountWrites(accounts)
	stats.PrefetchedWrites += len(seen)

	var (
		next atomic.Int64
		busy atomic.Int64
		quit = make(chan struct{})
		wg   sync.WaitGroup
	)
	for range min(n, len(owners)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			defer func() { busy.Add(int64(time.Since(started))) }()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(owners) {
					return
				}
				select {
				case <-quit:
					return
				default:
				}
				sdb.PrefetchStorageWrites(owners[i], slots[owners[i]])
			}
		}()
	}
	return func() {
		close(quit)
		wg.Wait()
		stats.PrefetchWriteTime += time.Duration(busy.Load())
	}
}
