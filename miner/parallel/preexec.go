package parallel

import (
	"encoding/binary"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// Preexecutor executes transactions alone on a parent state, as a node could
// when they arrive. A build on the same parent and header can use such a
// result as the first execution of the transaction: it commits it if its reads
// still hold, so the transaction is executed only if it conflicts.
type Preexecutor struct {
	exec    *executor
	context common.Hash
}

// executionContext identifies what an execution depends on besides the
// state changes of other transactions: the parent state and the header fields
// the EVM reads. A pre-executed result is used only in a build with the same
// context, since validating its state reads cannot detect a different one.
func executionContext(root common.Hash, h *types.Header, coinbase common.Address) common.Hash {
	var buf []byte
	u64 := func(v uint64) { buf = binary.BigEndian.AppendUint64(buf, v) }
	bigInt := func(v *big.Int) {
		if v == nil {
			buf = append(buf, 0)
			return
		}
		b := v.Bytes()
		buf = append(buf, byte(len(b)+1))
		buf = append(buf, b...)
	}
	opt := func(v *uint64) {
		if v == nil {
			buf = append(buf, 0)
			return
		}
		buf = append(buf, 1)
		u64(*v)
	}
	buf = append(buf, root[:]...)
	buf = append(buf, h.ParentHash[:]...)
	buf = append(buf, coinbase[:]...)
	buf = append(buf, h.MixDigest[:]...)
	bigInt(h.Number)
	bigInt(h.Difficulty)
	bigInt(h.BaseFee)
	u64(h.Time)
	u64(h.GasLimit)
	opt(h.ExcessBlobGas)
	opt(h.SlotNumber)
	return crypto.Keccak256Hash(buf)
}

// NewPreexecutor returns a Preexecutor for blocks with the given header on the
// state root. reader, if set, reads that state instead of db.Reader(root).
func NewPreexecutor(chain core.ChainContext, config *params.ChainConfig, db state.Database, root common.Hash, header *types.Header, reader state.Reader) (*Preexecutor, error) {
	if reader == nil {
		var err error
		if reader, err = db.Reader(root); err != nil {
			return nil, err
		}
	}
	return &Preexecutor{context: executionContext(root, header, header.Coinbase), exec: &executor{
		chain:     chain,
		config:    config,
		db:        db,
		root:      root,
		header:    types.CopyHeader(header),
		coinbase:  header.Coinbase,
		store:     newStore(),
		parent:    reader,
		jumpDests: core.NewJumpDestCache(),
	}}, nil
}

// Run executes tx on the parent state. The result is valid only for the
// header and state the Preexecutor was made for. A transaction whose nonce
// lies ahead of its sender's is executed as if it did not, which predicts its
// access but gives no result a build can use.
func (p *Preexecutor) Run(tx *types.Transaction) *Result {
	run := func(skipNonce bool) *Result {
		t := &Task{Lazy: &txpool.LazyTransaction{Hash: tx.Hash(), Tx: tx}, done: make(chan struct{}), skipNonce: skipNonce}
		return p.exec.execute(t, newVersionedStateReader(noVersions, p.exec.store, p.exec.parent))
	}
	r := run(false)
	r.context = p.context
	if errors.Is(r.Err, core.ErrNonceTooHigh) {
		if ahead := run(true); ahead.Err == nil {
			ahead.predictOnly = true
			ahead.context = p.context
			return ahead
		}
	}
	return r
}

// Prediction returns the state access of r, for Config.Predict.
func (r *Result) Prediction(coinbase common.Address) Prediction {
	var p Prediction
	for k := range r.changes() {
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

// changes yields the keys r wrote with a value other than the one it read
// first: a write restoring the original value, such as a reentrancy lock
// released again, does not affect other transactions.
func (r *Result) changes() func(func(key) bool) {
	return func(yield func(key) bool) {
		for k, v := range r.writes {
			if before, ok := r.reads[k]; ok && before == v {
				continue
			}
			if !yield(k) {
				return
			}
		}
	}
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
// own pre-executed result, or its prediction.
func seedable(tasks []*Task, coinbase common.Address, inlineGas uint64) map[*Task]*Result {
	seeds := make(map[*Task]*Result)
	written := make(map[key]bool)
	credited := map[common.Address]bool{coinbase: true} // every transaction pays a fee
	conflicts := func(r *Result) bool {
		for k := range r.reads {
			switch {
			case written[k]:
				return true
			case k.field == balance && credited[k.addr]:
				return true
			case k.field == storage && written[key{addr: k.addr, field: destructed}]:
				return true
			}
		}
		for addr := range r.minBalance {
			if credited[addr] || written[key{addr: addr, field: balance}] {
				return true
			}
		}
		return false
	}
	for _, t := range tasks {
		r := t.pre
		if r != nil && r.Err == nil && !r.predictOnly && !t.cheap(inlineGas) && !conflicts(r) {
			seeds[t] = r.copyForBuild()
		}
		if r != nil && r.Err == nil {
			for k := range r.changes() {
				written[k] = true
			}
			for addr := range r.deltas {
				credited[addr] = true
			}
			continue
		}
		for _, k := range t.predicted {
			written[k] = true
			if k.field == exists {
				written[key{addr: k.addr, field: destructed}] = true
			}
		}
		for _, addr := range t.deltas {
			credited[addr] = true
		}
	}
	return seeds
}

// prefetchSeeds loads the parent state that the seeded results read and
// write into shared, lowest position first, with n goroutines. The committer
// validates and applies seeded results on the block state, which reads
// through shared: without this its loads would be cold, as no worker executed
// these transactions. The returned function stops the goroutines and waits for
// them.
func prefetchSeeds(tasks []*Task, seeds map[*Task]*Result, shared *SharedReader, n int, stats *Stats) func() {
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
				load(order[i].reads)
				load(order[i].writes)
			}
		}()
	}
	return func() {
		close(quit)
		wg.Wait()
		stats.PrefetchTime += time.Duration(busy.Load())
	}
}
