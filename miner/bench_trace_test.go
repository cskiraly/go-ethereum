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

// Access traces of built blocks, for simulating conflict handling policies
// offline. A block is re-executed transaction by transaction on its parent
// state, and every state access is recorded with its time offset in the
// transaction.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc/eip1559"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/miner/parallel"
	"github.com/holiman/uint256"
)

var buildBenchTrace = flag.String("buildbench.trace", "", "JSONL file receiving an access trace of each case's reference block")

// Access kinds in a trace.
const (
	accessRead  = "r" // the value was read
	accessWrite = "w" // the value was written
	accessDelta = "d" // the balance was changed without being read
	accessMin   = "m" // a transfer required a minimum balance
)

// traceEvent is the first access of one kind to one key in a transaction.
type traceEvent struct {
	Kind string `json:"k"`
	Key  string `json:"key"` // address, "/" and b(alance), n(once), c(ode), e(xistence) or the storage slot
	At   int64  `json:"t"`   // ns since the transaction started
}

type traceTx struct {
	Index    int          `json:"i"`
	Hash     common.Hash  `json:"hash"`
	From     string       `json:"from"`
	Nonce    uint64       `json:"nonce"`
	Tip      string       `json:"tip"` // effective tip per gas, in wei
	GasUsed  uint64       `json:"gas"`
	Duration int64        `json:"dur"`    // ns, fastest uninstrumented pass
	Recorded int64        `json:"recdur"` // ns, the recorded pass; event times are on this scale
	Failed   bool         `json:"failed,omitempty"`
	Events   []traceEvent `json:"ev"`

	// Predicted are the keys the transaction writes when executed alone on the
	// parent state, as a builder could pre-execute it from the mempool.
	Predicted []string `json:"pw"`
}

type traceBlock struct {
	Case     string    `json:"case"`
	Number   uint64    `json:"number"`
	Coinbase string    `json:"coinbase"`
	Passes   int       `json:"passes"`
	Txs      []traceTx `json:"txs"`
}

// accessRecorder is a vm.StateDB that records the accesses of the current
// transaction, with the semantics of the parallel engine's tracking: balance
// changes that are not read are deltas, transfer guards are minimums, and a
// zero-value touch of an account with code or a nonce reads nothing more.
type accessRecorder struct {
	*state.StateDB
	coinbase common.Address
	start    time.Time
	seen     map[string]bool // kind+key already recorded
	written  map[string]bool // keys written so far: later reads of them are internal
	events   []traceEvent

	// Writes conflict only if they change the value, as validation compares
	// values: the value before the first write, and how to read the current.
	before map[string]string
	value  map[string]func() string
}

func (a *accessRecorder) begin() {
	a.start = time.Now()
	a.seen = make(map[string]bool)
	a.written = make(map[string]bool)
	a.events = nil
	a.before = make(map[string]string)
	a.value = make(map[string]func() string)
}

// prepareWrite remembers the value of key before its first write.
func (a *accessRecorder) prepareWrite(key string, value func() string) {
	if _, ok := a.value[key]; !ok {
		a.before[key] = value()
		a.value[key] = value
	}
}

// finish drops the writes that left their value unchanged.
func (a *accessRecorder) finish() []traceEvent {
	events := a.events[:0:0]
	for _, e := range a.events {
		if e.Kind == accessWrite && a.value[e.Key]() == a.before[e.Key] {
			continue
		}
		events = append(events, e)
	}
	return events
}

func (a *accessRecorder) fieldValue(addr common.Address, field byte) func() string {
	sdb := a.StateDB
	switch field {
	case 'e':
		return func() string { return fmt.Sprint(sdb.Exist(addr)) }
	case 'b':
		return func() string { return sdb.GetBalance(addr).String() }
	case 'n':
		return func() string { return fmt.Sprint(sdb.GetNonce(addr)) }
	default:
		return func() string { return sdb.GetCodeHash(addr).Hex() }
	}
}

func (a *accessRecorder) record(kind, key string) {
	if kind == accessRead && a.written[key] {
		return // reads its own write
	}
	if kind == accessWrite {
		a.written[key] = true
	}
	if a.seen[kind+key] {
		return
	}
	a.seen[kind+key] = true
	a.events = append(a.events, traceEvent{Kind: kind, Key: key, At: time.Since(a.start).Nanoseconds()})
}

func accountKey(addr common.Address, field byte) string {
	return fmt.Sprintf("%x/%c", addr[:], field)
}

func (a *accessRecorder) read(addr common.Address, fields string) {
	for i := range len(fields) {
		a.record(accessRead, accountKey(addr, fields[i]))
	}
}

func (a *accessRecorder) write(addr common.Address, fields string) {
	for i := range len(fields) {
		key := accountKey(addr, fields[i])
		a.prepareWrite(key, a.fieldValue(addr, fields[i]))
		a.record(accessWrite, key)
	}
}

func (a *accessRecorder) mayBeEmpty(addr common.Address) bool {
	hash := a.StateDB.GetCodeHash(addr)
	return a.StateDB.GetNonce(addr) == 0 && (hash == common.Hash{} || hash == types.EmptyCodeHash)
}

func (a *accessRecorder) changeBalance(addr common.Address, amount *uint256.Int) {
	if amount.IsZero() {
		a.read(addr, "enc")
		if a.mayBeEmpty(addr) {
			a.read(addr, "b")
			a.write(addr, "eb")
		}
		return
	}
	a.read(addr, "e")
	a.write(addr, "e")
	a.record(accessDelta, accountKey(addr, 'b'))
}

func (a *accessRecorder) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	a.changeBalance(addr, amount)
	return a.StateDB.AddBalance(addr, amount, reason)
}

func (a *accessRecorder) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	a.changeBalance(addr, amount)
	return a.StateDB.SubBalance(addr, amount, reason)
}

func (a *accessRecorder) GetBalance(addr common.Address) *uint256.Int {
	a.read(addr, "eb")
	return a.StateDB.GetBalance(addr)
}

func (a *accessRecorder) canTransfer(_ vm.StateDB, addr common.Address, amount *uint256.Int) bool {
	ok := a.StateDB.GetBalance(addr).Cmp(amount) >= 0
	if ok {
		a.record(accessMin, accountKey(addr, 'b'))
	} else {
		a.read(addr, "b")
	}
	return ok
}

func (a *accessRecorder) GetNonce(addr common.Address) uint64 {
	a.read(addr, "en")
	return a.StateDB.GetNonce(addr)
}

func (a *accessRecorder) SetNonce(addr common.Address, n uint64, reason tracing.NonceChangeReason) {
	a.write(addr, "en")
	a.StateDB.SetNonce(addr, n, reason)
}

func (a *accessRecorder) GetCodeHash(addr common.Address) common.Hash {
	a.read(addr, "ec")
	return a.StateDB.GetCodeHash(addr)
}

func (a *accessRecorder) GetCode(addr common.Address) []byte {
	a.read(addr, "ec")
	return a.StateDB.GetCode(addr)
}

func (a *accessRecorder) GetCodeSize(addr common.Address) int {
	a.read(addr, "ec")
	return a.StateDB.GetCodeSize(addr)
}

func (a *accessRecorder) SetCode(addr common.Address, c []byte, reason tracing.CodeChangeReason) []byte {
	a.write(addr, "ec")
	return a.StateDB.SetCode(addr, c, reason)
}

func slotKey(addr common.Address, slot common.Hash) string {
	return fmt.Sprintf("%x/%x", addr[:], slot[:])
}

func (a *accessRecorder) GetState(addr common.Address, slot common.Hash) common.Hash {
	a.record(accessRead, slotKey(addr, slot))
	return a.StateDB.GetState(addr, slot)
}

func (a *accessRecorder) GetStateAndCommittedState(addr common.Address, slot common.Hash) (common.Hash, common.Hash) {
	a.record(accessRead, slotKey(addr, slot))
	return a.StateDB.GetStateAndCommittedState(addr, slot)
}

func (a *accessRecorder) SetState(addr common.Address, slot, val common.Hash) common.Hash {
	key := slotKey(addr, slot)
	a.prepareWrite(key, func() string { return a.StateDB.GetState(addr, slot).Hex() })
	a.record(accessWrite, key)
	return a.StateDB.SetState(addr, slot, val)
}

func (a *accessRecorder) CreateAccount(addr common.Address) {
	a.write(addr, "ebnc")
	a.StateDB.CreateAccount(addr)
}

func (a *accessRecorder) CreateContract(addr common.Address) {
	a.write(addr, "ec")
	a.StateDB.CreateContract(addr)
}

func (a *accessRecorder) SelfDestruct(addr common.Address) {
	a.write(addr, "ebnc")
	a.StateDB.SelfDestruct(addr)
}

func (a *accessRecorder) Exist(addr common.Address) bool {
	a.read(addr, "e")
	return a.StateDB.Exist(addr)
}

func (a *accessRecorder) Empty(addr common.Address) bool {
	a.read(addr, "enc")
	if a.mayBeEmpty(addr) {
		a.read(addr, "b")
	}
	return a.StateDB.Empty(addr)
}

// traceReference re-executes block on its parent state: passes times without
// instrumentation to time every transaction, then once recording accesses.
func traceReference(env *benchEnv, block *types.Block, passes int) (*traceBlock, error) {
	parent := env.chain.GetHeaderByHash(block.ParentHash())
	if parent == nil {
		return nil, fmt.Errorf("parent of block %d not found", block.NumberU64())
	}
	trace := &traceBlock{Case: env.name, Number: block.NumberU64(), Coinbase: fmt.Sprintf("%x", block.Coinbase().Bytes()), Passes: passes}
	header := block.Header()
	config := env.chain.Config()
	signer := types.MakeSigner(config, header.Number, header.Time)
	for pass := range passes + 1 {
		statedb, err := env.chain.StateAt(parent)
		if err != nil {
			return nil, err
		}
		recording := pass == passes
		rec := &accessRecorder{StateDB: statedb, coinbase: header.Coinbase}
		blockCtx := core.NewEVMBlockContext(header, env.chain, nil)
		var db vm.StateDB = statedb
		if recording {
			blockCtx.CanTransfer = rec.canTransfer
			db = rec
		}
		evm := vm.NewEVM(blockCtx, db, config, vm.Config{})
		rec.begin() // the system calls are not recorded
		core.PreExecution(context.Background(), block.BeaconRoot(), parent, config, evm, header.Number, header.Time)
		gp := core.NewGasPool(header.GasLimit)
		for i, tx := range block.Transactions() {
			msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
			if err != nil {
				return nil, fmt.Errorf("tx %d: %w", i, err)
			}
			statedb.SetTxContext(tx.Hash(), i, uint32(i+1))
			rec.begin()
			receipt, _, err := core.ApplyTransactionWithEVM(msg, gp, statedb, header.Number, block.Hash(), header.Time, tx, evm)
			dur := time.Since(rec.start).Nanoseconds()
			if err != nil {
				evm.Release()
				return nil, fmt.Errorf("tx %d: %w", i, err)
			}
			if pass == 0 {
				tip, _ := tx.EffectiveGasTip(header.BaseFee)
				trace.Txs = append(trace.Txs, traceTx{
					Index:    i,
					Hash:     tx.Hash(),
					From:     fmt.Sprintf("%x", msg.From[:]),
					Nonce:    tx.Nonce(),
					Tip:      tip.String(),
					GasUsed:  receipt.GasUsed,
					Duration: dur,
					Failed:   receipt.Status == types.ReceiptStatusFailed,
				})
			}
			t := &trace.Txs[i]
			switch {
			case recording:
				t.Recorded = dur
				t.Events = rec.finish()
			case dur < t.Duration:
				t.Duration = dur
			}
		}
		evm.Release()
	}
	return trace, predictWrites(env, block, parent, trace)
}

// predictWrites executes every transaction alone on the parent state, without
// nonce checks, and records the keys it writes.
func predictWrites(env *benchEnv, block *types.Block, parent *types.Header, trace *traceBlock) error {
	header := block.Header()
	config := env.chain.Config()
	signer := types.MakeSigner(config, header.Number, header.Time)
	for i, tx := range block.Transactions() {
		statedb, err := env.chain.StateAt(parent)
		if err != nil {
			return err
		}
		rec := &accessRecorder{StateDB: statedb, coinbase: header.Coinbase}
		blockCtx := core.NewEVMBlockContext(header, env.chain, nil)
		blockCtx.CanTransfer = rec.canTransfer
		evm := vm.NewEVM(blockCtx, rec, config, vm.Config{})
		msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return err
		}
		msg.SkipNonceChecks = true
		statedb.SetTxContext(tx.Hash(), 0, 1)
		rec.begin()
		// a failure to execute alone leaves the prediction empty
		_, err = core.ApplyMessage(evm, msg, core.NewGasPool(header.GasLimit))
		evm.Release()
		if err != nil {
			continue
		}
		for _, e := range rec.finish() {
			if e.Kind == accessWrite || e.Kind == accessDelta {
				trace.Txs[i].Predicted = append(trace.Txs[i].Predicted, e.Key)
			}
		}
	}
	return nil
}

// traceOut is the encoder of -buildbench.trace, opened by TestBuildBench.
var traceOut *json.Encoder

// predictCandidateWrites executes every candidate of env alone on the parent
// state, as a builder could from its mempool, and returns what each one
// writes exactly, the balances it changes additively and those it reads
// exactly.
var buildBenchPredictTargeted = flag.Bool("buildbench.predicttargeted", false, "pre-execute only calls to contracts that wrote contended keys in earlier cases (learned across cases); implies -buildbench.predictstatic")

// explore picks the unknown targets to predict anyway.
var explore = rand.New(rand.NewSource(1))

// targets learns, across cases, which call targets write contended keys.
var targets = struct {
	hot     map[common.Address]bool // call targets whose calls wrote contended addresses
	learned bool
}{hot: make(map[common.Address]bool)}

var buildBenchPredictExplore = flag.Float64("buildbench.predictexplore", 0, "with -buildbench.predicttargeted, also pre-execute this fraction of calls to unknown targets (seeded), to discover new hot contracts")

var buildBenchPredictStatic = flag.Bool("buildbench.predictstatic", false, "predict plain ETH transfers statically instead of executing them")

var buildBenchPredict = flag.String("buildbench.predict", "parent", "predict mainnet candidates on the parent state of the block built (parent) or of the block each was included in (origin), as a node would on the head at the transaction's arrival")

// preexecuteCandidates executes every candidate alone on the parent state,
// with the header of the block being built, as a node would on arrival.
func preexecuteCandidates(env *benchEnv) (map[common.Hash]*parallel.Result, error) {
	parent, header, err := predictionHeader(env)
	if err != nil {
		return nil, err
	}
	db := state.NewMPTDatabase(env.chain.TrieDB(), env.chain.CodeDB()).WithSnapshot(env.chain.Snapshots())
	reader, err := db.Reader(parent.Root)
	if err != nil {
		return nil, err
	}
	pre, err := parallel.NewPreexecutor(env.chain, env.chain.Config(), db, parent.Root, header, parallel.NewSharedReader(reader))
	if err != nil {
		return nil, err
	}
	// The transactions of one sender run in nonce order, each on top of the
	// results of the earlier ones (unless -buildbench.preexecalone), on
	// -buildbench.preexecpar goroutines; busy is their summed time.
	signer := types.LatestSigner(env.chain.Config())
	var (
		chains  [][]int
		chainOf = make(map[common.Address]int)
	)
	for i, tx := range env.candidates {
		from, err := types.Sender(signer, tx)
		if err != nil {
			return nil, err
		}
		c, ok := chainOf[from]
		if !ok || *buildBenchPreexecAlone {
			c = len(chains)
			chainOf[from] = c
			chains = append(chains, nil)
		}
		chains[c] = append(chains[c], i)
	}
	results := make([]*parallel.Result, len(env.candidates))
	var (
		next atomic.Int64
		busy atomic.Int64
		wg   sync.WaitGroup
	)
	for range max(1, *buildBenchPreexecPar) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c := int(next.Add(1)) - 1
				if c >= len(chains) {
					return
				}
				started := time.Now()
				var basis []*parallel.Result
				for _, i := range chains[c] {
					r := pre.RunAfter(env.candidates[i], basis)
					results[i] = r
					if r.Usable() {
						basis = append(basis, r)
					}
				}
				busy.Add(time.Since(started).Nanoseconds())
			}
		}()
	}
	wg.Wait()
	env.preexecBusyNs = busy.Load()
	out := make(map[common.Hash]*parallel.Result, len(env.candidates))
	for i, tx := range env.candidates {
		out[tx.Hash()] = results[i]
	}
	return out, nil
}

// predictionHeader returns the parent and the header of the block being
// built, as far as executions depend on it.
func predictionHeader(env *benchEnv) (*types.Header, *types.Header, error) {
	params := env.params()
	parent := env.chain.GetHeaderByHash(params.parentHash)
	if parent == nil {
		return nil, nil, fmt.Errorf("parent %x not found", params.parentHash)
	}
	config := env.chain.Config()
	header := &types.Header{
		ParentHash:       parent.Hash(),
		Number:           new(big.Int).Add(parent.Number, common.Big1),
		Time:             params.timestamp,
		Coinbase:         params.coinbase,
		GasLimit:         core.CalcGasLimit(parent.GasLimit, env.gasCeil),
		BaseFee:          eip1559.CalcBaseFee(config, parent),
		Difficulty:       new(big.Int),
		MixDigest:        params.random,
		ParentBeaconRoot: params.beaconRoot,
	}
	if config.IsCancun(header.Number, header.Time) {
		excess := eip4844.CalcExcessBlobGas(config, parent, header.Time)
		header.ExcessBlobGas = &excess
	}
	if config.IsAmsterdam(header.Number, header.Time) {
		header.SlotNumber = params.slotNum
	}
	return parent, header, nil
}

func predictCandidateWrites(env *benchEnv) (map[common.Hash]parallel.Prediction, error) {
	parent, header, err := predictionHeader(env)
	if err != nil {
		return nil, err
	}
	config := env.chain.Config()
	// contextFor returns the state and block context a candidate is predicted on
	contextFor := func(tx *types.Transaction) (*types.Header, *types.Header) {
		if *buildBenchPredict == "origin" {
			if h := env.origin[tx.Hash()]; h != nil {
				if p := env.chain.GetHeaderByHash(h.ParentHash); p != nil && env.chain.HasState(p.Root) {
					return p, h
				}
			}
		}
		return parent, header
	}
	signer := types.LatestSigner(config)
	out := make(map[common.Hash]parallel.Prediction, len(env.candidates))
	// predictions on the same state share their parent reads, as a mempool
	// pre-executing arriving transactions on the head would
	db := state.NewMPTDatabase(env.chain.TrieDB(), env.chain.CodeDB()).WithSnapshot(env.chain.Snapshots())
	readers := make(map[common.Hash]*parallel.SharedReader)
	for _, tx := range env.candidates {
		parent, header := contextFor(tx)
		if *buildBenchPredictStatic || *buildBenchPredictTargeted {
			if p, ok := staticTransferPrediction(env, tx, signer, header.Coinbase, parent); ok {
				out[tx.Hash()] = p
				continue
			}
		}
		if *buildBenchPredictTargeted && targets.learned && tx.To() != nil && !targets.hot[*tx.To()] &&
			explore.Float64() >= *buildBenchPredictExplore {
			continue // not known to contend: no prediction, validation catches conflicts
		}
		shared := readers[parent.Root]
		if shared == nil {
			reader, err := db.Reader(parent.Root)
			if err != nil {
				return nil, err
			}
			shared = parallel.NewSharedReader(reader)
			readers[parent.Root] = shared
		}
		statedb, err := state.NewWithReader(parent.Root, db, shared)
		if err != nil {
			return nil, err
		}
		rec := &accessRecorder{StateDB: statedb, coinbase: header.Coinbase}
		blockCtx := core.NewEVMBlockContext(header, env.chain, nil)
		blockCtx.CanTransfer = rec.canTransfer
		evm := vm.NewEVM(blockCtx, rec, config, vm.Config{})
		msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			evm.Release()
			continue
		}
		msg.SkipNonceChecks = true
		statedb.SetTxContext(tx.Hash(), 0, 1)
		rec.begin()
		_, err = core.ApplyMessage(evm, msg, core.NewGasPool(header.GasLimit))
		evm.Release()
		if err != nil {
			continue
		}
		var p parallel.Prediction
		for _, e := range rec.finish() {
			addr, rest, _ := strings.Cut(e.Key, "/")
			a := common.HexToAddress(addr)
			switch {
			case e.Kind == accessDelta:
				p.Deltas = append(p.Deltas, a)
			case e.Kind == accessRead && rest == "b":
				p.Observes = append(p.Observes, a)
			case e.Kind == accessWrite:
				w := parallel.WriteKey{Addr: a}
				if len(rest) == 1 {
					w.Field = rest[0]
				} else {
					w.Field, w.Slot = 's', common.HexToHash(rest)
				}
				p.Writes = append(p.Writes, w)
			}
		}
		out[tx.Hash()] = p
	}
	if *buildBenchPredictTargeted {
		learnTargets(env, out)
	}
	return out, nil
}

// staticTransferPrediction predicts a plain ETH transfer (no calldata, to an
// account without code) without executing it: the sender's nonce and balance
// are written and its balance read, the recipient's balance and the
// coinbase's change additively.
func staticTransferPrediction(env *benchEnv, tx *types.Transaction, signer types.Signer, coinbase common.Address, parent *types.Header) (parallel.Prediction, bool) {
	to := tx.To()
	if to == nil || len(tx.Data()) > 0 || len(tx.SetCodeAuthorizations()) > 0 {
		return parallel.Prediction{}, false
	}
	statedb, err := env.chain.StateAt(parent)
	if err != nil || statedb.GetCodeSize(*to) > 0 {
		return parallel.Prediction{}, false
	}
	from, err := types.Sender(signer, tx)
	if err != nil {
		return parallel.Prediction{}, false
	}
	p := parallel.Prediction{
		Writes:   []parallel.WriteKey{{Addr: from, Field: 'n'}, {Addr: from, Field: 'b'}},
		Deltas:   []common.Address{coinbase},
		Observes: []common.Address{from},
	}
	if tx.Value().Sign() > 0 {
		p.Deltas = append(p.Deltas, *to)
	}
	return p, true
}

// learnTargets records the call targets whose predicted writes touched an
// address written by at least two candidates.
func learnTargets(env *benchEnv, predicted map[common.Hash]parallel.Prediction) {
	writers := make(map[common.Address]int)
	for _, p := range predicted {
		seen := make(map[common.Address]bool)
		for _, w := range p.Writes {
			seen[w.Addr] = true
		}
		for a := range seen {
			writers[a]++
		}
	}
	for _, tx := range env.candidates {
		p, ok := predicted[tx.Hash()]
		if !ok || tx.To() == nil || len(tx.Data()) == 0 {
			continue
		}
		for _, w := range p.Writes {
			if writers[w.Addr] >= 2 {
				targets.hot[*tx.To()] = true
				break
			}
		}
	}
	targets.learned = true
}
