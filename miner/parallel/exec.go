package parallel

import (
	"errors"
	"fmt"
	"math"
	"runtime/debug"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

var (
	errUnavailable = errors.New("transaction no longer available")
	errRunStopped  = errors.New("run stopped while waiting")
)

// noVersions lies below every position, so a reader at it serves only its parent.
const noVersions = math.MinInt

// allVersions lies above every position, so a reader at it serves the newest versions.
const allVersions = math.MaxInt

// executor runs tasks against the state seen from their position.
type executor struct {
	chain     core.ChainContext
	config    *params.ChainConfig
	db        state.Database
	root      common.Hash
	header    *types.Header
	coinbase  common.Address
	store     *store
	committed *store
	parent    state.Reader
	waiter    *writeWaiter     // set during a run with predictions
	jumpDests vm.JumpDestCache // code analysis shared by the executions, as one EVM would

	// env, if set, makes results record the header values they read and
	// what they paid, so that a build with another header can use them.
	env *execEnv
}

// run executes t on the parent state plus the versions below its position. A
// requeued task was stale once already, so it runs on the committed writes
// instead, which is the block state as of now.
func (e *executor) run(t *Task) *Result {
	if t.requeues > 0 {
		return e.execute(t, newVersionedStateReader(allVersions, e.committed, e.parent))
	}
	reader := newVersionedStateReader(t.Position, e.store, e.parent)
	if e.waiter != nil {
		w := e.waiter
		reader.wait = func(k key) {
			w.wait(k, t.Position)
			if k.field == balance && t.observes[k.addr] {
				w.waitDeltas(k.addr, t.Position)
			}
		}
	}
	return e.execute(t, reader)
}

// runOn executes t on the block state alone, as a sequential builder would.
func (e *executor) runOn(t *Task, sdb *state.StateDB) *Result {
	return e.execute(t, newVersionedStateReader(noVersions, e.store, blockStateReader{sdb}))
}

func (e *executor) execute(t *Task, reader *versionedStateReader) (res *Result) {
	defer func() {
		if r := recover(); r != nil {
			if r == errRunStopped {
				res = &Result{Err: errRunStopped}
				return
			}
			log.Error("parallel transaction execution panicked", "hash", t.Lazy.Hash, "position", t.Position, "err", r, "stack", string(debug.Stack()))
			res = &Result{Err: fmt.Errorf("execution panic: %v", r)}
		}
	}()
	tx := t.resolve()
	if tx == nil {
		return &Result{Err: errUnavailable}
	}
	sdb, err := state.NewWithReader(e.root, e.db, reader)
	if err != nil {
		return &Result{Err: err}
	}
	trackedStateDB := newTrackingStateDB(sdb, reader, e.coinbase)
	signer := types.MakeSigner(e.config, e.header.Number, e.header.Time)
	if sender, err := types.Sender(signer, tx); err == nil {
		trackedStateDB.sender, trackedStateDB.upfront = sender, e.upfront(tx)
	}
	blockCtx := core.NewEVMBlockContext(e.header, e.chain, &e.coinbase)
	blockCtx.CanTransfer = trackedStateDB.canTransfer
	var (
		vmConfig vm.Config
		hdr      headerReads
	)
	if e.env != nil {
		vmConfig.Tracer = hdr.hooks()
	}
	evm := vm.NewEVM(blockCtx, trackedStateDB, e.config, vmConfig)
	defer evm.Release()
	if e.jumpDests != nil {
		evm.SetJumpDestCache(e.jumpDests)
	}

	gas := core.NewGasPool(e.header.GasLimit)
	before := gas.Snapshot()
	sdb.SetTxContext(tx.Hash(), t.Position, uint32(t.Position+1))
	var (
		receipt    *types.Receipt
		accessList *bal.ConstructionBlockAccessList
	)
	if t.skipNonce {
		var msg *core.Message
		msg, err = core.TransactionToMessage(tx, signer, e.header.BaseFee)
		if err == nil {
			msg.SkipNonceChecks = true
			receipt, accessList, err = core.ApplyTransactionWithEVM(msg, gas, sdb, e.header.Number, e.header.Hash(), e.header.Time, tx, evm)
		}
	} else {
		receipt, accessList, err = core.ApplyTransaction(evm, gas, sdb, e.header, tx)
	}
	if err != nil {
		reads, readErr := trackedStateDB.reads()
		if readErr != nil {
			err = readErr
		}
		return &Result{Err: err, reads: reads, loads: reader.loads}
	}
	if res, err = trackedStateDB.result(); err != nil {
		return &Result{Err: err}
	}
	res.loads = reader.loads
	if res.Gas, err = gas.TransactionDelta(before); err != nil {
		return &Result{Err: err}
	}
	res.Receipt, res.AccessList = receipt, accessList
	if e.env != nil {
		res.env, res.headerReads = e.env, hdr
		res.pay = e.payment(tx, trackedStateDB.sender, receipt.GasUsed)
	}
	return res
}

// upfront returns the balance the gas purchase requires of tx's sender, as
// core's buyGas computes it, or nil if it overflows.
func (e *executor) upfront(tx *types.Transaction) *uint256.Int {
	need, overflow := uint256.FromBig(tx.GasFeeCap())
	if overflow {
		return nil
	}
	if _, overflow = need.MulOverflow(need, uint256.NewInt(tx.Gas())); overflow {
		return nil
	}
	value, overflow := uint256.FromBig(tx.Value())
	if overflow {
		return nil
	}
	if _, overflow = need.AddOverflow(need, value); overflow {
		return nil
	}
	if blobGas := tx.BlobGas(); blobGas > 0 && e.config.IsCancun(e.header.Number, e.header.Time) {
		blobCap, overflow := uint256.FromBig(tx.BlobGasFeeCap())
		if overflow {
			return nil
		}
		if _, overflow = blobCap.MulOverflow(blobCap, uint256.NewInt(blobGas)); overflow {
			return nil
		}
		if _, overflow = need.AddOverflow(need, blobCap); overflow {
			return nil
		}
	}
	return need
}
