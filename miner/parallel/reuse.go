package parallel

import (
	"maps"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"
)

// headerReads records which header values an execution read through the
// EVM. A pre-executed result depends on those, not on the whole header, so a
// build with another header can use it if they are the same there.
type headerReads uint16

const (
	readTime headerReads = 1 << iota
	readNumber
	readCoinbase
	readRandom
	readGasLimit
	readBaseFee
	readBlobBaseFee
	readBlockHash
	readGasPrice
	readSlot
)

var headerOps = [256]headerReads{
	vm.TIMESTAMP:   readTime,
	vm.NUMBER:      readNumber,
	vm.COINBASE:    readCoinbase,
	vm.PREVRANDAO:  readRandom,
	vm.GASLIMIT:    readGasLimit,
	vm.BASEFEE:     readBaseFee,
	vm.BLOBBASEFEE: readBlobBaseFee,
	vm.BLOCKHASH:   readBlockHash,
	vm.GASPRICE:    readGasPrice,
	vm.SLOTNUM:     readSlot,
}

// hooks returns tracing hooks that add the header values read to h.
func (h *headerReads) hooks() *tracing.Hooks {
	return &tracing.Hooks{OnOpcode: func(_ uint64, op byte, _, _ uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
		*h |= headerOps[op]
	}}
}

// execEnv is the environment pre-executed results ran in: the header, the
// coinbase, the blob base fee and the writes of the block's system calls.
type execEnv struct {
	header   *types.Header
	coinbase common.Address
	blobFee  *big.Int // nil before Cancun
	seed     map[key]value
}

// payment is what a transaction paid for gas, and what it would pay with
// another base fee: its sender's balance changes by gasUsed times the price,
// plus blob gas times the blob price, and the coinbase gets gasUsed times the
// tip.
type payment struct {
	sender                     common.Address
	gasUsed, gasLimit, blobGas uint64
	feeCap, tipCap, blobFeeCap *big.Int
	price, tip, blobPrice      *big.Int // as executed
}

func (e *executor) payment(tx *types.Transaction, sender common.Address, gasUsed uint64) *payment {
	p := &payment{sender: sender, gasUsed: gasUsed, gasLimit: tx.Gas(), feeCap: tx.GasFeeCap(), tipCap: tx.GasTipCap()}
	p.price, p.tip, _ = effectivePrice(p.feeCap, p.tipCap, e.header.BaseFee)
	if blobGas := tx.BlobGas(); blobGas > 0 && e.env.blobFee != nil {
		p.blobGas, p.blobFeeCap, p.blobPrice = blobGas, tx.BlobGasFeeCap(), e.env.blobFee
	}
	return p
}

// effectivePrice returns the gas price and the tip per gas a transaction
// pays at baseFee (nil before London), or false if its fee cap is below it.
func effectivePrice(feeCap, tipCap, baseFee *big.Int) (price, tip *big.Int, ok bool) {
	if baseFee == nil {
		return feeCap, feeCap, true
	}
	if feeCap.Cmp(baseFee) < 0 {
		return nil, nil, false
	}
	tip = new(big.Int).Sub(feeCap, baseFee)
	if tip.Cmp(tipCap) > 0 {
		tip.Set(tipCap)
	}
	return new(big.Int).Add(baseFee, tip), tip, true
}

func bigEqual(a, b *big.Int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Cmp(b) == 0
}

func optEqual(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// sameHeaderReads reports whether the header values r read are the same in
// this build.
func (e *Engine) sameHeaderReads(r *Result) bool {
	h, b, reads := r.env.header, e.exec.header, r.headerReads
	return (reads&readTime == 0 || h.Time == b.Time) &&
		(reads&(readNumber|readBlockHash) == 0 || bigEqual(h.Number, b.Number)) &&
		(reads&readCoinbase == 0 || r.env.coinbase == e.exec.coinbase) &&
		(reads&readRandom == 0 || h.MixDigest == b.MixDigest && bigEqual(h.Difficulty, b.Difficulty)) &&
		(reads&readGasLimit == 0 || h.GasLimit == b.GasLimit) &&
		(reads&readBaseFee == 0 || bigEqual(h.BaseFee, b.BaseFee)) &&
		(reads&readBlobBaseFee == 0 || bigEqual(r.env.blobFee, e.blobFee)) &&
		(reads&readSlot == 0 || optEqual(h.SlotNumber, b.SlotNumber))
}

// seedDiff returns the keys the system calls of env and of this build wrote
// differently, such as the beacon root's slot, which depends on the time.
func (e *Engine) seedDiff(env *execEnv) map[key]bool {
	if d, ok := e.seedDiffs[env]; ok {
		return d
	}
	d := make(map[key]bool)
	for k, v := range env.seed {
		if w, ok := e.seed[k]; !ok || w != v {
			d[k] = true
		}
	}
	for k := range e.seed {
		if _, ok := env.seed[k]; !ok {
			d[k] = true
		}
	}
	e.seedDiffs[env] = d
	return d
}

// touches reports whether r accessed any state of addr.
func (r *Result) touches(addr common.Address) bool {
	for k := range r.reads {
		if k.addr == addr {
			return true
		}
	}
	for k := range r.writes {
		if k.addr == addr {
			return true
		}
	}
	return r.deltas[addr] != nil || r.minBalance[addr] != nil
}

// plainSender reports whether r's sender had no code and got none, so that
// no code of its own could spend its balance.
func (r *Result) plainSender() bool {
	k := key{addr: r.pay.sender, field: code}
	if _, wrote := r.writes[k]; wrote {
		return false
	}
	v, read := r.reads[k]
	return read && (v.hash() == common.Hash{} || v.hash() == types.EmptyCodeHash)
}

// adapt returns the pre-executed result r for this build. A result that
// read only header values the build shares is used as it is, or, if the base
// fee, the blob base fee or the coinbase differ, as a copy whose payment is
// recomputed: the sender's balance delta, its requirement and the coinbase
// credit. A result that depends on what differs becomes a prediction only. A
// result executed on top of others of its sender uses their adapted results.
func (e *Engine) adapt(r *Result) *Result {
	if r.context != e.context || r.env == nil {
		return nil
	}
	if !r.Usable() {
		return r
	}
	predict := func() *Result {
		cp := *r
		cp.predictOnly = true
		e.stats.PreexecRefused++
		return &cp
	}
	if !e.sameHeaderReads(r) {
		return predict()
	}
	if diff := e.seedDiff(r.env); len(diff) > 0 {
		for k := range r.reads {
			if diff[k] {
				return predict()
			}
		}
	}
	cp := *r
	changed := false
	if len(r.basis) > 0 {
		cp.basis = make([]*Result, len(r.basis))
		for i, b := range r.basis {
			ab := e.adapted[b]
			if ab == nil || !ab.Usable() {
				return predict()
			}
			if ab.adjusted {
				// b's payment changed the sender's balance it left
				if _, read := r.reads[key{addr: ab.pay.sender, field: balance}]; read {
					return predict()
				}
			}
			cp.basis[i] = ab
			changed = changed || ab != b
		}
	}
	p, b := r.pay, e.exec.header
	if p == nil {
		return predict()
	}
	if p.gasLimit > b.GasLimit {
		return predict()
	}
	price, tip, ok := effectivePrice(p.feeCap, p.tipCap, b.BaseFee)
	if !ok {
		return predict()
	}
	blobPrice := p.blobPrice
	if p.blobGas > 0 {
		if blobPrice = e.blobFee; blobPrice == nil || p.blobFeeCap.Cmp(blobPrice) < 0 {
			return predict()
		}
	}
	coinbase, cbChanged := e.exec.coinbase, r.env.coinbase != e.exec.coinbase
	if price.Cmp(p.price) != 0 && r.headerReads&readGasPrice != 0 {
		return predict()
	}
	gasUsed, blobGas := new(big.Int).SetUint64(p.gasUsed), new(big.Int).SetUint64(p.blobGas)
	cloned, rebased := false, false
	cloneDeltas := func() {
		if !cloned {
			cp.deltas = maps.Clone(r.deltas)
			if cp.deltas == nil {
				cp.deltas = make(map[common.Address]*big.Int)
			}
			cloned = true
		}
	}
	addDelta := func(addr common.Address, d *big.Int) {
		cloneDeltas()
		sum := new(big.Int).Set(d)
		if old := cp.deltas[addr]; old != nil {
			sum.Add(sum, old)
		}
		if sum.Sign() == 0 {
			delete(cp.deltas, addr)
		} else {
			cp.deltas[addr] = sum
		}
	}
	// the sender: gasUsed * (price - price') + blobGas * (blobPrice - blobPrice')
	refund := new(big.Int).Mul(gasUsed, new(big.Int).Sub(p.price, price))
	if p.blobGas > 0 {
		refund.Add(refund, new(big.Int).Mul(blobGas, new(big.Int).Sub(p.blobPrice, blobPrice)))
	}
	if refund.Sign() != 0 {
		if _, exact := r.writes[key{addr: p.sender, field: balance}]; exact {
			return predict()
		}
		addDelta(p.sender, refund)
		cp.adjusted, rebased = true, true
	}
	// A higher price raises what the gas purchase takes before execution,
	// gasLimit * price (+ blob fees), so what later guards on the sender's
	// balance require, by at most the same. The purchase's own requirement,
	// at the fee caps, does not change, and covers the only later guard of a
	// sender without code: the top call's value transfer.
	upfront := new(big.Int).Mul(new(big.Int).SetUint64(p.gasLimit), new(big.Int).Sub(price, p.price))
	if p.blobGas > 0 {
		upfront.Add(upfront, new(big.Int).Mul(blobGas, new(big.Int).Sub(blobPrice, p.blobPrice)))
	}
	if upfront.Sign() > 0 && !r.plainSender() {
		need := r.minBalance[p.sender]
		if need == nil {
			return predict()
		}
		more, overflow := uint256.FromBig(upfront)
		if overflow {
			return predict()
		}
		cp.minBalance = maps.Clone(r.minBalance)
		cp.minBalance[p.sender] = new(uint256.Int).Add(need, more)
		rebased = true
	}
	// the coinbase: gasUsed * tip'
	if cbChanged || tip.Cmp(p.tip) != 0 {
		fee := new(big.Int)
		if r.fee != nil {
			fee = r.fee.ToBig()
		}
		credit := new(big.Int).Mul(gasUsed, p.tip)
		switch {
		case cbChanged:
			// the credit moves to another account: r must not have
			// touched either, nor credited the old one otherwise, as the
			// EVM warms the coinbase and the fee lumps all its credits
			if r.touches(r.env.coinbase) || r.touches(coinbase) || fee.Cmp(credit) != 0 || r.AccessList != nil {
				return predict()
			}
			fee.Mul(gasUsed, tip)
		default:
			if _, exact := r.writes[key{addr: coinbase, field: balance}]; exact {
				return predict()
			}
			more := new(big.Int).Mul(gasUsed, new(big.Int).Sub(tip, p.tip))
			if r.deltas[coinbase] != nil {
				addDelta(coinbase, more)
			} else {
				fee.Add(fee, more)
			}
		}
		if fee.Sign() < 0 {
			return predict()
		}
		cp.fee = nil
		if fee.Sign() > 0 {
			cp.fee, _ = uint256.FromBig(fee)
		}
		if r.AccessList != nil && (r.fee == nil) != (cp.fee == nil) {
			return predict()
		}
		rebased = true
	}
	if p.blobGas > 0 && blobPrice.Cmp(p.blobPrice) != 0 && r.Receipt != nil {
		receipt := *r.Receipt
		receipt.BlobGasPrice = blobPrice
		cp.Receipt = &receipt
		rebased = true
	}
	if rebased {
		e.stats.PreexecRebased++
	} else if !changed {
		return r
	}
	return &cp
}
