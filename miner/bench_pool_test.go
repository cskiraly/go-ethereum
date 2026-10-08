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
	"cmp"
	"math/big"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/holiman/uint256"
)

// fixedPool is a txpool.SubPool serving a fixed set of candidate transactions,
// so that every build in a benchmark sees exactly the same input regardless of
// the parent block. It does no validation; Pending filters like legacypool.
type fixedPool struct {
	pending map[common.Address][]*types.Transaction // nonce ordered per sender
	byHash  map[common.Hash]*types.Transaction
	times   map[common.Hash]time.Time
	count   int
}

// newFixedPool groups txs by sender. Arrival times follow the slice order so
// that equal-priced transactions are ordered deterministically.
func newFixedPool(signer types.Signer, txs []*types.Transaction) (*fixedPool, error) {
	p := &fixedPool{
		pending: make(map[common.Address][]*types.Transaction),
		byHash:  make(map[common.Hash]*types.Transaction, len(txs)),
		times:   make(map[common.Hash]time.Time, len(txs)),
	}
	base := time.Unix(1_700_000_000, 0)
	for i, tx := range txs {
		from, err := types.Sender(signer, tx)
		if err != nil {
			return nil, err
		}
		p.pending[from] = append(p.pending[from], tx)
		p.byHash[tx.Hash()] = tx
		p.times[tx.Hash()] = base.Add(time.Duration(i) * time.Microsecond)
	}
	for _, list := range p.pending {
		slices.SortFunc(list, func(a, b *types.Transaction) int {
			return cmp.Compare(a.Nonce(), b.Nonce())
		})
	}
	p.count = len(txs)
	return p, nil
}

func (p *fixedPool) Pending(filter txpool.PendingFilter) (map[common.Address][]*txpool.LazyTransaction, int) {
	var count int
	pending := make(map[common.Address][]*txpool.LazyTransaction, len(p.pending))
	for addr, list := range p.pending {
		var lazies []*txpool.LazyTransaction
		for _, tx := range list {
			if (tx.Type() == types.BlobTxType) != filter.BlobTxs {
				continue
			}
			if filter.MinTip != nil && tx.EffectiveGasTipIntCmp(filter.MinTip, filter.BaseFee) < 0 {
				break
			}
			if filter.GasLimitCap != 0 && tx.Gas() > filter.GasLimitCap {
				break
			}
			lazies = append(lazies, &txpool.LazyTransaction{
				Pool:      p,
				Hash:      tx.Hash(),
				Tx:        tx,
				Time:      p.times[tx.Hash()],
				GasFeeCap: uint256.MustFromBig(tx.GasFeeCap()),
				GasTipCap: uint256.MustFromBig(tx.GasTipCap()),
				Gas:       tx.Gas(),
				BlobGas:   tx.BlobGas(),
			})
		}
		if len(lazies) > 0 {
			pending[addr] = lazies
			count += len(lazies)
		}
	}
	return pending, count
}

func (p *fixedPool) Get(hash common.Hash) *types.Transaction { return p.byHash[hash] }
func (p *fixedPool) Has(hash common.Hash) bool               { return p.byHash[hash] != nil }

func (p *fixedPool) Filter(tx *types.Transaction) bool { return true }
func (p *fixedPool) FilterType(kind byte) bool         { return true }
func (p *fixedPool) Init(gasTip uint64, head *types.Header, reserver txpool.Reserver) error {
	return nil
}
func (p *fixedPool) Close() error                                    { return nil }
func (p *fixedPool) Reset(oldHead, newHead *types.Header)            {}
func (p *fixedPool) SetGasTip(tip *big.Int)                          {}
func (p *fixedPool) GetRLP(hash common.Hash, version uint) []byte    { return nil }
func (p *fixedPool) GetMetadata(hash common.Hash) *txpool.TxMetadata { return nil }
func (p *fixedPool) ValidateTxBasics(tx *types.Transaction) error    { return nil }
func (p *fixedPool) Add(txs []*types.Transaction, sync bool) []error {
	return make([]error, len(txs))
}
func (p *fixedPool) SubscribeTransactions(ch chan<- core.NewTxsEvent, reorgs bool) event.Subscription {
	return event.NewSubscription(func(quit <-chan struct{}) error { <-quit; return nil })
}
func (p *fixedPool) Nonce(addr common.Address) uint64 { return 0 }
func (p *fixedPool) Stats() (int, int)                { return p.count, 0 }
func (p *fixedPool) Content() (map[common.Address][]*types.Transaction, map[common.Address][]*types.Transaction) {
	return p.pending, nil
}
func (p *fixedPool) ContentFrom(addr common.Address) ([]*types.Transaction, []*types.Transaction) {
	return p.pending[addr], nil
}
func (p *fixedPool) Status(hash common.Hash) txpool.TxStatus {
	if p.Has(hash) {
		return txpool.TxStatusPending
	}
	return txpool.TxStatusUnknown
}
func (p *fixedPool) Clear() {}
