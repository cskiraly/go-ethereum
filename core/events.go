// Copyright 2014 The go-ethereum Authors
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

package core

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// NewTxsEvent is posted when a batch of transactions enter the transaction pool.
type NewTxsEvent struct{ Txs []*types.Transaction }

// RemovalReason describes why a previously-accepted transaction was
// removed from the transaction pool. The String form is the short,
// lowercase label exposed to event consumers (RPC feeds, logs);
// programmatic consumers should dispatch on the typed value, never
// on the label text.
type RemovalReason uint8

const (
	RemovalUnknown RemovalReason = iota
	RemovalReplaced
	RemovalUnderpriced
	RemovalCapacity
	RemovalRateLimited
	RemovalNonceExpired
	RemovalNonceGap
	RemovalUnderfunded
	RemovalInvalid
	RemovalDuplicate
	RemovalIncluded
	RemovalExpired
	RemovalGasLimitExceeded
)

// String returns the short lowercase label for the removal reason.
func (r RemovalReason) String() string {
	switch r {
	case RemovalReplaced:
		return "replaced"
	case RemovalUnderpriced:
		return "underpriced"
	case RemovalCapacity:
		return "capacity"
	case RemovalRateLimited:
		return "rate limited"
	case RemovalNonceExpired:
		return "nonce expired"
	case RemovalNonceGap:
		return "nonce gap"
	case RemovalUnderfunded:
		return "underfunded"
	case RemovalInvalid:
		return "invalid"
	case RemovalDuplicate:
		return "duplicate"
	case RemovalIncluded:
		return "included"
	case RemovalExpired:
		return "expired"
	case RemovalGasLimitExceeded:
		return "gas limit exceeded"
	}
	return "unknown"
}

// RemovedTxsEvent is posted when one or more transactions are evicted
// from the transaction pool after acceptance (replaced, underpriced,
// truncated for capacity, dropped during reorg cleanup, etc.).
//
// Hashes and Reasons are positionally aligned: Reasons[i] explains why
// Hashes[i] was removed.
type RemovedTxsEvent struct {
	Hashes  []common.Hash
	Reasons []RemovalReason
}

// RemovedLogsEvent is posted when a reorg happens
type RemovedLogsEvent struct{ Logs []*types.Log }

type ChainEvent struct {
	Header       *types.Header
	Receipts     []*types.Receipt
	Transactions []*types.Transaction
}

type ChainHeadEvent struct {
	Header *types.Header
}

// NewPayloadEvent is posted when engine_newPayloadVX processes a block.
type NewPayloadEvent struct {
	Hash           common.Hash
	Number         uint64
	ProcessingTime time.Duration
}
