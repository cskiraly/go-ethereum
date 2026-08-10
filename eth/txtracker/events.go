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

package txtracker

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// TxStatus enumerates the hot-state lifecycle a transaction travels through
// inside a geth node, from first observation to block finalization (or to
// terminal rejection/drop). The values are stable and meant to be exposed
// to external consumers (RPC, UI).
type TxStatus uint8

const (
	// StatusUnknown is the zero value; used for "previous status" fields
	// on the first state change for a tx.
	StatusUnknown TxStatus = iota
	// StatusAnnounced — a remote peer announced the hash (no body yet).
	StatusAnnounced
	// StatusRequested — we sent a GetPooledTransactions for it.
	StatusRequested
	// StatusReceived — the body arrived (from a direct reply or broadcast)
	// but has not yet been accepted by the tx pool.
	StatusReceived
	// StatusPooled — the tx pool accepted the tx and is holding it for
	// inclusion.
	StatusPooled
	// StatusIncluded — the tx appeared in a chain-head block.
	StatusIncluded
	// StatusFinalized — the block containing the tx is at or below the
	// finalized head.
	StatusFinalized
	// StatusRejected — the tx pool rejected the tx on submission
	// (underpriced, invalid nonce, etc.). Terminal.
	StatusRejected
	// StatusDropped — the tx was removed from the pool after acceptance
	// (replaced, evicted, reorged-out-and-not-readmitted). Terminal, but
	// a new submission of the same hash can start the lifecycle again.
	StatusDropped
)

// String returns a short, stable lowercase label suitable for logs and
// JSON representation.
func (s TxStatus) String() string {
	switch s {
	case StatusAnnounced:
		return "announced"
	case StatusRequested:
		return "requested"
	case StatusReceived:
		return "received"
	case StatusPooled:
		return "pooled"
	case StatusIncluded:
		return "included"
	case StatusFinalized:
		return "finalized"
	case StatusRejected:
		return "rejected"
	case StatusDropped:
		return "dropped"
	default:
		return "unknown"
	}
}

// ObsKind enumerates the raw per-tx facts the tracker observes. Each
// concrete thing that happens with a tx hash produces exactly one
// Observation of the corresponding kind. Directionality (inbound vs.
// outbound) is baked into the kind so that subscribers don't need a
// separate field.
type ObsKind uint8

const (
	// ObsUnknown is the zero value; never emitted.
	ObsUnknown ObsKind = iota
	// ObsAnnouncedInbound — a peer advertised a hash to us (eth/68
	// NewPooledTransactionHashes).
	ObsAnnouncedInbound
	// ObsAnnouncedOutbound — we advertised a hash to a peer.
	ObsAnnouncedOutbound
	// ObsRequestedOutbound — we sent a GetPooledTransactions.
	ObsRequestedOutbound
	// ObsRequestedInbound — a peer sent us a GetPooledTransactions we
	// then served.
	ObsRequestedInbound
	// ObsDeliveredInbound — a peer delivered the tx body to us (direct
	// reply to our request, or broadcast).
	ObsDeliveredInbound
	// ObsDeliveredOutbound — we sent the tx body to a peer (broadcast).
	ObsDeliveredOutbound
	// ObsPoolAccepted — the tx pool accepted the tx on submission.
	ObsPoolAccepted
	// ObsPoolRejected — the tx pool rejected the tx on submission.
	ObsPoolRejected
	// ObsPoolEvicted — the tx pool removed a previously-accepted tx
	// (replaced, evicted for capacity, reorged-out and not re-added).
	ObsPoolEvicted
	// ObsChainIncluded — a chain-head block contained the tx.
	ObsChainIncluded
	// ObsChainReorged — a chain-head transition removed a block that
	// had contained the tx. Emitted by the reorg walk in
	// handleChainHead (collectReorg) for every tracked tx whose
	// inclusion block left the canonical chain.
	ObsChainReorged
	// ObsChainFinalized — the block containing the tx was finalized.
	ObsChainFinalized
)

// String returns a short, stable lowercase label.
func (k ObsKind) String() string {
	switch k {
	case ObsAnnouncedInbound:
		return "announced-inbound"
	case ObsAnnouncedOutbound:
		return "announced-outbound"
	case ObsRequestedOutbound:
		return "requested-outbound"
	case ObsRequestedInbound:
		return "requested-inbound"
	case ObsDeliveredInbound:
		return "delivered-inbound"
	case ObsDeliveredOutbound:
		return "delivered-outbound"
	case ObsPoolAccepted:
		return "pool-accepted"
	case ObsPoolRejected:
		return "pool-rejected"
	case ObsPoolEvicted:
		return "pool-evicted"
	case ObsChainIncluded:
		return "chain-included"
	case ObsChainReorged:
		return "chain-reorged"
	case ObsChainFinalized:
		return "chain-finalized"
	default:
		return "unknown"
	}
}

// Observation is a Level 1 event: a raw per-tx fact the node observed.
// Every wire, pool, or chain action that mentions a tx produces exactly
// one Observation. Optional fields are populated only for the kinds
// where they apply (Peer on per-peer events, BlockNum/BlockHash on
// chain events, Reason on reject/evict). TxType is populated when the
// tracker has it on record for this tx.
type Observation struct {
	TxHash    common.Hash `json:"txHash"`
	Timestamp time.Time   `json:"timestamp"`
	Kind      ObsKind     `json:"kind"`
	Peer      string      `json:"peer,omitempty"`
	BlockNum  uint64      `json:"blockNum,omitempty"`
	BlockHash common.Hash `json:"blockHash,omitempty"`
	Reason    string      `json:"reason,omitempty"`
	TxType    uint8       `json:"txType,omitempty"`
}

// StateChange is a Level 2 event: a derived transition of a tx's
// TxStatus state machine. The tracker's state machine consumes
// Observations and emits exactly one StateChange per forward
// transition. Trigger records which ObsKind caused the transition;
// Peer/BlockNum/BlockHash/Reason are forwarded from the triggering
// Observation where applicable.
//
// CycleIdx and CycleBitmap together let a subscriber (e.g. the
// mempool-lens SPA) tolerate dropped notifications without losing
// per-cycle state coverage for a tx. CycleIdx mirrors TxInfo.Returns:
// 0 for the initial lifecycle, +1 each time the tx re-enters via an
// active observation on a terminal-state tx or a chain reorg.
// CycleBitmap has bit (NewStatus-1) set for each TxStatus the tx
// has visited in the current cycle, including the new one. So a
// drop in the middle of a cycle is recovered by the next surviving
// event: its bitmap OR-d into the subscriber's accumulated bitmap
// re-asserts whatever state visits were missed. Cycle boundaries
// (Returns bump) reset the bitmap to just the new status, so the
// per-cycle scope stays crisp.
type StateChange struct {
	TxHash      common.Hash `json:"txHash"`
	OldStatus   TxStatus    `json:"oldStatus"`
	NewStatus   TxStatus    `json:"newStatus"`
	Timestamp   time.Time   `json:"timestamp"`
	Trigger     ObsKind     `json:"trigger"`
	Peer        string      `json:"peer,omitempty"`
	BlockNum    uint64      `json:"blockNum,omitempty"`
	BlockHash   common.Hash `json:"blockHash,omitempty"`
	Reason      string      `json:"reason,omitempty"`
	TxType      uint8       `json:"txType,omitempty"`
	CycleIdx    uint8       `json:"cycleIdx"`
	CycleBitmap uint8       `json:"cycleBitmap"`
}

// TxInfo is the hot per-transaction state the tracker holds alongside
// status. Exposed via GetTx so a UI or RPC can enrich its own cached
// view on demand (e.g. after reconnecting and re-subscribing).
//
// Per-stage timestamps (Requested/Received/Pooled/Included/Finalized/
// Dropped) are populated by the state machine the first time the tx
// reaches each status. They remain at their zero value until that
// transition happens, so a UI rendering a lamp strip can detect
// "stage not reached yet" by checking IsZero(). FirstSeen is the time
// the tracker first observed the hash; LastChange is the timestamp of
// the most recent forward transition.
type TxInfo struct {
	Hash       common.Hash `json:"hash"`
	Status     TxStatus    `json:"status"`
	FirstSeen  time.Time   `json:"firstSeen"`
	LastChange time.Time   `json:"lastChange"`
	Requested  time.Time   `json:"requested,omitempty"`
	Received   time.Time   `json:"received,omitempty"`
	Pooled     time.Time   `json:"pooled,omitempty"`
	Included   time.Time   `json:"included,omitempty"`
	Finalized  time.Time   `json:"finalized,omitempty"`
	Dropped    time.Time   `json:"dropped,omitempty"`
	Announcers []string    `json:"announcers,omitempty"` // peers that announced this hash (order: first observed)
	Deliverer  string      `json:"deliverer,omitempty"`  // peer whose body was accepted by the pool
	// IncludedDeliverer is the value of Deliverer captured at the
	// moment this tx first reached chain inclusion. It is frozen on
	// first set and used as the attribution source for finalization
	// credit, so a peer that flips Deliverer post-inclusion (e.g.
	// after the original entry's TxInfo was FIFO-evicted and
	// re-created via a buggy re-acceptance of an already-mined tx)
	// cannot steal the finalization-credit slot.
	IncludedDeliverer string      `json:"includedDeliverer,omitempty"`
	BlockNum          uint64      `json:"blockNum,omitempty"`
	BlockHash         common.Hash `json:"blockHash,omitempty"`
	RejectErr         string      `json:"rejectErr,omitempty"`
	DropReason        string      `json:"dropReason,omitempty"`
	Local             bool        `json:"local,omitempty"`
	// Returns counts the times the tx genuinely re-entered the
	// lifecycle. A "real" re-entry is one driven by an active local
	// step (we sent GetPooledTransactions, our pool re-accepted) or
	// a chain reorg that pulled the tx back into the pool. Passive
	// peer signals (announce, body push) on a terminal-state tx do
	// NOT count — they're tracked via the dedicated counters below.
	// Equivalent to RequestedAfterTerminal + PooledAfterTerminal +
	// ReorgRestarts.
	Returns uint8 `json:"returns,omitempty"`
	// CycleBitmap has bit (status-1) set for each TxStatus the tx has
	// visited in the *current* cycle (i.e. since the last Returns bump
	// or since first observation if Returns is still zero). Exposed so
	// a subscriber that reconnects mid-flight can reconstruct the
	// lamp-strip coverage for the current cycle from a single GetTx
	// call, without needing the StateChange history.
	CycleBitmap uint8 `json:"cycleBitmap,omitempty"`
	// Post-terminal activity counters. Each counts one kind of
	// observation observed AFTER a tx reached Rejected or Dropped,
	// regardless of whether the observation drove a state transition.
	// Saturating-add at 255: an extreme spammer counts as ≥ 255 rather
	// than wrapping. Useful for "this tx is being announced 100× by
	// a single peer after we dropped it" diagnostics without polluting
	// the active-cycle bookkeeping.
	AnnouncedAfterTerminal uint8           `json:"announcedAfterTerminal,omitempty"`
	ReceivedAfterTerminal  uint8           `json:"receivedAfterTerminal,omitempty"`
	RequestedAfterTerminal uint8           `json:"requestedAfterTerminal,omitempty"`
	PooledAfterTerminal    uint8           `json:"pooledAfterTerminal,omitempty"`
	ReorgRestarts          uint8           `json:"reorgRestarts,omitempty"`
	TxType                 uint8           `json:"txType,omitempty"`
	TxSize                 uint32          `json:"txSize,omitempty"`
	From                   common.Address  `json:"from,omitempty"`
	Nonce                  uint64          `json:"nonce,omitempty"`
	Gas                    uint64          `json:"gas,omitempty"`
	GasFeeCap              *hexutil.Big    `json:"gasFeeCap,omitempty"`
	GasTipCap              *hexutil.Big    `json:"gasTipCap,omitempty"`
	Value                  *hexutil.Big    `json:"value,omitempty"`
	To                     *common.Address `json:"to,omitempty"`
}
