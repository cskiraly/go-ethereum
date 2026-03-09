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

// Package txtracker tracks the lifecycle of transactions from first announcement
// through pool acceptance, chain inclusion, and finalization.
package txtracker

import (
	"container/list"
	"encoding/json"
	"math/big"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
)

// TxStatus represents the lifecycle state of a tracked transaction.
type TxStatus uint8

const (
	TxAnnounced TxStatus = iota + 1 // Hash announced by peer, body not yet received
	TxRequested                      // Body requested from peer, awaiting response
	TxReceived                       // Full body received from peer
	TxPooled                         // Accepted into local transaction pool
	TxIncluded                       // Mined into a canonical block
	TxFinalized                      // Containing block is finalized
	TxRejected                       // Pool rejected the transaction
)

// String returns a human-readable name for the status.
func (s TxStatus) String() string {
	switch s {
	case TxAnnounced:
		return "announced"
	case TxRequested:
		return "requested"
	case TxReceived:
		return "received"
	case TxPooled:
		return "pooled"
	case TxIncluded:
		return "included"
	case TxFinalized:
		return "finalized"
	case TxRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// MarshalJSON serializes TxStatus as a JSON string (e.g., "pooled").
func (s TxStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// TxTrackerEvent is emitted whenever a tracked transaction changes state.
type TxTrackerEvent struct {
	TxHash    common.Hash    `json:"txHash"`
	OldStatus TxStatus       `json:"oldStatus"`
	NewStatus TxStatus       `json:"newStatus"`
	Timestamp mclock.AbsTime `json:"timestamp"`
	Peer      string         `json:"peer,omitempty"`
	BlockNum  uint64         `json:"blockNum,omitempty"`
	BlockHash common.Hash    `json:"blockHash,omitempty"`
	RejectErr string         `json:"rejectErr,omitempty"`
	Local     bool           `json:"local,omitempty"`
}

const (
	// defaultMaxEntries is the default maximum number of tracked transactions.
	defaultMaxEntries = 65536

	// Channel sizes for the event loop.
	announceChanSize      = 1024
	fetchRequestChanSize = 256
	receiveChanSize      = 256
	pooledChanSize   = 256
	rejectedChanSize = 256
	peerDropChanSize = 64
	queryChanSize    = 64
	chainEventSize   = 10
	newTxsEventSize  = 128
)

// txRecord is the internal per-transaction lifecycle record.
type txRecord struct {
	status TxStatus
	local  bool   // true if originated from local RPC, not a peer
	txType uint8  // Consensus tx type from announcement metadata
	txSize uint32 // Size from announcement metadata

	// Transaction metadata (populated when full body is received).
	from      common.Address
	nonce     uint64
	gas       uint64
	gasFeeCap *big.Int
	gasTipCap *big.Int
	value     *big.Int
	to        *common.Address

	firstSeen mclock.AbsTime // When first announced or received
	requested mclock.AbsTime // When body was requested from a peer
	received  mclock.AbsTime // When full body arrived
	pooled    mclock.AbsTime // When accepted into pool
	included  mclock.AbsTime // When included in canonical block
	finalized mclock.AbsTime // When block was finalized

	announcers    []string    // Peers that announced (ordered, first = earliest)
	requestedFrom string      // Peer we requested the body from
	deliverer     string      // Peer that delivered the full transaction
	blockNum   uint64      // Block number (when included)
	blockHash  common.Hash // Block hash (when included)
	rejectErr  string      // Rejection reason (when status == TxRejected)

	evictElem *list.Element // Position in eviction list
}

// peerStats tracks per-peer transaction contribution statistics.
type peerStats struct {
	announced      int64 // Total tx hashes announced by this peer
	delivered      int64 // Total tx bodies delivered by this peer
	usefulDelivery int64 // Deliveries that were accepted into pool
	firstAnnouncer int64 // Times this peer was the first to announce a tx
}

// TxInfo is the public, read-only view of a transaction's tracked state.
type TxInfo struct {
	Status     TxStatus
	Local      bool
	TxType     uint8
	TxSize     uint32
	From       common.Address
	Nonce      uint64
	Gas        uint64
	GasFeeCap  *big.Int
	GasTipCap  *big.Int
	Value      *big.Int
	To         *common.Address
	FirstSeen     mclock.AbsTime
	Requested     mclock.AbsTime
	Received      mclock.AbsTime
	Pooled        mclock.AbsTime
	Included      mclock.AbsTime
	Finalized     mclock.AbsTime
	Announcers    []string
	RequestedFrom string
	Deliverer     string
	BlockNum   uint64
	BlockHash  common.Hash
	RejectErr  string
}

// PeerStats is the public view of a peer's transaction contribution.
type PeerStats struct {
	Announced      int64
	Delivered      int64
	UsefulDelivery int64
	FirstAnnouncer int64
}

// BlockchainReader abstracts the blockchain for chain event subscription and
// finalization queries.
type BlockchainReader interface {
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	CurrentFinalBlock() *types.Header
}

// TxPoolReader abstracts the transaction pool for new transaction event
// subscription.
type TxPoolReader interface {
	SubscribeTransactions(ch chan<- core.NewTxsEvent, reorgs bool) event.Subscription
}

// Internal event types for the single-goroutine event loop.
type announceEvent struct {
	peer   string
	hashes []common.Hash
	types  []byte
	sizes  []uint32
}

type fetchRequestedEvent struct {
	peer   string
	hashes []common.Hash
}

type receiveEvent struct {
	peer string
	txs  []*types.Transaction
}

type pooledEvent struct {
	hashes []common.Hash
}

type rejectedEvent struct {
	hashes []common.Hash
	errs   []error
}

type txQuery struct {
	hash common.Hash
	resp chan *TxInfo
}

type statusQuery struct {
	hash common.Hash
	resp chan TxStatus
}

type peerStatsQuery struct {
	peer string
	resp chan PeerStats
}

// Config holds the configuration for a Tracker.
type Config struct {
	MaxEntries int
	Clock      mclock.Clock
	Chain      BlockchainReader
	TxPool     TxPoolReader
}

// Tracker maintains per-transaction lifecycle records from first announcement
// through finalization. It runs a single-goroutine event loop, matching the
// pattern used by TxFetcher.
type Tracker struct {
	txs       map[common.Hash]*txRecord
	peers     map[string]*peerStats
	clock     mclock.Clock
	chain     BlockchainReader
	txpool    TxPoolReader

	maxEntries int
	evictList  *list.List // LRU ordered by last update (front = most recent)

	// Last known chain head for reorg detection.
	lastHeadHash common.Hash

	// Event channels (buffered; senders block when full or on quit).
	announceCh      chan *announceEvent
	fetchRequestCh  chan *fetchRequestedEvent
	receiveCh       chan *receiveEvent
	pooledCh    chan *pooledEvent
	rejectedCh  chan *rejectedEvent
	peerDropCh  chan string

	// Query channels (blocking request/response).
	queryCh     chan *txQuery
	statusCh    chan *statusQuery
	peerStatsCh chan *peerStatsQuery

	// Event feed for state transition notifications.
	eventFeed event.Feed

	quit  chan struct{}
	step  chan struct{} // Test synchronization: sent after each event is processed
	ready chan struct{} // Closed when event loop has subscribed to feeds
}

// New creates a new transaction lifecycle Tracker.
func New(config Config) *Tracker {
	maxEntries := config.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	clock := config.Clock
	if clock == nil {
		clock = mclock.System{}
	}
	return &Tracker{
		txs:         make(map[common.Hash]*txRecord),
		peers:       make(map[string]*peerStats),
		clock:       clock,
		chain:       config.Chain,
		txpool:      config.TxPool,
		maxEntries:  maxEntries,
		evictList:   list.New(),
		announceCh:     make(chan *announceEvent, announceChanSize),
		fetchRequestCh: make(chan *fetchRequestedEvent, fetchRequestChanSize),
		receiveCh:      make(chan *receiveEvent, receiveChanSize),
		pooledCh:    make(chan *pooledEvent, pooledChanSize),
		rejectedCh:  make(chan *rejectedEvent, rejectedChanSize),
		peerDropCh:  make(chan string, peerDropChanSize),
		queryCh:     make(chan *txQuery, queryChanSize),
		statusCh:    make(chan *statusQuery, queryChanSize),
		peerStatsCh: make(chan *peerStatsQuery, queryChanSize),
		quit:  make(chan struct{}),
		step:  make(chan struct{}, 1),
		ready: make(chan struct{}),
	}
}

// Start begins the tracker's event loop goroutine.
func (t *Tracker) Start() {
	go t.loop()
}

// Stop terminates the tracker's event loop.
func (t *Tracker) Stop() {
	close(t.quit)
}

// NotifyAnnounced records that a peer announced transaction hashes.
func (t *Tracker) NotifyAnnounced(peer string, hashes []common.Hash, types []byte, sizes []uint32) {
	select {
	case t.announceCh <- &announceEvent{peer: peer, hashes: hashes, types: types, sizes: sizes}:
	case <-t.quit:
	}
}

// NotifyFetchRequested records that transaction bodies were requested from a peer.
func (t *Tracker) NotifyFetchRequested(peer string, hashes []common.Hash) {
	select {
	case t.fetchRequestCh <- &fetchRequestedEvent{peer: peer, hashes: hashes}:
	case <-t.quit:
	}
}

// NotifyReceived records that transaction bodies were received from a peer.
func (t *Tracker) NotifyReceived(peer string, txs []*types.Transaction) {
	select {
	case t.receiveCh <- &receiveEvent{peer: peer, txs: txs}:
	case <-t.quit:
	}
}

// NotifyPooled records that transactions were accepted into the local pool.
func (t *Tracker) NotifyPooled(hashes []common.Hash) {
	select {
	case t.pooledCh <- &pooledEvent{hashes: hashes}:
	case <-t.quit:
	}
}

// NotifyRejected records that transactions were rejected by the local pool.
func (t *Tracker) NotifyRejected(hashes []common.Hash, errs []error) {
	select {
	case t.rejectedCh <- &rejectedEvent{hashes: hashes, errs: errs}:
	case <-t.quit:
	}
}

// NotifyPeerDrop records that a peer has disconnected.
func (t *Tracker) NotifyPeerDrop(peer string) {
	select {
	case t.peerDropCh <- peer:
	case <-t.quit:
	}
}

// Get returns the full lifecycle info for a tracked transaction, or nil if
// the transaction is not tracked. This method blocks until the event loop
// processes the query.
func (t *Tracker) Get(hash common.Hash) *TxInfo {
	resp := make(chan *TxInfo, 1)
	select {
	case t.queryCh <- &txQuery{hash: hash, resp: resp}:
		select {
		case info := <-resp:
			return info
		case <-t.quit:
			return nil
		}
	case <-t.quit:
		return nil
	}
}

// Status returns just the current status of a tracked transaction, or 0 if
// untracked.
func (t *Tracker) Status(hash common.Hash) TxStatus {
	resp := make(chan TxStatus, 1)
	select {
	case t.statusCh <- &statusQuery{hash: hash, resp: resp}:
		select {
		case s := <-resp:
			return s
		case <-t.quit:
			return 0
		}
	case <-t.quit:
		return 0
	}
}

// GetPeerStats returns the transaction contribution statistics for a peer.
func (t *Tracker) GetPeerStats(peer string) PeerStats {
	resp := make(chan PeerStats, 1)
	select {
	case t.peerStatsCh <- &peerStatsQuery{peer: peer, resp: resp}:
		select {
		case ps := <-resp:
			return ps
		case <-t.quit:
			return PeerStats{}
		}
	case <-t.quit:
		return PeerStats{}
	}
}

// SubscribeEvents creates a subscription for transaction state transition events.
func (t *Tracker) SubscribeEvents(ch chan<- TxTrackerEvent) event.Subscription {
	return t.eventFeed.Subscribe(ch)
}

// emitEvent sends a state transition event to all subscribers.
func (t *Tracker) emitEvent(hash common.Hash, oldStatus, newStatus TxStatus, rec *txRecord, peer string) {
	t.eventFeed.Send(TxTrackerEvent{
		TxHash:    hash,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Timestamp: t.clock.Now(),
		Peer:      peer,
		BlockNum:  rec.blockNum,
		BlockHash: rec.blockHash,
		RejectErr: rec.rejectErr,
		Local:     rec.local,
	})
}

// loop is the main event loop, processing all events sequentially to avoid locks.
func (t *Tracker) loop() {
	// Subscribe to chain events for inclusion/finalization tracking.
	var chainEventCh chan core.ChainEvent
	var chainSub event.Subscription
	if t.chain != nil {
		chainEventCh = make(chan core.ChainEvent, chainEventSize)
		chainSub = t.chain.SubscribeChainEvent(chainEventCh)
		defer chainSub.Unsubscribe()
	}

	// Subscribe to new transaction events for local tx detection.
	var newTxsCh chan core.NewTxsEvent
	var txsSub event.Subscription
	if t.txpool != nil {
		newTxsCh = make(chan core.NewTxsEvent, newTxsEventSize)
		txsSub = t.txpool.SubscribeTransactions(newTxsCh, false)
		defer txsSub.Unsubscribe()
	}

	// Signal that subscriptions are set up.
	close(t.ready)

	for {
		select {
		case ev := <-t.announceCh:
			t.handleAnnounce(ev)

		case ev := <-t.fetchRequestCh:
			t.handleFetchRequested(ev)

		case ev := <-t.receiveCh:
			t.handleReceive(ev)

		case ev := <-t.pooledCh:
			t.handlePooled(ev)

		case ev := <-t.rejectedCh:
			t.handleRejected(ev)

		case peer := <-t.peerDropCh:
			delete(t.peers, peer)

		case ev := <-chainEventCh:
			t.handleChainEvent(ev)

		case ev := <-newTxsCh:
			t.handleNewTxs(ev)

		case q := <-t.queryCh:
			q.resp <- t.answerQuery(q.hash)

		case q := <-t.statusCh:
			rec := t.txs[q.hash]
			if rec != nil {
				q.resp <- rec.status
			} else {
				q.resp <- 0
			}

		case q := <-t.peerStatsCh:
			ps := t.peers[q.peer]
			if ps != nil {
				q.resp <- PeerStats{
					Announced:      ps.announced,
					Delivered:      ps.delivered,
					UsefulDelivery: ps.usefulDelivery,
					FirstAnnouncer: ps.firstAnnouncer,
				}
			} else {
				q.resp <- PeerStats{}
			}

		case <-t.quit:
			return
		}
		// Signal test synchronization.
		select {
		case t.step <- struct{}{}:
		default:
		}
	}
}

// handleAnnounce processes a batch of transaction hash announcements from a peer.
func (t *Tracker) handleAnnounce(ev *announceEvent) {
	now := t.clock.Now()
	ps := t.getOrCreatePeer(ev.peer)

	for i, hash := range ev.hashes {
		ps.announced++
		txAnnouncedMeter.Mark(1)

		rec := t.txs[hash]
		if rec != nil {
			// Already tracked: just record additional announcer if not duplicate.
			if !slices.Contains(rec.announcers, ev.peer) {
				rec.announcers = append(rec.announcers, ev.peer)
			}
			t.touchLRU(rec)
			continue
		}
		// New transaction: create record.
		rec = &txRecord{
			status:     TxAnnounced,
			firstSeen:  now,
			announcers: []string{ev.peer},
		}
		if i < len(ev.types) {
			rec.txType = ev.types[i]
		}
		if i < len(ev.sizes) {
			rec.txSize = ev.sizes[i]
		}
		t.insertRecord(hash, rec)
		t.emitEvent(hash, 0, TxAnnounced, rec, ev.peer)
		txTrackedMeter.Mark(1)

		// This peer is the first announcer.
		ps.firstAnnouncer++
	}
}

// handleFetchRequested processes notification that transaction bodies were
// requested from a peer. Only advances Announced records to Requested.
func (t *Tracker) handleFetchRequested(ev *fetchRequestedEvent) {
	now := t.clock.Now()

	for _, hash := range ev.hashes {
		txFetchRequestedMeter.Mark(1)

		rec := t.txs[hash]
		if rec == nil {
			continue
		}
		// Only advance if currently Announced (not yet requested or received).
		if rec.status == TxAnnounced {
			rec.status = TxRequested
			rec.requested = now
			rec.requestedFrom = ev.peer
			t.touchLRU(rec)
			t.emitEvent(hash, TxAnnounced, TxRequested, rec, ev.peer)
		}
	}
}

// handleReceive processes notification that transaction bodies arrived from a peer.
func (t *Tracker) handleReceive(ev *receiveEvent) {
	now := t.clock.Now()
	ps := t.getOrCreatePeer(ev.peer)

	for _, tx := range ev.txs {
		hash := tx.Hash()
		ps.delivered++
		txReceivedMeter.Mark(1)

		rec := t.txs[hash]
		if rec == nil {
			// No prior announcement; create a record starting at Received.
			rec = &txRecord{
				status:    TxReceived,
				firstSeen: now,
				received:  now,
				deliverer: ev.peer,
			}
			fillTxMeta(rec, tx)
			t.insertRecord(hash, rec)
			t.emitEvent(hash, 0, TxReceived, rec, ev.peer)
			txTrackedMeter.Mark(1)
			continue
		}
		// Only advance if status is before Received (i.e., Announced).
		if rec.status < TxReceived {
			oldStatus := rec.status
			rec.status = TxReceived
			rec.received = now
			rec.deliverer = ev.peer
			fillTxMeta(rec, tx)
			t.touchLRU(rec)
			t.emitEvent(hash, oldStatus, TxReceived, rec, ev.peer)
		} else if rec.local && rec.deliverer == "" {
			// Repair race: handleNewTxs created this as local before
			// the receive event was processed.
			rec.local = false
			rec.deliverer = ev.peer
			fillTxMeta(rec, tx)
			t.touchLRU(rec)
		}
	}
}

// handlePooled processes notification that transactions were accepted into the pool.
func (t *Tracker) handlePooled(ev *pooledEvent) {
	now := t.clock.Now()

	for _, hash := range ev.hashes {
		txPooledMeter.Mark(1)

		rec := t.txs[hash]
		if rec == nil {
			// Pool accepted a tx we didn't see come in via P2P — should be rare
			// since the handler calls NotifyReceived first, but handle gracefully.
			rec = &txRecord{
				status:    TxPooled,
				firstSeen: now,
				pooled:    now,
			}
			t.insertRecord(hash, rec)
			t.emitEvent(hash, 0, TxPooled, rec, "")
			txTrackedMeter.Mark(1)
			continue
		}
		// Only advance if not already pooled or beyond.
		if rec.status < TxPooled {
			oldStatus := rec.status
			rec.status = TxPooled
			rec.pooled = now
			t.touchLRU(rec)
			t.emitEvent(hash, oldStatus, TxPooled, rec, "")

			// Update useful delivery stat for the deliverer.
			if rec.deliverer != "" {
				if ps := t.peers[rec.deliverer]; ps != nil {
					ps.usefulDelivery++
				}
			}
		}
	}
}

// handleRejected processes notification that transactions were rejected by the pool.
func (t *Tracker) handleRejected(ev *rejectedEvent) {
	now := t.clock.Now()

	for i, hash := range ev.hashes {
		txRejectedMeter.Mark(1)

		rec := t.txs[hash]
		if rec == nil {
			rec = &txRecord{
				status:    TxRejected,
				firstSeen: now,
			}
			if i < len(ev.errs) && ev.errs[i] != nil {
				rec.rejectErr = ev.errs[i].Error()
			}
			t.insertRecord(hash, rec)
			t.emitEvent(hash, 0, TxRejected, rec, "")
			txTrackedMeter.Mark(1)
			continue
		}
		// Only mark as rejected if not already pooled or included.
		if rec.status < TxPooled {
			oldStatus := rec.status
			rec.status = TxRejected
			if i < len(ev.errs) && ev.errs[i] != nil {
				rec.rejectErr = ev.errs[i].Error()
			}
			t.touchLRU(rec)
			t.emitEvent(hash, oldStatus, TxRejected, rec, "")
		}
	}
}

// handleChainEvent processes a new block event, marking included transactions
// and detecting reorgs.
func (t *Tracker) handleChainEvent(ev core.ChainEvent) {
	now := t.clock.Now()
	blockNum := ev.Header.Number.Uint64()
	blockHash := ev.Header.Hash()
	parentHash := ev.Header.ParentHash

	// Detect reorg: if the parent of this block doesn't match our last head.
	if t.lastHeadHash != (common.Hash{}) && parentHash != t.lastHeadHash {
		t.handleReorg(blockNum)
	}
	t.lastHeadHash = blockHash

	// Mark all transactions in the block as included.
	for _, tx := range ev.Transactions {
		hash := tx.Hash()

		rec := t.txs[hash]
		if rec == nil {
			// Transaction not previously tracked (e.g., from a synced block
			// or a peer path we don't observe). Create a record directly
			// at Included.
			rec = &txRecord{
				status:    TxIncluded,
				firstSeen: now,
				included:  now,
				blockNum:  blockNum,
				blockHash: blockHash,
			}
			fillTxMeta(rec, tx)
			t.insertRecord(hash, rec)
			t.emitEvent(hash, 0, TxIncluded, rec, "")
			txTrackedMeter.Mark(1)
			txIncludedMeter.Mark(1)
			txChainOnlyMeter.Mark(1)
			continue
		}
		if rec.status != TxFinalized {
			oldStatus := rec.status
			rec.status = TxIncluded
			rec.included = now
			rec.blockNum = blockNum
			rec.blockHash = blockHash
			fillTxMeta(rec, tx)
			t.touchLRU(rec)
			t.emitEvent(hash, oldStatus, TxIncluded, rec, "")
			txIncludedMeter.Mark(1)
			switch oldStatus {
			case TxAnnounced:
				txIncludedFromAnnouncedMeter.Mark(1)
			case TxReceived:
				txIncludedFromReceivedMeter.Mark(1)
			case TxPooled:
				txIncludedFromPooledMeter.Mark(1)
			case TxRejected:
				txIncludedFromRejectedMeter.Mark(1)
			}
		}
	}
	// Check finalization: advance any included transactions past the finalized block.
	t.checkFinalization()
}

// handleReorg reverts TxIncluded transactions back to TxPooled when a reorg
// removes their containing block.
func (t *Tracker) handleReorg(newBlockNum uint64) {
	// Any transaction included at or above newBlockNum may be reorged out.
	// We conservatively revert all TxIncluded transactions with blockNum >= newBlockNum.
	for hash, rec := range t.txs {
		if rec.status == TxIncluded && rec.blockNum >= newBlockNum {
			log.Debug("Transaction reorged", "tx", hash, "block", rec.blockNum)
			rec.status = TxPooled
			rec.included = 0
			rec.blockNum = 0
			rec.blockHash = common.Hash{}
			t.touchLRU(rec)
			t.emitEvent(hash, TxIncluded, TxPooled, rec, "")
			txReorgedMeter.Mark(1)
		}
	}
}

// checkFinalization advances TxIncluded transactions to TxFinalized when their
// block is finalized.
func (t *Tracker) checkFinalization() {
	if t.chain == nil {
		return
	}
	finalBlock := t.chain.CurrentFinalBlock()
	if finalBlock == nil {
		return
	}
	finalNum := finalBlock.Number.Uint64()

	for hash, rec := range t.txs {
		if rec.status == TxIncluded && rec.blockNum <= finalNum {
			rec.status = TxFinalized
			rec.finalized = t.clock.Now()
			t.touchLRU(rec)
			t.emitEvent(hash, TxIncluded, TxFinalized, rec, "")
			txFinalizedMeter.Mark(1)
		}
	}
}

// handleNewTxs processes NewTxsEvent to detect locally-submitted transactions.
// Any transaction appearing in NewTxsEvent that has no prior record is marked
// as local.
func (t *Tracker) handleNewTxs(ev core.NewTxsEvent) {
	now := t.clock.Now()

	for _, tx := range ev.Txs {
		hash := tx.Hash()
		if t.txs[hash] != nil {
			continue // Already tracked via P2P path.
		}
		// New transaction not seen via P2P — treat as locally submitted.
		rec := &txRecord{
			status:    TxPooled,
			local:     true,
			firstSeen: now,
			pooled:    now,
		}
		fillTxMeta(rec, tx)
		t.insertRecord(hash, rec)
		t.emitEvent(hash, 0, TxPooled, rec, "")
		txTrackedMeter.Mark(1)
		txLocalMeter.Mark(1)
	}
}

// answerQuery builds a TxInfo from an internal record.
func (t *Tracker) answerQuery(hash common.Hash) *TxInfo {
	rec := t.txs[hash]
	if rec == nil {
		return nil
	}
	info := &TxInfo{
		Status:        rec.status,
		Local:         rec.local,
		TxType:        rec.txType,
		TxSize:        rec.txSize,
		From:          rec.from,
		Nonce:         rec.nonce,
		Gas:           rec.gas,
		GasFeeCap:     rec.gasFeeCap,
		GasTipCap:     rec.gasTipCap,
		Value:         rec.value,
		To:            rec.to,
		FirstSeen:     rec.firstSeen,
		Requested:     rec.requested,
		Received:      rec.received,
		Pooled:        rec.pooled,
		Included:      rec.included,
		Finalized:     rec.finalized,
		RequestedFrom: rec.requestedFrom,
		Deliverer:     rec.deliverer,
		BlockNum:      rec.blockNum,
		BlockHash:     rec.blockHash,
		RejectErr:     rec.rejectErr,
	}
	// Copy announcers to avoid data races.
	if len(rec.announcers) > 0 {
		info.Announcers = make([]string, len(rec.announcers))
		copy(info.Announcers, rec.announcers)
	}
	return info
}

// insertRecord adds a new transaction record and manages eviction.
func (t *Tracker) insertRecord(hash common.Hash, rec *txRecord) {
	rec.evictElem = t.evictList.PushFront(hash)
	t.txs[hash] = rec
	txTrackerSize.Update(int64(len(t.txs)))

	// Evict oldest entries if over capacity.
	for len(t.txs) > t.maxEntries {
		t.evictOldest()
	}
}

// touchLRU moves a record to the front of the LRU list.
func (t *Tracker) touchLRU(rec *txRecord) {
	if rec.evictElem != nil {
		t.evictList.MoveToFront(rec.evictElem)
	}
}

// evictOldest removes the least-recently-updated transaction record.
func (t *Tracker) evictOldest() {
	back := t.evictList.Back()
	if back == nil {
		return
	}
	hash := t.evictList.Remove(back).(common.Hash)
	delete(t.txs, hash)
	txEvictedMeter.Mark(1)
	txTrackerSize.Update(int64(len(t.txs)))
}

// getOrCreatePeer returns the peerStats for a peer, creating it if needed.
// fillTxMeta populates transaction metadata fields on a record from a full
// transaction object. Also updates txType and txSize to accurate values.
func fillTxMeta(rec *txRecord, tx *types.Transaction) {
	rec.txType = tx.Type()
	rec.txSize = uint32(tx.Size())
	rec.nonce = tx.Nonce()
	rec.gas = tx.Gas()
	rec.gasFeeCap = tx.GasFeeCap()
	rec.gasTipCap = tx.GasTipCap()
	rec.value = tx.Value()
	rec.to = tx.To()
	if chainID := tx.ChainId(); chainID != nil && chainID.Sign() > 0 {
		if from, err := types.Sender(types.LatestSignerForChainID(chainID), tx); err == nil {
			rec.from = from
		}
	} else {
		if from, err := types.Sender(types.HomesteadSigner{}, tx); err == nil {
			rec.from = from
		}
	}
}

func (t *Tracker) getOrCreatePeer(peer string) *peerStats {
	ps := t.peers[peer]
	if ps == nil {
		ps = &peerStats{}
		t.peers[peer] = ps
	}
	return ps
}

