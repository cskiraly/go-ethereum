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
	"time"

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
	TxDropped                        // Evicted from pool after being accepted
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
	case TxDropped:
		return "dropped"
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
	RejectErr  string         `json:"rejectErr,omitempty"`
	DropReason string         `json:"dropReason,omitempty"`
	Local      bool           `json:"local,omitempty"`
	TxType    uint8          `json:"txType"`
}

const (
	// defaultMaxEntries is the default maximum number of tracked transactions.
	defaultMaxEntries = 262144

	// Channel sizes for the event loop.
	announceChanSize      = 1024
	fetchRequestChanSize = 256
	receiveChanSize      = 256
	pooledChanSize   = 256
	rejectedChanSize = 256
	peerDropChanSize = 64
	queryChanSize    = 64
	chainEventSize        = 10
	newTxsEventSize       = 128
	removedTxsEventSize   = 128
	finalizeCheckInterval = 5 * time.Second
	emitChanSize          = 4096 // buffered to avoid blocking the event loop
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

	firstSeen time.Time // When first announced or received
	requested time.Time // When body was requested from a peer
	received  time.Time // When full body arrived
	pooled    time.Time // When accepted into pool
	included  time.Time // When included in canonical block
	finalized time.Time // When block was finalized
	dropped   time.Time // When evicted from pool

	announcers    []string    // Peers that announced (ordered, first = earliest)
	requestedFrom string      // Peer we requested the body from
	deliverer     string      // Peer that delivered the full transaction
	blockNum   uint64      // Block number (when included)
	blockHash  common.Hash // Block hash (when included)
	rejectErr  string      // Rejection reason (when status == TxRejected)
	dropReason string      // Drop reason (when status == TxDropped)

	evictElem *list.Element // Position in eviction list
}

// peerStats tracks per-peer transaction contribution statistics.
type peerStats struct {
	announced      int64 // Total tx hashes announced by this peer
	delivered      int64 // Total tx bodies delivered by this peer
	usefulDelivery int64 // Deliveries that were accepted into pool
	firstAnnouncer int64 // Times this peer was the first to announce a tx
	included       int64 // Delivered txs that were included on chain
	finalized      int64 // Delivered txs that were finalized on chain
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
	FirstSeen     time.Time
	Requested     time.Time
	Received      time.Time
	Pooled        time.Time
	Included      time.Time
	Finalized     time.Time
	Dropped       time.Time
	Announcers    []string
	RequestedFrom string
	Deliverer     string
	BlockNum   uint64
	BlockHash  common.Hash
	RejectErr  string
	DropReason string
}

// PeerStats is the public view of a peer's transaction contribution.
type PeerStats struct {
	Announced      int64
	Delivered      int64
	UsefulDelivery int64
	FirstAnnouncer int64
	Included       int64
	Finalized      int64
}

// TrackerStats is the public snapshot of tracker-wide statistics.
type TrackerStats struct {
	Total    int            `json:"total"`    // Current number of tracked transactions
	Capacity int            `json:"capacity"` // Maximum tracker capacity
	Evicted  EvictionStats  `json:"evicted"`  // Cumulative per-state eviction counts
}

// EvictionStats counts how many records were LRU-evicted from each state.
type EvictionStats struct {
	Announced int64 `json:"announced"`
	Requested int64 `json:"requested"`
	Received  int64 `json:"received"`
	Pooled    int64 `json:"pooled"`
	Included  int64 `json:"included"`
	Finalized int64 `json:"finalized"`
	Rejected  int64 `json:"rejected"`
	Dropped   int64 `json:"dropped"`
	Total     int64 `json:"total"`
}

// BlockchainReader abstracts the blockchain for chain event subscription and
// finalization queries.
type BlockchainReader interface {
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	CurrentFinalBlock() *types.Header
}

// TxPoolReader abstracts the transaction pool for new transaction event
// subscription and removal event subscription.
type TxPoolReader interface {
	SubscribeTransactions(ch chan<- core.NewTxsEvent, reorgs bool) event.Subscription
	SubscribeRemovedTransactions(ch chan<- core.RemovedTxsEvent) event.Subscription
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
	evicted    EvictionStats // Cumulative per-state eviction counts

	// Last known chain head for reorg detection.
	lastHeadHash common.Hash

	// Last finalized block number used by checkFinalization, to avoid
	// redundant full scans when the finalized pointer hasn't advanced.
	lastFinalNum uint64

	// Event channels (buffered; senders block when full or on quit).
	announceCh      chan *announceEvent
	fetchRequestCh  chan *fetchRequestedEvent
	receiveCh       chan *receiveEvent
	pooledCh    chan *pooledEvent
	rejectedCh  chan *rejectedEvent
	peerDropCh  chan string

	// Query channels (blocking request/response).
	queryCh      chan *txQuery
	statusCh     chan *statusQuery
	peerStatsCh  chan *peerStatsQuery
	trackerStatsCh    chan chan TrackerStats
	allPeerStatsCh chan chan map[string]PeerStats

	// Event feed for state transition notifications.
	eventFeed event.Feed
	emitCh    chan TxTrackerEvent // buffered; drained by emitLoop

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
		txs:        make(map[common.Hash]*txRecord),
		peers:      make(map[string]*peerStats),
		clock:      clock,
		chain:      config.Chain,
		txpool:     config.TxPool,
		maxEntries: maxEntries,
		evictList:  list.New(),
		announceCh:     make(chan *announceEvent, announceChanSize),
		fetchRequestCh: make(chan *fetchRequestedEvent, fetchRequestChanSize),
		receiveCh:      make(chan *receiveEvent, receiveChanSize),
		pooledCh:    make(chan *pooledEvent, pooledChanSize),
		rejectedCh:  make(chan *rejectedEvent, rejectedChanSize),
		peerDropCh:  make(chan string, peerDropChanSize),
		queryCh:        make(chan *txQuery, queryChanSize),
		statusCh:       make(chan *statusQuery, queryChanSize),
		peerStatsCh:    make(chan *peerStatsQuery, queryChanSize),
		trackerStatsCh:    make(chan chan TrackerStats, queryChanSize),
		allPeerStatsCh: make(chan chan map[string]PeerStats, queryChanSize),
		emitCh: make(chan TxTrackerEvent, emitChanSize),
		quit:   make(chan struct{}),
		step:  make(chan struct{}, 1),
		ready: make(chan struct{}),
	}
}

// Start begins the tracker's event loop goroutine.
func (t *Tracker) Start() {
	go t.emitLoop()
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

// GetStats returns tracker-wide statistics including eviction counters.
func (t *Tracker) GetStats() TrackerStats {
	resp := make(chan TrackerStats, 1)
	select {
	case t.trackerStatsCh <- resp:
		select {
		case s := <-resp:
			return s
		case <-t.quit:
			return TrackerStats{}
		}
	case <-t.quit:
		return TrackerStats{}
	}
}

// GetAllPeerStats returns transaction contribution statistics for all peers.
func (t *Tracker) GetAllPeerStats() map[string]PeerStats {
	resp := make(chan map[string]PeerStats, 1)
	select {
	case t.allPeerStatsCh <- resp:
		select {
		case ps := <-resp:
			return ps
		case <-t.quit:
			return nil
		}
	case <-t.quit:
		return nil
	}
}

// SubscribeEvents creates a subscription for transaction state transition events.
func (t *Tracker) SubscribeEvents(ch chan<- TxTrackerEvent) event.Subscription {
	return t.eventFeed.Subscribe(ch)
}

// emitEvent queues a state transition event for delivery to subscribers.
// The send is non-blocking: if the emit buffer is full, the event is dropped
// to prevent slow subscribers from stalling the event loop.
func (t *Tracker) emitEvent(hash common.Hash, oldStatus, newStatus TxStatus, rec *txRecord, peer string) {
	ev := TxTrackerEvent{
		TxHash:     hash,
		OldStatus:  oldStatus,
		NewStatus:  newStatus,
		Timestamp:  t.clock.Now(),
		Peer:       peer,
		BlockNum:   rec.blockNum,
		BlockHash:  rec.blockHash,
		RejectErr:  rec.rejectErr,
		DropReason: rec.dropReason,
		Local:      rec.local,
		TxType:     rec.txType,
	}
	select {
	case t.emitCh <- ev:
	default:
		txEmitDroppedMeter.Mark(1)
	}
}

// emitLoop drains the emit channel and forwards events to the event.Feed.
// This runs in a separate goroutine so that event.Feed.Send (which blocks
// until all subscribers consume the value) cannot stall the main event loop.
func (t *Tracker) emitLoop() {
	for {
		select {
		case ev := <-t.emitCh:
			t.eventFeed.Send(ev)
		case <-t.quit:
			return
		}
	}
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

	// Subscribe to removed transaction events for pool drop detection.
	var removedTxsCh chan core.RemovedTxsEvent
	var removedSub event.Subscription
	if t.txpool != nil {
		removedTxsCh = make(chan core.RemovedTxsEvent, removedTxsEventSize)
		removedSub = t.txpool.SubscribeRemovedTransactions(removedTxsCh)
		defer removedSub.Unsubscribe()
	}

	// Periodic finalization check. SetFinalized is called by the Engine API
	// (ForkchoiceUpdated) independently of ChainEvent, so checking only on
	// ChainEvent can miss finalization advances. This timer ensures we
	// catch up promptly.
	var finalizeTicker *time.Ticker
	var finalizeTickerCh <-chan time.Time
	if t.chain != nil {
		finalizeTicker = time.NewTicker(finalizeCheckInterval)
		finalizeTickerCh = finalizeTicker.C
		defer finalizeTicker.Stop()
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

		case ev := <-removedTxsCh:
			t.handleRemovedTxs(ev)

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
					Included:       ps.included,
					Finalized:      ps.finalized,
				}
			} else {
				q.resp <- PeerStats{}
			}

		case resp := <-t.trackerStatsCh:
			resp <- TrackerStats{
				Total:    len(t.txs),
				Capacity: t.maxEntries,
				Evicted:  t.evicted,
			}

		case resp := <-t.allPeerStatsCh:
			result := make(map[string]PeerStats, len(t.peers))
			for peer, ps := range t.peers {
				result[peer] = PeerStats{
					Announced:      ps.announced,
					Delivered:      ps.delivered,
					UsefulDelivery: ps.usefulDelivery,
					FirstAnnouncer: ps.firstAnnouncer,
					Included:       ps.included,
					Finalized:      ps.finalized,
				}
			}
			resp <- result

		case <-finalizeTickerCh:
			t.checkFinalization()

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
	now := time.Now()
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
	now := time.Now()

	for _, hash := range ev.hashes {
		txFetchRequestedMeter.Mark(1)

		rec := t.txs[hash]
		if rec == nil {
			continue
		}
		// Advance if currently Announced or Dropped (re-fetch cycle).
		if rec.status == TxAnnounced || rec.status == TxDropped {
			oldStatus := rec.status
			rec.status = TxRequested
			rec.requested = now
			rec.requestedFrom = ev.peer
			t.touchLRU(rec)
			t.emitEvent(hash, oldStatus, TxRequested, rec, ev.peer)
		}
	}
}

// handleReceive processes notification that transaction bodies arrived from a peer.
func (t *Tracker) handleReceive(ev *receiveEvent) {
	now := time.Now()
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
		// Advance if status is before Received or Dropped (re-fetch).
		if rec.status < TxReceived || rec.status == TxDropped {
			oldStatus := rec.status
			rec.status = TxReceived
			rec.received = now
			rec.deliverer = ev.peer
			fillTxMeta(rec, tx)
			t.touchLRU(rec)
			t.emitEvent(hash, oldStatus, TxReceived, rec, ev.peer)
		} else if rec.received.IsZero() {
			// Record already advanced past Received (e.g., handleNewTxs
			// created it at TxPooled before this receive event was
			// processed). Backfill receive metadata and fix local flag.
			rec.received = now
			rec.deliverer = ev.peer
			rec.local = false
			fillTxMeta(rec, tx)
			t.touchLRU(rec)
		}
	}
}

// handlePooled processes notification that transactions were accepted into the pool.
func (t *Tracker) handlePooled(ev *pooledEvent) {
	now := time.Now()

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
		// Advance if not already pooled or beyond, or if re-entering from Dropped.
		if rec.status < TxPooled || rec.status == TxDropped {
			oldStatus := rec.status
			rec.status = TxPooled
			rec.pooled = now
			t.touchLRU(rec)
			t.emitEvent(hash, oldStatus, TxPooled, rec, "")

			if oldStatus == TxDropped {
				txReaddedMeter.Mark(1)
			}
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
	now := time.Now()

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
		// Mark as rejected if not yet pooled/included, or if dropped (re-submit rejected).
		if rec.status < TxPooled || rec.status == TxDropped {
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
	now := time.Now()
	blockNum := ev.Header.Number.Uint64()
	blockHash := ev.Header.Hash()
	parentHash := ev.Header.ParentHash

	// Detect reorg: if the parent of this block doesn't match our last head.
	if t.lastHeadHash != (common.Hash{}) && parentHash != t.lastHeadHash {
		t.handleReorg(blockNum)
	}
	t.lastHeadHash = blockHash

	// Finalize before inserting new transactions. The new block may advance
	// the finalized pointer, and inserting its transactions first could LRU-
	// evict the very TxIncluded records that are now finalizable.
	t.checkFinalization()

	// Mark all transactions in the block as included.
	for _, tx := range ev.Transactions {
		hash := tx.Hash()

		rec := t.txs[hash]
		if rec == nil && blockNum <= t.lastFinalNum {
			// Block is already finalized — no point tracking a transaction
			// we first see at this stage. It didn't come through P2P and
			// will never provide useful lifecycle data.
			continue
		}
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
			case TxDropped:
				txIncludedFromDroppedMeter.Mark(1)
			}
			// Credit the delivering peer for on-chain inclusion.
			if rec.deliverer != "" {
				if ps := t.peers[rec.deliverer]; ps != nil {
					ps.included++
				}
			}
		}
	}
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
			rec.included = time.Time{}
			rec.blockNum = 0
			rec.blockHash = common.Hash{}
			t.touchLRU(rec)
			t.emitEvent(hash, TxIncluded, TxPooled, rec, "")
			txReorgedMeter.Mark(1)
			// Reverse the inclusion credit for the delivering peer.
			if rec.deliverer != "" {
				if ps := t.peers[rec.deliverer]; ps != nil {
					ps.included--
				}
			}
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
		log.Trace("Finalization check: no finalized block available")
		return
	}
	finalNum := finalBlock.Number.Uint64()

	// Skip the full scan if the finalized block hasn't advanced.
	if finalNum <= t.lastFinalNum {
		return
	}
	log.Debug("Finalization advancing", "old", t.lastFinalNum, "new", finalNum)
	t.lastFinalNum = finalNum

	var count int
	for hash, rec := range t.txs {
		if rec.status == TxIncluded && rec.blockNum <= finalNum {
			rec.status = TxFinalized
			rec.finalized = time.Now()
			t.touchLRU(rec)
			t.emitEvent(hash, TxIncluded, TxFinalized, rec, "")
			txFinalizedMeter.Mark(1)
			count++
			// Credit the delivering peer for finalization.
			if rec.deliverer != "" {
				if ps := t.peers[rec.deliverer]; ps != nil {
					ps.finalized++
				}
			}
		}
	}
	if count > 0 {
		log.Debug("Finalized transactions", "count", count, "finalBlock", finalNum)
	}
}

// handleRemovedTxs processes a RemovedTxsEvent, marking pooled transactions
// as dropped. Transactions already at TxIncluded or TxFinalized are ignored
// since pool removal events also fire for transactions that get mined.
func (t *Tracker) handleRemovedTxs(ev core.RemovedTxsEvent) {
	now := time.Now()
	for i, hash := range ev.Hashes {
		rec := t.txs[hash]
		if rec == nil || rec.status != TxPooled {
			continue
		}
		var reason string
		if i < len(ev.Reasons) {
			reason = ev.Reasons[i]
		}
		rec.status = TxDropped
		rec.dropped = now
		rec.dropReason = reason
		t.touchLRU(rec)
		t.emitEvent(hash, TxPooled, TxDropped, rec, "")
		txDroppedMeter.Mark(1)
	}
}

// handleNewTxs processes NewTxsEvent to detect locally-submitted transactions.
// Any transaction appearing in NewTxsEvent that has no prior record is marked
// as local.
func (t *Tracker) handleNewTxs(ev core.NewTxsEvent) {
	now := time.Now()

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
		Dropped:       rec.dropped,
		RequestedFrom: rec.requestedFrom,
		Deliverer:     rec.deliverer,
		BlockNum:      rec.blockNum,
		BlockHash:     rec.blockHash,
		RejectErr:     rec.rejectErr,
		DropReason:    rec.dropReason,
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
	rec := t.txs[hash]
	delete(t.txs, hash)

	txEvictedMeter.Mark(1)
	t.evicted.Total++
	if rec != nil {
		switch rec.status {
		case TxAnnounced:
			txEvictedAnnouncedMeter.Mark(1)
			t.evicted.Announced++
		case TxRequested:
			txEvictedRequestedMeter.Mark(1)
			t.evicted.Requested++
		case TxReceived:
			txEvictedReceivedMeter.Mark(1)
			t.evicted.Received++
		case TxPooled:
			txEvictedPooledMeter.Mark(1)
			t.evicted.Pooled++
		case TxIncluded:
			txEvictedIncludedMeter.Mark(1)
			t.evicted.Included++
		case TxFinalized:
			txEvictedFinalizedMeter.Mark(1)
			t.evicted.Finalized++
		case TxRejected:
			txEvictedRejectedMeter.Mark(1)
			t.evicted.Rejected++
		case TxDropped:
			txEvictedDroppedMeter.Mark(1)
			t.evicted.Dropped++
		}
	}
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

