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

// Package txtracker tracks the hot lifecycle of transactions observed by the
// node and exposes a per-tx event stream.
//
// Two levels of events are offered:
//
//   - Observations (Level 1) — raw per-tx facts. Every wire-, pool-, or
//     chain-level thing that mentions a tx hash produces exactly one
//     Observation. Multiple observations of the same kind can fire for a
//     single tx (e.g. 5 inbound announcements). Subscribe via
//     SubscribeObservations.
//
//   - StateChanges (Level 2) — derived transitions of a tx's TxStatus
//     state machine. At most one StateChange per forward transition;
//     deduplicated. Subscribe via SubscribeStateChanges.
//
// The tracker's internal state machine consumes observations and produces
// state changes; both are emitted on separate feeds. A hot per-tx snapshot
// (TxInfo) is queryable via GetTx.
//
// Public API (producers):
//   - NotifyAnnounced(peer, hashes, types, sizes) — peer advertised to us
//   - NotifyAnnouncedOutbound(peer, hashes)   — we advertised to peer
//   - NotifyFetchRequested(peer, hashes)      — we sent GetPooledTransactions
//   - NotifyRequestedInbound(peer, hashes)    — we served a peer request
//   - NotifyReceived(peer, hashes)            — body arrived (any source)
//   - NotifyDeliveredOutbound(peer, hashes)   — we sent a body to peer
//   - NotifyAccepted(peer, hashes)            — pool accepted
//   - NotifyRejected(peer, hash, err)         — pool rejected on submit
//   - NotifyDropped(hash, reason)             — pool evicted after accept
//
// Public API (consumers):
//   - SubscribeObservations(ch) event.Subscription
//   - SubscribeStateChanges(ch) event.Subscription
//   - GetTx(hash) *TxInfo
//   - ObsDropped(), StateDropped() uint64  (diagnostics)
//
// The tracker keeps state in memory only — no persistent storage.
package txtracker

import (
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
)

const (
	// Default maximum number of tx→TxInfo entries to retain. Tests can
	// override via Tracker.SetMaxTracked.
	defaultMaxTracked = 262144
	// emitBuffer is the capacity of each internal emit channel used to
	// decouple event emission from subscribers. Events are dropped (with
	// the per-feed dropped counter incremented) if subscribers can't
	// keep up.
	emitBuffer = 4096
)

// Chain is the blockchain interface needed by the tracker.
type Chain interface {
	SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription
	GetBlock(hash common.Hash, number uint64) *types.Block
	GetCanonicalHash(number uint64) common.Hash
	CurrentFinalBlock() *types.Header
}

// StatsConsumer receives per-peer signals from the tracker. NotifyBlock
// fires exactly once per handled chain head with per-peer inclusion /
// finalization deltas. The Notify{Announced,Delivered,Accepted,
// Rejected,Dropped} hooks fire as the tracker processes each
// observation kind that carries a peer attribution; consumers that
// only care about chain-side credits can stub them as no-ops. All
// hooks are invoked AFTER the tracker has released its lock so the
// consumer can take its own locks safely.
type StatsConsumer interface {
	NotifyBlock(inclusions, finalized map[string]int)
	NotifyAnnounced(peer string, count int)
	NotifyDelivered(peer string, count int)
	NotifyAccepted(peer string, count int)
	NotifyRejected(peer string)
	NotifyDropped(peer string)
	// NotifyBouncingBlocked fires when the tracker's bouncing
	// protection suppressed `count` peer-attributed events — either
	// announces we didn't fetch (IsBouncing-true path) or pushed
	// bodies we intercepted before pool.Add. The lens Peers tab
	// surfaces it as a "this peer is hammering us" column.
	NotifyBouncingBlocked(peer string, count int)
}

// Tracker records per-tx lifecycle state, emits observations and state
// changes on two distinct feeds, and drives per-block peer-credit signals
// to a StatsConsumer.
type Tracker struct {
	mu         sync.Mutex
	txs        map[common.Hash]*TxInfo // current per-tx hot state
	order      []common.Hash           // insertion order for FIFO eviction
	maxTracked int                     // FIFO cap; defaults to defaultMaxTracked

	chain        Chain
	consumer     StatsConsumer
	lastFinalNum uint64      // last finalized block number processed
	lastHeadHash common.Hash // last chain-head hash processed (for reorg detection)
	lastHeadNum  uint64      // height of lastHeadHash
	headCh       chan core.ChainHeadEvent
	sub          event.Subscription

	// Level 1 — raw observations
	obsFeed    event.Feed
	obsCh      chan Observation
	obsDropped uint64

	// Level 2 — derived state transitions
	stateFeed    event.Feed
	stateCh      chan StateChange
	stateDropped uint64

	// FIFO-eviction histogram: index = TxStatus (uint8). Incremented
	// when an entry leaves t.txs because the hot map exceeds
	// maxTracked. Bucket-0 (StatusUnknown) is reserved for
	// completeness — a tracked entry should never be at Unknown.
	evictedByStatus [9]uint64

	// bouncing holds hashes the local pool just evicted for an
	// overflow-style reason ("rate limited", "capacity", "replaced").
	// IsBouncing consults this map on the fetcher's announce hot
	// path. See bouncing.go.
	bouncing sync.Map // map[common.Hash]bouncingEntry

	// bouncingCount mirrors len(bouncing) as an atomic counter so the
	// chain-head loop can cheaply skip recoverSender on untracked
	// txs when there are no bouncing entries to clear (the dominant
	// case during catch-up after a restart). Maintained in lockstep
	// with bouncingSizeGauge in markBouncing / deleteBouncing.
	bouncingCount atomic.Int64

	// bouncingStats accumulates cumulative-since-startup aggregates
	// over the bouncing map: inserts per source/reason, clears per
	// path, sums of per-entry counters for departed entries, and
	// histograms over lifetime and announces-blocked. Surfaced via
	// txtracker_bouncingStats. Allocated by New(); never reset.
	bouncingStats *bouncingStatsAcc

	// blockClass accumulates cumulative-since-startup per-bucket
	// classifications of chain-included txs. Atomic counters; read
	// without holding t.mu. See block_class.go.
	blockClass *blockClassAcc

	// peerCoverage, when non-nil, is invoked once per chain-included tx
	// to ask the eth peerset how many peers know the hash. Wired by the
	// eth backend via SetPeerCoverage so the tracker package stays free
	// of eth-protocol imports. Lock-free read; the tracker treats nil as
	// "Source B disabled".
	peerCoverage PeerCoverageFunc

	// pooledBySender counts currently-StatusPooled txs per sender, kept
	// in sync by updatePooledCount on every transition + by evict on
	// FIFO removal. Used at drop time to decide whether to apply the
	// single-tx TTL on a new bouncing entry. Accessed under t.mu.
	pooledBySender map[common.Address]int

	// poolFloor, if set, gates bouncing-entry clearance on whether the
	// tx fee would still pass admission (avoids refetches that would
	// just rebound as underpriced). Set via SetPoolFloor before
	// Start; read from the chain-head goroutine without locking.
	poolFloor PoolFloor

	// Runtime A/B toggles for the bouncing protection layers. All
	// default to true; flipping any to false causes that layer to
	// short-circuit while the corresponding bouncing/whatif/* meter
	// records the would-have-fired event. Settable via the txtracker
	// JSON-RPC namespace (BouncingFlags / SetBouncingFlag); read on
	// hot paths without locking.
	bouncingEnabled        atomic.Bool // master read toggle (IsBouncing)
	bouncingDropEnabled    atomic.Bool // post-acceptance insert (NotifyDropped)
	bouncingRejectEnabled  atomic.Bool // submission-time insert (NotifyRejected)
	bouncingFeeGateEnabled atomic.Bool // stage-2 fee gate in clear sweep

	emitQuit chan struct{} // signals both emit loops to stop

	// capture is an optional NDJSON sink that mirrors every
	// Observation and StateChange to disk. Set via SetCapturePath
	// before Start; nil otherwise. Used by the lens's "capture
	// report" flow as the server-side ground truth.
	capture *captureSink

	quit     chan struct{}
	stopOnce sync.Once     // makes Stop idempotent and safe before Start
	step     chan struct{} // test sync: sent after each chain-head event is processed
	wg       sync.WaitGroup
}

// New creates a new tracker.
func New() *Tracker {
	t := &Tracker{
		txs:            make(map[common.Hash]*TxInfo),
		maxTracked:     defaultMaxTracked,
		obsCh:          make(chan Observation, emitBuffer),
		stateCh:        make(chan StateChange, emitBuffer),
		emitQuit:       make(chan struct{}),
		quit:           make(chan struct{}),
		step:           make(chan struct{}, 1),
		pooledBySender: make(map[common.Address]int),
		bouncingStats:  newBouncingStatsAcc(),
		blockClass:     newBlockClassAcc(),
	}
	// Bouncing toggles default to ON so the protection is active from
	// startup; the operator flips one OFF via the txtracker RPC to A/B
	// the layer at runtime.
	t.bouncingEnabled.Store(true)
	t.bouncingDropEnabled.Store(true)
	t.bouncingRejectEnabled.Store(true)
	t.bouncingFeeGateEnabled.Store(true)
	return t
}

// SetPeerCoverage wires the eth-peerset "does any peer know this hash"
// callback used by the Source B block-classification metrics. Pass nil
// to disable. Must be called BEFORE Start so the block-import loop
// sees a stable reference without a write lock.
func (t *Tracker) SetPeerCoverage(f PeerCoverageFunc) {
	t.peerCoverage = f
}

// SetCapturePath enables NDJSON capture of every Observation and
// StateChange to the given file or directory. Must be called BEFORE
// Start so the sink can subscribe before the emit loops fire.
// Empty path is a no-op (no capture). On error the tracker still
// runs; capture is left disabled.
func (t *Tracker) SetCapturePath(path string) error {
	if path == "" {
		return nil
	}
	sink, err := newCaptureSink(path)
	if err != nil {
		return err
	}
	t.capture = sink
	return nil
}

// CaptureInfo returns a snapshot of the capture sink state, or
// {Enabled: false} when no capture path has been configured.
func (t *Tracker) CaptureInfo() CaptureInfo {
	if t.capture == nil {
		return CaptureInfo{Enabled: false}
	}
	return t.capture.info()
}

// setMaxTracked overrides the FIFO cap; intended for tests that need
// to exercise the eviction path without pushing the full default cap
// through. Must be called before any Notify* path runs concurrently.
func (t *Tracker) setMaxTracked(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n < 1 {
		n = 1
	}
	t.maxTracked = n
}

// SubscribeObservations returns a subscription that delivers every raw
// Observation (Level 1) the tracker emits.
func (t *Tracker) SubscribeObservations(ch chan<- Observation) event.Subscription {
	return t.obsFeed.Subscribe(ch)
}

// SubscribeStateChanges returns a subscription that delivers every Level 2
// StateChange (state-machine transition) the tracker emits.
func (t *Tracker) SubscribeStateChanges(ch chan<- StateChange) event.Subscription {
	return t.stateFeed.Subscribe(ch)
}

// ObsDropped returns the number of Observations dropped due to a full
// emit buffer.
func (t *Tracker) ObsDropped() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.obsDropped
}

// StateDropped returns the number of StateChanges dropped due to a full
// emit buffer.
func (t *Tracker) StateDropped() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stateDropped
}

// EvictedByStatus returns a copy of the FIFO-eviction histogram keyed
// by TxStatus. Each bucket is the cumulative number of tracked txs
// that left the hot map at that status because the map exceeded
// maxTracked. The slot for StatusUnknown is reserved and should
// always be zero in practice.
func (t *Tracker) EvictedByStatus() [9]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.evictedByStatus
}

// Start begins listening for chain head events and starts the event
// forwarders. `consumer` receives per-block peer-credit signals; if nil,
// signals are computed but discarded.
func (t *Tracker) Start(chain Chain, consumer StatsConsumer) {
	t.chain = chain
	t.consumer = consumer
	// Seed lastFinalNum so collectFinalization doesn't backfill from genesis.
	if fh := chain.CurrentFinalBlock(); fh != nil {
		t.lastFinalNum = fh.Number.Uint64()
	}
	// Buffer chain-head events generously: event.Feed.Send blocks
	// when a subscriber's channel is full, which would back-pressure
	// into the chain's block-insertion path. handleChainHead is fast
	// post-A (collectFinalization no longer walks blocks) but the
	// larger buffer adds defence in depth against a stall during
	// catch-up bursts after a long downtime.
	t.headCh = make(chan core.ChainHeadEvent, 4096)
	t.sub = chain.SubscribeChainHeadEvent(t.headCh)
	t.wg.Add(3)
	go t.loop()
	go t.obsEmitLoop()
	go t.stateEmitLoop()
	if t.capture != nil {
		t.capture.start(t)
	}
}

// Stop shuts down the tracker.
func (t *Tracker) Stop() {
	t.stopOnce.Do(func() {
		if t.capture != nil {
			t.capture.stop()
		}
		// sub is nil if Stop races ahead of Start (or Start was never
		// called); guard so teardown on an error path can't panic.
		if t.sub != nil {
			t.sub.Unsubscribe()
		}
		close(t.quit)
		close(t.emitQuit)
	})
	t.wg.Wait()
}

// GetTx returns a snapshot of the tracked state for a tx, or nil if it is
// not currently tracked (never observed, or evicted). Safe to call from
// any goroutine.
func (t *Tracker) GetTx(hash common.Hash) *TxInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	ti, ok := t.txs[hash]
	if !ok {
		return nil
	}
	out := *ti
	if len(ti.Announcers) > 0 {
		out.Announcers = append([]string(nil), ti.Announcers...)
	}
	if ti.GasFeeCap != nil {
		out.GasFeeCap = (*hexutil.Big)(new(big.Int).Set((*big.Int)(ti.GasFeeCap)))
	}
	if ti.GasTipCap != nil {
		out.GasTipCap = (*hexutil.Big)(new(big.Int).Set((*big.Int)(ti.GasTipCap)))
	}
	if ti.Value != nil {
		out.Value = (*hexutil.Big)(new(big.Int).Set((*big.Int)(ti.Value)))
	}
	if ti.To != nil {
		to := *ti.To
		out.To = &to
	}
	// Populate the Bouncing flag at read time from the sync.Map rather
	// than mirroring it on the TxInfo struct itself — the bouncing
	// entry's lifecycle (insert / TTL / sender-inclusion / FIFO clear)
	// is independent of t.mu, so a stored boolean would drift. Cheap:
	// one sync.Map.Load per GetTx.
	if _, ok := t.bouncing.Load(hash); ok {
		out.Bouncing = true
	}
	return &out
}

//
// Producer methods — each records an atomic fact (Observation) and
// optionally drives a state-machine transition (StateChange).
//

// NotifyAnnounced records that a peer advertised each of the hashes to us
// (inbound). Every call produces an ObsAnnouncedInbound observation for
// each hash, even when the hash has been announced before. The first
// inbound announcement per hash transitions the tx to StatusAnnounced.
// Safe to call from any goroutine.
//
// types and sizes are parallel slices to hashes carrying the eth/68
// NewPooledTransactionHashes metadata. When their lengths match
// hashes the tracker stashes TxType and TxSize on the TxInfo for each
// hash that doesn't already have them, so consumers see the type
// before any body arrives. Pass nil/nil to skip metadata population
// (legacy callers, tests).
func (t *Tracker) NotifyAnnounced(peer string, hashes []common.Hash, types []byte, sizes []uint32) {
	t.notifyAnnouncedHashes(peer, hashes, types, sizes)
}

// NotifyAnnouncedOutbound records that we advertised each of the hashes to
// a peer. Emits ObsAnnouncedOutbound; never drives a state transition. Not yet wired
// into the eth handler — reserved for outbound-observation wiring.
func (t *Tracker) NotifyAnnouncedOutbound(peer string, hashes []common.Hash) {
	t.notifyPeerHashes(peer, hashes, ObsAnnouncedOutbound)
}

// NotifyFetchRequested records that we sent a GetPooledTransactions to the
// peer for these hashes. Drives the transition into StatusRequested the
// first time we request a given hash.
func (t *Tracker) NotifyFetchRequested(peer string, hashes []common.Hash) {
	t.notifyPeerHashes(peer, hashes, ObsRequestedOutbound)
}

// NotifyRequestedInbound records that a peer asked us for these hashes
// (we then serve them). Pure observation; no state transition. Not yet wired
// into the eth handler — reserved for outbound-observation wiring.
func (t *Tracker) NotifyRequestedInbound(peer string, hashes []common.Hash) {
	t.notifyPeerHashes(peer, hashes, ObsRequestedInbound)
}

// NotifyReceived records that we received tx bodies from a peer
// (direct reply, broadcast, or cache hit). Does not imply pool
// acceptance. Body-derived fields on TxInfo (TxType, TxSize, From,
// Nonce, Gas, GasFeeCap, GasTipCap, Value, To) are populated on first
// receipt; subsequent calls leave them as-is.
func (t *Tracker) NotifyReceived(peer string, txs []*types.Transaction) {
	if len(txs) == 0 {
		return
	}
	now := time.Now()

	observations := make([]Observation, 0, len(txs))
	var changes []StateChange

	t.mu.Lock()
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		hash := tx.Hash()
		ti := t.ensureInfo(hash, now)
		fillTxBody(ti, tx)
		t.applyPerEventBookkeeping(ti, ObsDeliveredInbound, peer)
		obs := t.buildObservation(ti, ObsDeliveredInbound, now, peer, 0, common.Hash{}, "")
		observations = append(observations, obs)
		if ch, ok := t.maybeTransition(ti, obs, now, peer); ok {
			changes = append(changes, ch)
		}
	}
	t.evict()
	obsDrops := t.obsDropped
	stateDrops := t.stateDropped
	t.mu.Unlock()

	t.emitObservations(observations, &obsDrops)
	t.emitStateChanges(changes, &stateDrops)

	t.mu.Lock()
	t.obsDropped = obsDrops
	t.stateDropped = stateDrops
	t.mu.Unlock()

	// Per-peer delivered counter. Counts non-nil txs; matches the
	// observation count emitted on the feed.
	if t.consumer != nil {
		count := 0
		for _, tx := range txs {
			if tx != nil {
				count++
			}
		}
		t.consumer.NotifyDelivered(peer, count)
	}
}

// NotifyDeliveredOutbound records that we sent a tx body to a peer.
// Pure observation; no state transition. Not yet wired
// into the eth handler — reserved for outbound-observation wiring.
func (t *Tracker) NotifyDeliveredOutbound(peer string, hashes []common.Hash) {
	t.notifyPeerHashes(peer, hashes, ObsDeliveredOutbound)
}

// NotifyLocalSubmitted records that the local node accepted a tx into
// the pool from a non-peer ingress (e.g. eth_sendTransaction /
// eth_sendRawTransaction). Body-derived fields on TxInfo are populated
// from the supplied tx, ti.Local is set to true, and the state machine
// is driven straight to StatusPooled via an ObsPoolAccepted observation
// with peer="" — Deliverer is NOT credited (the tx originated locally,
// not from a peer).
//
// This complements the peer-driven NotifyAccepted path: for txs that
// flow through fetcher.deliverTransactions the fetcher's onAccepted
// callback fires NotifyAccepted with the originating peer. Local
// submissions bypass the fetcher entirely (internal/ethapi → txpool.Add)
// so this method is the only way the tracker observes them.
func (t *Tracker) NotifyLocalSubmitted(txs []*types.Transaction) {
	if len(txs) == 0 {
		return
	}
	now := time.Now()

	observations := make([]Observation, 0, len(txs))
	var changes []StateChange

	t.mu.Lock()
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		hash := tx.Hash()
		ti := t.ensureInfo(hash, now)
		fillTxBody(ti, tx)
		ti.Local = true
		obs := t.buildObservation(ti, ObsPoolAccepted, now, "", 0, common.Hash{}, "")
		observations = append(observations, obs)
		if ch, ok := t.maybeTransition(ti, obs, now, ""); ok {
			changes = append(changes, ch)
		}
	}
	t.evict()
	obsDrops := t.obsDropped
	stateDrops := t.stateDropped
	t.mu.Unlock()

	t.emitObservations(observations, &obsDrops)
	t.emitStateChanges(changes, &stateDrops)

	t.mu.Lock()
	t.obsDropped = obsDrops
	t.stateDropped = stateDrops
	t.mu.Unlock()
}

// NotifyAccepted records that the tx pool accepted each hash as delivered
// by peer. Transitions the tx to StatusPooled and attributes peer as the
// deliverer for per-peer credit on subsequent inclusions.
func (t *Tracker) NotifyAccepted(peer string, hashes []common.Hash) {
	t.notifyPeerHashes(peer, hashes, ObsPoolAccepted)
}

// NotifyRejected records that the tx pool rejected a single tx on
// submission (err as returned by pool.Add). Transitions to
// StatusRejected with the error text as the reason.
func (t *Tracker) NotifyRejected(peer string, hash common.Hash, err error) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	t.notifyHashReason(peer, hash, reason, ObsPoolRejected, bouncingRejectionProtected(err))
}

// NotifyDropped records that a previously-accepted tx was removed from
// the pool. Transitions to StatusDropped. No-op if the hash is unknown.
func (t *Tracker) NotifyDropped(hash common.Hash, reason core.RemovalReason) {
	t.notifyHashReason("", hash, reason.String(), ObsPoolEvicted, bouncingDropProtected(reason))
}

// notifyPeerHashes is the shared implementation for multi-hash,
// peer-driven observation kinds. It locks once for the whole batch,
// builds observations + possibly state changes under the lock, then
// emits them after unlocking so the event feeds can't stall the caller.
func (t *Tracker) notifyPeerHashes(peer string, hashes []common.Hash, kind ObsKind) {
	if len(hashes) == 0 {
		return
	}
	now := time.Now()

	observations := make([]Observation, 0, len(hashes))
	var changes []StateChange

	t.mu.Lock()
	for _, hash := range hashes {
		// ObsPoolEvicted with unknown hash is treated as no-op upstream,
		// but notifyPeerHashes isn't used for that path — kept defensive.
		ti := t.ensureInfo(hash, now)
		t.applyPerEventBookkeeping(ti, kind, peer)
		obs := t.buildObservation(ti, kind, now, peer, 0, common.Hash{}, "")
		observations = append(observations, obs)
		if ch, ok := t.maybeTransition(ti, obs, now, peer); ok {
			changes = append(changes, ch)
		}
	}
	t.evict()
	obsDrops := t.obsDropped
	stateDrops := t.stateDropped
	t.mu.Unlock()

	t.emitObservations(observations, &obsDrops)
	t.emitStateChanges(changes, &stateDrops)

	t.mu.Lock()
	t.obsDropped = obsDrops
	t.stateDropped = stateDrops
	t.mu.Unlock()

	// Per-peer cumulative counts for the peerstats consumer. Only
	// inbound-pool acceptance has a counter; the other kinds that
	// route through notifyPeerHashes (announce-outbound,
	// requested-outbound/inbound, delivered-outbound) don't credit
	// the per-peer signal counters this hook serves.
	if t.consumer != nil && kind == ObsPoolAccepted {
		t.consumer.NotifyAccepted(peer, len(hashes))
	}
}

// notifyAnnouncedHashes is the inbound-announcement variant of
// notifyPeerHashes. It runs the same locking + emit dance but also
// stashes per-hash type/size metadata on the TxInfo when supplied,
// so consumers see TxType on the very first ObsAnnouncedInbound.
//
// types and sizes are parallel slices to hashes; they are honoured
// only when both lengths match len(hashes). Any mismatch (including
// the legacy nil/nil case) skips metadata population entirely.
func (t *Tracker) notifyAnnouncedHashes(peer string, hashes []common.Hash, types []byte, sizes []uint32) {
	if len(hashes) == 0 {
		return
	}
	haveMeta := len(types) == len(hashes) && len(sizes) == len(hashes)
	now := time.Now()

	observations := make([]Observation, 0, len(hashes))
	var changes []StateChange

	t.mu.Lock()
	for i, hash := range hashes {
		ti := t.ensureInfo(hash, now)
		if haveMeta {
			applyAnnouncedMeta(ti, types[i], sizes[i])
		}
		t.applyPerEventBookkeeping(ti, ObsAnnouncedInbound, peer)
		obs := t.buildObservation(ti, ObsAnnouncedInbound, now, peer, 0, common.Hash{}, "")
		observations = append(observations, obs)
		if ch, ok := t.maybeTransition(ti, obs, now, peer); ok {
			changes = append(changes, ch)
		}
	}
	t.evict()
	obsDrops := t.obsDropped
	stateDrops := t.stateDropped
	t.mu.Unlock()

	t.emitObservations(observations, &obsDrops)
	t.emitStateChanges(changes, &stateDrops)

	t.mu.Lock()
	t.obsDropped = obsDrops
	t.stateDropped = stateDrops
	t.mu.Unlock()

	// Per-peer cumulative count for the peerstats consumer. Fires
	// once per inbound-announcement batch with the full observation
	// count (including duplicate hashes within the batch — that
	// matches the per-event semantics consumers used to see on the
	// observations subscription).
	if t.consumer != nil {
		t.consumer.NotifyAnnounced(peer, len(hashes))
	}
}

// notifyHashReason handles the single-hash, reason-carrying kinds
// (ObsPoolRejected, ObsPoolEvicted).
func (t *Tracker) notifyHashReason(peer string, hash common.Hash, reason string, kind ObsKind, bounceProtected bool) {
	now := time.Now()

	var (
		obs    Observation
		change StateChange
		hasObs bool
		hasCh  bool
	)

	// deliverer captured under the lock when the eviction is genuine,
	// so the post-unlock consumer hook can attribute the drop without
	// re-acquiring the tracker lock.
	var dropDeliverer string

	t.mu.Lock()
	if kind == ObsPoolEvicted {
		// Eviction of an unknown hash is a no-op — we have no record to
		// transition, and creating a fresh entry just to mark it Dropped
		// would be misleading.
		if _, ok := t.txs[hash]; !ok {
			t.mu.Unlock()
			return
		}
	}
	ti := t.ensureInfo(hash, now)
	// RejectErr is unconditional: a rejection is unconditional. DropReason
	// is gated on the eviction actually firing the StateChange — for an
	// inclusion-driven pool removal the state stays at Included and we
	// don't want a misleading "nonce expired" lingering on the TxInfo.
	if kind == ObsPoolRejected {
		ti.RejectErr = reason
	}
	obs = t.buildObservation(ti, kind, now, peer, 0, common.Hash{}, reason)
	hasObs = true
	if c, ok := t.maybeTransition(ti, obs, now, peer); ok {
		change = c
		hasCh = true
		if kind == ObsPoolEvicted && c.NewStatus == StatusDropped {
			ti.DropReason = reason
			dropDeliverer = ti.Deliverer
			if bounceProtected {
				t.markBouncing(hash, ti, now, bouncingFromDrop, reason)
			}
		}
	}
	// Submission-time rejections with bounce-loop reasons (gap,
	// capacity, auth conflict) get the same protection as
	// post-acceptance overflow drops. Done outside the maybeTransition
	// branch so a hash that's already StatusRejected gets re-marked
	// after a previous bouncing entry was cleared (e.g. by
	// sender-inclusion) and the fetcher tried again.
	if kind == ObsPoolRejected && bounceProtected {
		t.markBouncing(hash, ti, now, bouncingFromReject, reason)
	}
	t.evict()
	obsDrops := t.obsDropped
	stateDrops := t.stateDropped
	t.mu.Unlock()

	if hasObs {
		t.emitObs(obs, &obsDrops)
	}
	if hasCh {
		t.emitState(change, &stateDrops)
	}

	t.mu.Lock()
	t.obsDropped = obsDrops
	t.stateDropped = stateDrops
	t.mu.Unlock()

	// Per-peer counters for the peerstats consumer. Rejected uses
	// the directly-supplied peer (the one whose delivery the pool
	// rejected). Dropped is attributed to the original deliverer
	// captured under the lock; only credited when the transition
	// actually fired (an inclusion-driven removal leaves
	// dropDeliverer empty and the call is skipped).
	if t.consumer != nil {
		switch kind {
		case ObsPoolRejected:
			t.consumer.NotifyRejected(peer)
		case ObsPoolEvicted:
			if dropDeliverer != "" {
				t.consumer.NotifyDropped(dropDeliverer)
			}
		}
	}
}

//
// Internals — bookkeeping, state machine, emission.
//

// ensureInfo returns the TxInfo for hash, creating one if missing. Must
// be called with t.mu held.
func (t *Tracker) ensureInfo(hash common.Hash, now time.Time) *TxInfo {
	ti, ok := t.txs[hash]
	if ok {
		return ti
	}
	ti = &TxInfo{Hash: hash, FirstSeen: now}
	t.txs[hash] = ti
	t.order = append(t.order, hash)
	return ti
}

// fillTxBody populates body-derived fields on ti from a full tx if they
// are not already set. Idempotent — safe to call multiple times for the
// same tx; subsequent calls are no-ops once the body has been filled.
//
// Must be called with t.mu held. Sender recovery uses a Signer derived
// from tx.ChainId() (with a Homestead fallback for pre-EIP-155 txs);
// recovery failures leave From at its zero value rather than erroring.
//
// We use ti.Gas as the "body has been filled" sentinel because Gas is
// only ever written here and is always non-zero for a valid tx (EIP-150
// floor is 21 000). TxType and TxSize used to play that role but are
// now also populated by applyAnnouncedMeta from eth/68 announcements,
// which can fire before any body arrives — switching to Gas keeps the
// body-fill path running exactly once even after applyAnnouncedMeta
// has set TxType / TxSize.
func fillTxBody(ti *TxInfo, tx *types.Transaction) {
	if ti.Gas != 0 {
		return // body already filled
	}
	ti.TxType = tx.Type()
	ti.TxSize = uint32(tx.Size())
	ti.Nonce = tx.Nonce()
	ti.Gas = tx.Gas()
	ti.GasFeeCap = (*hexutil.Big)(tx.GasFeeCap())
	ti.GasTipCap = (*hexutil.Big)(tx.GasTipCap())
	ti.Value = (*hexutil.Big)(tx.Value())
	ti.To = tx.To()
	var signer types.Signer
	if chainID := tx.ChainId(); chainID != nil && chainID.Sign() > 0 {
		signer = types.LatestSignerForChainID(chainID)
	} else {
		signer = types.HomesteadSigner{}
	}
	if from, err := types.Sender(signer, tx); err == nil {
		ti.From = from
	}
}

// applyAnnouncedMeta sets TxType and TxSize on ti from eth/68 announcement
// metadata if they are not already set. Must be called with t.mu held.
//
// "Already set" is detected by ti.TxSize != 0; a real tx in flight on
// the wire is never zero-sized. Subsequent announcements with conflicting
// metadata are silently ignored — peers can lie and the first observation
// wins. When the tx body eventually arrives, fillTxBody overwrites both
// fields with the authoritative values from the body.
func applyAnnouncedMeta(ti *TxInfo, txType uint8, txSize uint32) {
	if ti.TxSize != 0 || txSize == 0 {
		return
	}
	ti.TxType = txType
	ti.TxSize = txSize
}

// applyPerEventBookkeeping applies TxInfo side-effects that are specific
// to the observation kind (e.g. recording an announcer, attributing a
// deliverer). Must be called with t.mu held.
func (t *Tracker) applyPerEventBookkeeping(ti *TxInfo, kind ObsKind, peer string) {
	switch kind {
	case ObsAnnouncedInbound:
		if peer != "" && !containsString(ti.Announcers, peer) {
			ti.Announcers = append(ti.Announcers, peer)
		}
	case ObsPoolAccepted:
		if ti.Deliverer == "" && peer != "" {
			ti.Deliverer = peer // first deliverer wins
		}
	}
}

// buildObservation assembles an Observation value from the inputs + any
// tx-level classification cached on TxInfo. Must be called with t.mu held.
func (t *Tracker) buildObservation(ti *TxInfo, kind ObsKind, ts time.Time, peer string, blockNum uint64, blockHash common.Hash, reason string) Observation {
	return Observation{
		TxHash:    ti.Hash,
		Timestamp: ts,
		Kind:      kind,
		Peer:      peer,
		BlockNum:  blockNum,
		BlockHash: blockHash,
		Reason:    reason,
		TxType:    ti.TxType,
	}
}

// maybeTransition runs the state machine on a just-built Observation
// against the current TxInfo. Returns (StateChange, true) if this
// observation caused a forward transition, else (_, false). Mutates
// ti.Status on transition. Must be called with t.mu held.
func (t *Tracker) maybeTransition(ti *TxInfo, obs Observation, now time.Time, peer string) (StateChange, bool) {
	old := ti.Status
	next := old
	// Snapshot Returns BEFORE the post-terminal bookkeeping so we can
	// detect a cycle bump and reset the bitmap to scope it per-cycle.
	oldReturns := ti.Returns

	// Post-terminal activity bookkeeping fires regardless of whether
	// the observation transitions the status. Passive observations
	// (announce, body-push) on a terminal-state tx no longer reset
	// status — they bump the dedicated counter only. Active observations
	// (we requested it, pool re-accepted) AND chain reorg DO transition
	// (see the cases below) and bump Returns + the per-kind counter.
	if old == StatusRejected || old == StatusDropped {
		switch obs.Kind {
		case ObsAnnouncedInbound:
			bumpU8(&ti.AnnouncedAfterTerminal)
		case ObsDeliveredInbound:
			bumpU8(&ti.ReceivedAfterTerminal)
		case ObsRequestedOutbound:
			bumpU8(&ti.RequestedAfterTerminal)
			bumpU8(&ti.Returns)
		case ObsPoolAccepted:
			bumpU8(&ti.PooledAfterTerminal)
			bumpU8(&ti.Returns)
		}
	} else if old == StatusIncluded && obs.Kind == ObsChainReorged {
		bumpU8(&ti.ReorgRestarts)
		bumpU8(&ti.Returns)
	}

	switch obs.Kind {
	case ObsAnnouncedInbound:
		if old == StatusUnknown {
			next = StatusAnnounced
		}
		// Passive announce on a terminal-state tx is counter-only;
		// status stays terminal. (Was previously: reset to Announced
		// + bump Returns. Removed because peer announces aren't an
		// "active restart" — see Returns-counter redefinition.)
	case ObsRequestedOutbound:
		if old <= StatusAnnounced || old == StatusRejected || old == StatusDropped {
			next = StatusRequested
		}
	case ObsDeliveredInbound:
		if old < StatusReceived {
			next = StatusReceived
		}
		// Passive body-push on a terminal-state tx is counter-only.
	case ObsPoolAccepted:
		if old < StatusPooled || old == StatusRejected || old == StatusDropped {
			next = StatusPooled
		}
	case ObsPoolRejected:
		next = StatusRejected
	case ObsPoolEvicted:
		// Inclusion-driven pool removal: when a tx is mined the pool
		// emits RemovedTxsEvent (with reason "nonce expired" from
		// demoteUnexecutables) and we receive it as ObsPoolEvicted.
		// By that point the chain-side observation has typically
		// transitioned the tx to StatusIncluded / StatusFinalized, so
		// we keep the chain status rather than overwriting with Dropped.
		// Hitting ObsPoolEvicted from earlier stages
		// (Announced/Requested/Received) means we missed the
		// Pool-acceptance signal entirely; fabricating a Dropped state
		// we never observed entering is worse than dropping the signal.
		// Mirrors the dormant tracker's status==TxPooled guard.
		if old == StatusPooled {
			next = StatusDropped
		}
	case ObsChainIncluded:
		if old != StatusIncluded && old != StatusFinalized {
			next = StatusIncluded
		}
	case ObsChainReorged:
		if old == StatusIncluded {
			next = StatusPooled
		}
	case ObsChainFinalized:
		if old != StatusFinalized {
			next = StatusFinalized
		}
	default:
		// ObsAnnouncedOutbound, ObsRequestedInbound, ObsDeliveredOutbound:
		// pure observations, no state change.
	}
	if next == old {
		return StateChange{}, false
	}
	ti.Status = next
	ti.LastChange = now
	t.updatePooledCount(ti.From, old, next)
	switch next {
	case StatusRequested:
		ti.Requested = now
	case StatusReceived:
		ti.Received = now
	case StatusPooled:
		// Re-entry from Included (reorg) preserves the original Pooled
		// timestamp so a UI can still see when the tx first hit the pool.
		if ti.Pooled.IsZero() {
			ti.Pooled = now
		}
	case StatusIncluded:
		ti.Included = now
	case StatusFinalized:
		ti.Finalized = now
	case StatusDropped:
		ti.Dropped = now
	}
	// Cycle bitmap maintenance. Bit (next-1) marks the just-entered
	// state. If Returns advanced during this transition (active re-entry
	// or chain reorg), this is a new cycle — reset the bitmap to just
	// the new state. Otherwise OR in the new bit, preserving prior
	// state visits in the current cycle. next is guaranteed >= 1 here
	// (StatusUnknown can never be a transition target, only a source on
	// the first transition), so next-1 never underflows.
	newBit := uint8(1) << uint8(next-1)
	if ti.Returns != oldReturns {
		ti.CycleBitmap = newBit
	} else {
		ti.CycleBitmap |= newBit
	}
	return StateChange{
		TxHash:      ti.Hash,
		OldStatus:   old,
		NewStatus:   next,
		Timestamp:   now,
		Trigger:     obs.Kind,
		Peer:        peer,
		BlockNum:    obs.BlockNum,
		BlockHash:   obs.BlockHash,
		Reason:      obs.Reason,
		TxType:      ti.TxType,
		CycleIdx:    ti.Returns,
		CycleBitmap: ti.CycleBitmap,
	}, true
}

// emitObs sends a single observation to the emit channel, counting the
// drop in *drops if the channel is full. Must NOT be called with t.mu
// held (writing to the buffered channel can block briefly, and we don't
// want to stall other producers).
func (t *Tracker) emitObs(obs Observation, drops *uint64) {
	select {
	case t.obsCh <- obs:
	default:
		*drops++
	}
}

// emitState sends a single state change, counting the drop in *drops if
// the channel is full.
func (t *Tracker) emitState(ch StateChange, drops *uint64) {
	select {
	case t.stateCh <- ch:
	default:
		*drops++
	}
}

// emitObservations drains a slice of observations to the emit channel.
func (t *Tracker) emitObservations(obs []Observation, drops *uint64) {
	for _, o := range obs {
		t.emitObs(o, drops)
	}
}

// emitStateChanges drains a slice of state changes to the emit channel.
func (t *Tracker) emitStateChanges(cs []StateChange, drops *uint64) {
	for _, c := range cs {
		t.emitState(c, drops)
	}
}

// evict trims t.txs / t.order down to t.maxTracked entries (FIFO).
// Must be called with t.mu held.
//
// For each evicted entry, t.evictedByStatus is incremented at the
// entry's last-known TxStatus index. Reading the histogram lets UIs
// surface "we are losing X% of pooled txs to FIFO churn" type alerts.
func (t *Tracker) evict() {
	for len(t.txs) > t.maxTracked {
		oldest := t.order[0]
		t.order = t.order[1:]
		if ti, ok := t.txs[oldest]; ok {
			if int(ti.Status) < len(t.evictedByStatus) {
				t.evictedByStatus[ti.Status]++
			}
			// Keep pooledBySender in sync when a still-Pooled entry
			// leaves the hot map: the count must reflect entries we
			// can still see.
			if ti.Status == StatusPooled && ti.From != (common.Address{}) {
				t.pooledBySender[ti.From]--
				if t.pooledBySender[ti.From] <= 0 {
					delete(t.pooledBySender, ti.From)
				}
			}
		}
		t.deleteBouncing(oldest, clearFIFO)
		delete(t.txs, oldest)
	}
	if cap(t.order) > 2*t.maxTracked {
		t.order = append([]common.Hash(nil), t.order...)
	}
}

// containsString reports whether v is present in s.
func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// loop reads from the chain head subscription and dispatches to the
// per-block handler.
func (t *Tracker) loop() {
	defer t.wg.Done()
	for {
		select {
		case ev := <-t.headCh:
			t.handleChainHead(ev)
			select {
			case t.step <- struct{}{}:
			default:
			}
		case <-t.sub.Err():
			return
		case <-t.quit:
			return
		}
	}
}

// obsEmitLoop drains the internal observation channel and forwards events
// to the public obs feed. Runs in its own goroutine so that Feed.Send
// (which may fan out to multiple subscribers) cannot block a tracker
// producer.
func (t *Tracker) obsEmitLoop() {
	defer t.wg.Done()
	for {
		select {
		case obs := <-t.obsCh:
			t.obsFeed.Send(obs)
		case <-t.emitQuit:
			return
		}
	}
}

// stateEmitLoop is the sibling of obsEmitLoop for the state-change feed.
func (t *Tracker) stateEmitLoop() {
	defer t.wg.Done()
	for {
		select {
		case ch := <-t.stateCh:
			t.stateFeed.Send(ch)
		case <-t.emitQuit:
			return
		}
	}
}

// maxReorgDepth caps how far back the reorg-detection walk goes when
// the chain-head's parent doesn't match the previously-known head.
// Reorgs deeper than this are very rare in practice (post-merge it
// takes a multi-slot CL fork) and walking further risks holding the
// tracker lock too long. Txs in blocks beyond this depth stay
// "Included" until they finalize or are evicted from the hot map.
const maxReorgDepth = 64

// handleChainHead computes per-peer deltas for the new head block and any
// newly-finalized blocks, emits Observations + StateChanges for matching
// tracked txs, and hands the per-peer credits to the StatsConsumer AFTER
// releasing t.mu.
//
// On a chain-head event whose ParentHash doesn't match the previously-
// known head hash, the tracker walks the orphaned side of the reorg
// (capped at maxReorgDepth) and emits ObsChainReorged for every
// tracked tx that was StatusIncluded in an orphaned block. The state
// machine drops those txs back to StatusPooled. Txs that subsequently
// reappear on the new chain re-enter StatusIncluded via the normal
// inclusion path.
func (t *Tracker) handleChainHead(ev core.ChainHeadEvent) {
	block := t.chain.GetBlock(ev.Header.Hash(), ev.Header.Number.Uint64())
	if block == nil {
		return
	}
	blockNum := block.NumberU64()
	blockHash := block.Hash()
	blockTime := block.Time()
	parentHash := ev.Header.ParentHash
	now := time.Now()

	var observations []Observation
	var changes []StateChange

	t.mu.Lock()
	// Reorg detection: if we have a previous head and the new block's
	// parent doesn't match it, walk the orphaned side and reorg the
	// matching txs. Only fires when there is actually a previously-known
	// head (skipped on the first chain-head event).
	if t.lastHeadHash != (common.Hash{}) && parentHash != t.lastHeadHash {
		reorgObs, reorgChanges := t.collectReorg(now, parentHash, blockNum)
		observations = append(observations, reorgObs...)
		changes = append(changes, reorgChanges...)
	}

	inclusions := make(map[string]int)
	// Senders of txs in this block — used after unlock to clear any
	// bouncing entries belonging to those senders. A landed tx frees
	// the sender's pool slot, so previously-rate-limited / capacity /
	// replaced hashes from the same sender deserve a fresh chance.
	// Populated for every block tx (tracked or not) so a tx that
	// reaches the chain without ever passing through our pool / fetcher
	// still counts as a sender-inclusion signal for the bouncing map.
	//
	// recoverSender on untracked block txs is gated on the bouncing
	// map being non-empty: if there are no bouncing entries to clear,
	// deriving the sender of every block tx is wasted ECDSA work.
	// Crucially this gate fires during catch-up after a node restart
	// (the bouncing map starts empty) where the chain emits a flood
	// of head events — without the gate, every block tx of every
	// catch-up block ate ~100µs of cache-cold ECDSA, blocking chain
	// insertion. Read once outside the loop so a steady-state
	// non-empty map doesn't pay an atomic-load per tx.
	includedSenders := make(map[common.Address]struct{})
	// Skip the per-tx loop entirely when there's nothing to update
	// (no tracked entries) and nothing to clear (no bouncing entries).
	// Common during catch-up after a fresh restart, where the chain
	// emits a flood of head events before the pool has produced any
	// NotifyAccepted calls. Without this guard we still pay
	// 150×tx.Hash + 150×map-miss per block (~1.5µs) — small per
	// block but unbounded across catch-up.
	gateRecoverSender := t.bouncingCount.Load() > 0
	// Gate block-classification metrics on the tracker having at least
	// one tracked entry. During cold-start catch-up before any pool
	// activity, every block tx would otherwise look "private" and swamp
	// the counters with no diagnostic value.
	classifyBlocks := len(t.txs) > 0
	if len(t.txs) > 0 || gateRecoverSender {
		for _, tx := range block.Transactions() {
			h := tx.Hash()
			ti, ok := t.txs[h]
			if classifyBlocks {
				var tiForClass *TxInfo
				if ok {
					tiForClass = ti
				}
				t.blockClass.record(classifyBlockInclusion(tiForClass))
				if t.peerCoverage != nil {
					total, knowing := t.peerCoverage(h)
					t.blockClass.recordPeerCoverage(total, knowing)
				}
			}
			if !ok {
				// Create a fresh TxInfo so the chain-inclusion transition
				// flows through the standard state machine and a 0→5
				// StateChange is emitted to subscribers. Without this,
				// hashes that first appear on chain (Flashbots / MEV-Boost
				// / direct-to-builder paths) are invisible to event
				// consumers — they were skipped entirely before. Pre-block
				// fields stay at their zero values; consumers detect the
				// chain-only origin by Status transitioning 0→5 (or
				// equivalently, by Pooled / Requested / Received all
				// being zero on the resulting TxInfo). fillTxBody below
				// populates body fields + From from the block tx, which
				// also feeds the bouncing-clear includedSenders set —
				// supersedes the previous gateRecoverSender shortcut.
				ti = t.ensureInfo(h, now)
			}
			// Body fields may still be empty if the tracker never observed
			// this tx via NotifyReceived (e.g. a tx that reached the pool
			// through a path that skipped the receive notification). The
			// block carries the body, so fill in here as a fallback.
			fillTxBody(ti, tx)
			if ti.From != (common.Address{}) {
				includedSenders[ti.From] = struct{}{}
			}
			// Pre-slot gate on peer credit: a tx whose body we accepted at
			// or after this block's slot time is almost certainly a
			// re-broadcast of an already-mined tx, not genuine relay work,
			// so its deliverer earns neither inclusion nor (via the frozen
			// IncludedDeliverer) finalization credit. The ObsChainIncluded
			// event below still fires so the lens feed sees the inclusion;
			// only the peerstats credit is suppressed. Mirrors the
			// pre-slot gate on peerdrop-latency's emitter tracker.
			//
			// A zero block time (genesis, or a mock block in tests) is
			// treated as "no slot known" and never gates — real mined
			// blocks always carry a non-zero timestamp.
			if ti.Deliverer != "" && (blockTime == 0 || ti.Pooled.IsZero() || uint64(ti.Pooled.Unix()) < blockTime) {
				inclusions[ti.Deliverer]++
				// Freeze the deliverer-of-record at first inclusion. Used
				// downstream by finalization-credit attribution
				// (collectFinalization) so a late mutation of ti.Deliverer
				// — e.g. via a buggy re-acceptance of an already-mined tx
				// after the original TxInfo was FIFO-evicted and recreated
				// — cannot steal credit from the original deliverer.
				if ti.IncludedDeliverer == "" {
					ti.IncludedDeliverer = ti.Deliverer
				}
			}
			obs := t.buildObservation(ti, ObsChainIncluded, now, "", blockNum, blockHash, "")
			observations = append(observations, obs)
			if c, ok := t.maybeTransition(ti, obs, now, ""); ok {
				ti.BlockNum = blockNum
				ti.BlockHash = blockHash
				changes = append(changes, c)
			}
		}
		// Chain-only TxInfos created above can push us over maxTracked
		// on busy blocks; trim once after the whole block has been
		// processed rather than per-tx to keep the FIFO order intact.
		t.evict()
	}

	t.lastHeadHash = blockHash
	t.lastHeadNum = blockNum

	finalized, finObs, finChanges := t.collectFinalization(now)
	observations = append(observations, finObs...)
	changes = append(changes, finChanges...)

	obsDrops := t.obsDropped
	stateDrops := t.stateDropped
	t.mu.Unlock()

	t.emitObservations(observations, &obsDrops)
	t.emitStateChanges(changes, &stateDrops)

	t.mu.Lock()
	t.obsDropped = obsDrops
	t.stateDropped = stateDrops
	t.mu.Unlock()

	baseFee := uint64(0)
	if ev.Header.BaseFee != nil && ev.Header.BaseFee.IsUint64() {
		baseFee = ev.Header.BaseFee.Uint64()
	}
	t.clearBouncingForSenders(includedSenders, baseFee)

	if t.consumer != nil {
		t.consumer.NotifyBlock(inclusions, finalized)
	}
}

// collectReorg walks the orphaned side of a reorg and emits an
// ObsChainReorged for every tracked tx in StatusIncluded whose block
// is being orphaned. The state machine drops those txs back to
// StatusPooled.
//
// The walk starts at t.lastHeadHash (the previously-known head) and
// follows ParentHash backwards until it meets the new chain's
// ancestry — reached when the walked block's hash matches an ancestor
// of newParent at the same height — or until maxReorgDepth blocks
// have been visited. Must be called with t.mu held.
//
// To detect "ancestor of newParent at this height", we walk the new
// chain back too, building a small set of new-chain ancestor hashes
// keyed by height. The first old-chain block whose hash appears in
// that set is the common ancestor; everything visited before it is
// orphaned.
func (t *Tracker) collectReorg(now time.Time, newParent common.Hash, newHeadNum uint64) ([]Observation, []StateChange) {
	if t.lastHeadHash == (common.Hash{}) {
		return nil, nil
	}
	var (
		observations []Observation
		changes      []StateChange
	)

	// Build the set of new-chain ancestor hashes back to the old head's
	// height (or maxReorgDepth, whichever is smaller). The new head's
	// parent itself is the most recent ancestor.
	newAncestors := make(map[common.Hash]struct{})
	cursor := newParent
	cursorNum := newHeadNum - 1
	for i := 0; i < maxReorgDepth && cursor != (common.Hash{}); i++ {
		newAncestors[cursor] = struct{}{}
		if cursorNum == 0 {
			break
		}
		nb := t.chain.GetBlock(cursor, cursorNum)
		if nb == nil {
			break
		}
		cursor = nb.ParentHash()
		cursorNum--
	}

	// Walk the old chain back; for each block not in newAncestors,
	// process its txs as orphaned.
	oldHash := t.lastHeadHash
	oldNum := t.lastHeadNum
	for i := 0; i < maxReorgDepth && oldHash != (common.Hash{}); i++ {
		if _, ok := newAncestors[oldHash]; ok {
			break // common ancestor — stop walking
		}
		oblock := t.chain.GetBlock(oldHash, oldNum)
		if oblock == nil {
			break // chain pruned the orphan; can't reorg-correct what we can't see
		}
		orphanHash := oblock.Hash()
		orphanNum := oblock.NumberU64()
		for _, tx := range oblock.Transactions() {
			h := tx.Hash()
			ti, ok := t.txs[h]
			if !ok {
				continue
			}
			obs := t.buildObservation(ti, ObsChainReorged, now, "", orphanNum, orphanHash, "")
			observations = append(observations, obs)
			if c, ok := t.maybeTransition(ti, obs, now, ""); ok {
				// Clear the included-block context; if the tx is re-included
				// on the new chain, the inclusion path will repopulate it.
				ti.BlockNum = 0
				ti.BlockHash = common.Hash{}
				changes = append(changes, c)
			}
		}
		oldHash = oblock.ParentHash()
		if oldNum == 0 {
			break
		}
		oldNum--
	}
	return observations, changes
}

// collectFinalization accumulates per-peer finalization credits for blocks
// newly finalized since lastFinalNum and returns the credits map alongside
// observations + state changes for each affected tracked tx. Must be
// called with t.mu held.
// The pivot here is to iterate t.txs (which already records BlockNum
// and BlockHash at inclusion time) instead of walking each newly-
// finalized block from disk. The walk-by-block design was the
// dominant cost during catch-up after a restart: blocks finalized
// 32+ blocks ago aren't in the chain's recent-block LRU, so each
// GetBlockByNumber paid full RLP decode and the per-tx loop hit
// cache-cold tx.Hash() and types.Sender() across freshly-decoded
// instances. With the inversion, the only chain query is one cheap
// canonical-hash lookup per unique BlockNum that has tracked entries
// — used to confirm the recorded inclusion is on the canonical chain
// (orphan-block references won't match).
func (t *Tracker) collectFinalization(now time.Time) (map[string]int, []Observation, []StateChange) {
	credits := make(map[string]int)
	var observations []Observation
	var changes []StateChange

	finalHeader := t.chain.CurrentFinalBlock()
	if finalHeader == nil {
		return credits, observations, changes
	}
	finalNum := finalHeader.Number.Uint64()
	if finalNum <= t.lastFinalNum {
		return credits, observations, changes
	}

	// Group Included tracked entries by their recorded BlockNum so the
	// canonical-hash lookup happens once per height, not once per tx.
	buckets := make(map[uint64][]*TxInfo)
	for _, ti := range t.txs {
		if ti.Status != StatusIncluded {
			continue
		}
		if ti.BlockNum <= t.lastFinalNum || ti.BlockNum > finalNum {
			continue
		}
		buckets[ti.BlockNum] = append(buckets[ti.BlockNum], ti)
	}

	for num, tis := range buckets {
		canonHash := t.chain.GetCanonicalHash(num)
		if canonHash == (common.Hash{}) {
			continue
		}
		for _, ti := range tis {
			// BlockHash was recorded when we transitioned to
			// StatusIncluded. If it now disagrees with the
			// canonical hash, the recorded inclusion is in an
			// orphaned block and the entry should NOT be marked
			// finalized — the normal head/reorg path will
			// re-resolve its status when it sees the new chain.
			if ti.BlockHash != canonHash {
				continue
			}
			// Read finalization credit from the deliverer that
			// was frozen at inclusion time (handleChainHead
			// per-block loop), not the live ti.Deliverer. The
			// live field can be flipped post-inclusion by an
			// eviction-then-replay path; the frozen field is the
			// one that earned the inclusion credit.
			if ti.IncludedDeliverer != "" {
				credits[ti.IncludedDeliverer]++
			}
			obs := t.buildObservation(ti, ObsChainFinalized, now, "", num, canonHash, "")
			observations = append(observations, obs)
			if c, ok := t.maybeTransition(ti, obs, now, ""); ok {
				changes = append(changes, c)
			}
		}
	}

	if total := sumCounts(credits); total > 0 {
		log.Trace("Accumulated finalization credits",
			"from", t.lastFinalNum+1, "to", finalNum, "txs", total)
	}
	t.lastFinalNum = finalNum
	return credits, observations, changes
}

func sumCounts(m map[string]int) int {
	var sum int
	for _, v := range m {
		sum += v
	}
	return sum
}

// bumpU8 saturating-adds 1 to *p. Used by the post-terminal activity
// counters on TxInfo so an extreme spammer (a peer announcing the same
// hash hundreds of times) caps at 255 rather than silently wrapping.
func bumpU8(p *uint8) {
	if *p < 255 {
		*p++
	}
}
