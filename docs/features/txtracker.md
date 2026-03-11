# Transaction Tracker

## Overview

The `eth/txtracker` package maintains a per-transaction lifecycle record from
first announcement through finalization. It provides a unified view that was
previously scattered across TxFetcher (fetching state), TxPool (pool status),
and the blockchain (inclusion/finalization).

## Lifecycle State Machine

```
P2P path:    Announced → Requested → Received → Pooled → Included → Finalized
                                        │          ↑ ↑       │
                                        │          │ └───────┘ (reorg)
                                        └──→ Rejected │
                                                      ↓
                                                   Dropped → {Requested, Received, Pooled, Included}

Local path:  (local submit) → Pooled → Included → Finalized
                                 │        ↑
                                 ↓        │
                              Dropped ────┘
```

## Architecture

### Event Loop

Single-goroutine event loop (same pattern as TxFetcher):
- All state lives in the loop goroutine, no locks needed
- Callers send events via buffered channels (non-blocking)
- Queries use request/response channels (blocking)
- Chain events and pool events come via standard subscriptions

### Key Design Decisions

1. **Minimal modifications to core/ packages**: The tracker subscribes to
   `ChainEvent`, `NewTxsEvent`, and `RemovedTxsEvent` via standard interfaces.
   `RemovedTxsEvent` was added to the `SubPool` interface for reactive pool
   drop detection. Local transactions are detected as txs appearing in
   `NewTxsEvent` without prior tracker record.

2. **LRU eviction**: Bounded to `maxEntries` (default 65536). Each status
   transition moves the entry to the front. Finalized and rejected txs age out
   naturally since they receive no further updates.

3. **Reorg handling**: When a new block's parent doesn't match the previous
   head, all `TxIncluded` transactions at or above the new block number revert
   to `TxPooled`.

4. **Peer statistics**: Maintained alongside tx records in the event loop.
   Tracks announcements, deliveries, useful deliveries, first-announcer
   counts, included (delivered txs that made it on chain), and finalized
   (delivered txs whose block was finalized) per peer. The included counter
   is incremented when a tracked tx with a deliverer transitions to
   `TxIncluded`, and decremented on reorg. The finalized counter is
   incremented in `checkFinalization`. Stats are cleared on peer disconnect.

5. **Finalized-block skip**: Transactions first seen in blocks at or below
   `lastFinalNum` are not tracked. During chain sync, `handleChainEvent` fires
   for every imported block — including thousands of already-finalized blocks.
   Creating records for their transactions would waste LRU capacity (evicting
   interesting P2P-observed records) and flood the event feed with noise.
   The guard (`rec == nil && blockNum <= t.lastFinalNum`) only skips **new**
   records; transactions already tracked via P2P are still updated to
   `TxIncluded` normally. Before the CL first reports a finalized block,
   `lastFinalNum` is 0, so no blocks are skipped.

6. **Finalization detection**: `checkFinalization()` scans all `TxIncluded`
   entries and advances those at or below `CurrentFinalBlock()` to
   `TxFinalized`. Runs both on `ChainEvent` and on a 5-second periodic
   timer, because `SetFinalized` (called by the Engine API's
   `ForkchoiceUpdated`) fires independently of and after `ChainEvent`.
   Caches `lastFinalNum` to skip redundant scans when the finalized pointer
   hasn't advanced.

### Files

- `eth/txtracker/tracker.go` — Core types, event loop, state machine
- `eth/txtracker/metrics.go` — Meter and gauge registrations
- `eth/txtracker/tracker_test.go` — Unit tests
- `eth/handler.go` — Creates, starts, stops tracker; hooks addTxs and peer drop
- `eth/handler_eth.go` — Feeds announcement and receive events to tracker
- `eth/api_txtracker.go` — RPC API (GetTx, GetPeerStats, Events subscription)

## Event Feed & RPC API

### Event Feed

Real-time event feed for state transitions:

- `TxTrackerEvent` struct emitted on every state change via `event.Feed`
- `SubscribeEvents(ch)` for external consumers
- `TxStatus.MarshalJSON()` serializes as string (`"pooled"` not `3`)
- `emitEvent` helper called from each handler after state change, captures
  old status before overwrite

### RPC API (`txtracker` namespace)

- `txtracker_getTx(hash)` — full lifecycle info
- `txtracker_getPeerStats(peer)` — peer contribution stats
- `txtracker_getAllPeerStats()` — contribution stats for all connected peers
- `txtracker_subscribe("events")` — WebSocket subscription for live events,
  follows the `NewHeads` pattern from `eth/filters/api.go`

## Transaction Retention & Eviction

Transactions are tracked in multiple components simultaneously, each with
different retention strategies. The components below are grouped by whether
they existed on master before this branch or were added as part of the
txtracker work.

### Pre-existing components (master)

**eth peer knownTxs** (`eth/protocols/eth/peer.go`) tracks which transaction
hashes a peer already knows about, to avoid redundant sends. Each peer has a
`knownCache` (`mapset.Set[common.Hash]`, capacity **32,768**). Hashes are
marked known when sending full txs, announcing hashes, replying to
`GetPooledTransactions`, or receiving announcements/broadcasts. Used by
`BroadcastTransactions()` to skip peers that already have a tx. Eviction is
random (`Pop()`) when over capacity — not LRU. Garbage collected with the
peer struct on disconnect.

**tx_fetcher** (`eth/fetcher/tx_fetcher.go`) tracks transactions through a
three-stage fetch pipeline (wait → announce → fetch). Per-transaction state
is ephemeral — cleaned up on delivery, timeout, or peer disconnect. Two LRU
caches provide longer-term memory:

| Cache | Capacity | TTL | Purpose |
|-------|----------|-----|---------|
| `underpriced` | 32,768 entries | 5 min | Avoid re-requesting recently rejected underpriced txs |
| `txOnChainCache` | 32,768 entries | none (purged on reorg) | Avoid re-fetching recently mined txs |

Fetch pipeline timers: `txArriveTimeout` = 500ms (wait before requesting),
`txFetchTimeout` = 5s (max time to wait for a response).

**legacypool** (`core/txpool/legacypool/`) has time-based and capacity-based
eviction:

- *Queued (non-executable) txs*: evicted after `Lifetime` (default **3 hours**),
  checked every minute. The heartbeat resets when new txs arrive for the account.
- *Capacity limits*: `GlobalSlots` = 5,120 executable, `GlobalQueue` = 1,024
  non-executable. When exceeded, lowest-priced txs are evicted.
- *Per-account limits*: `AccountSlots` = 16 executable, `AccountQueue` = 64
  non-executable.

**blobpool** (`core/txpool/blobpool/`) has price-based eviction via an eviction
heap, plus special handling for nonce-gapped transactions:

- *Gapped txs*: kept for `gappedLifetime` = 1 min, max 128 globally. This
  handles brief reordering; after that they're dropped to prevent DoS.
- *Oversaturated pool*: evicts cheapest transactions based on dynamic fee
  calculation (considers both base fee and blob fee).

### Txtracker

**txtracker** (`eth/txtracker/tracker.go`) uses LRU eviction with no
time-based expiry:

- Bounded to `maxEntries` (default **65,536**). Each status transition moves
  the entry to the LRU front. Finalized and rejected txs age out naturally
  since they receive no further updates.
- No time-based expiry — a transaction stays tracked as long as it keeps
  receiving status updates or hasn't been pushed out by newer entries.

Per-peer statistics (`peerStats`) are created lazily on first announcement
and **deleted entirely on peer disconnect** (`NotifyPeerDrop`). Fields
tracked per peer: `announced`, `delivered`, `usefulDelivery`,
`firstAnnouncer`. These are transient — no persistence across reconnects.

Per-transaction peer attribution survives peer disconnect (stored on the tx
record, not the peer record): `announcers []string`, `requestedFrom string`,
`deliverer string`. These are evicted with the tx via LRU.

**txview browser** (`cmd/txview/internal/ui/app.js`) has **no eviction**:

- The `txs` Map grows unbounded as subscription events arrive.
- `topCache` entries go stale after 5s and are refetched, but the main event
  map never shrinks.
- Page refresh is the only cleanup — clears all state and starts fresh.
- Long sessions will consume increasing browser memory.

### Summary

| Component | Strategy | Capacity | Time limit | Peer disconnect |
|-----------|----------|----------|------------|-----------------|
| eth peer knownTxs | random eviction | 32,768 per peer | none | GC with peer |
| tx_fetcher (underpriced) | LRU + TTL | 32,768 | 5 min | n/a |
| tx_fetcher (on-chain) | LRU | 32,768 | purge on reorg | n/a |
| tx_fetcher (pipeline) | explicit cleanup | unbounded | 500ms / 5s timeouts | full cleanup |
| legacypool (queued) | time-based | 1,024 global | 3 hours | n/a |
| legacypool (pending) | price-based | 5,120 global | none | n/a |
| blobpool (gapped) | time-based | 128 global | 1 min | n/a |
| blobpool (main) | price-based | blob space limits | none | n/a |
| **txtracker** | **LRU** | **65,536** | **none** | **stats deleted, tx records kept** |
| **txview browser** | **none** | **unbounded** | **page refresh** | **n/a** |

## txview — Web UI

Standalone web tool for visualizing transaction lifecycles.

### System Design

txview has a three-tier architecture: geth (data), txview binary (bridge),
and browser (UI).

**Geth (txtracker namespace)** owns all transaction lifecycle data. It
tracks every transaction from first announcement through finalization,
maintaining timestamps, peer attribution, and status for each. Three RPC
methods expose this:

- `txtracker_subscribe("events")` — streams lifecycle events (announced,
  requested, received, pooled, included, finalized, rejected) as they
  happen, each carrying txHash, newStatus, peer, blockNum, rejectErr, etc.
- `txtracker_getTx(hash)` — returns the full record: From, To, Nonce, Gas,
  fee caps, Value, type, size, all timestamps, Deliverer, Announcers,
  block info, and rejection error.
- `txtracker_getPeerStats(peer)` — per-peer contribution statistics.
- `txtracker_getAllPeerStats()` — bulk per-peer stats for the Peers pane.

**txview binary** (`cmd/txview/main.go`) is a stateless bridge. It does no
data processing or caching — it exists solely to let a browser talk to geth
without cross-origin issues:

1. *WebSocket reverse proxy* (`/ws`) — proxies browser connections to
   geth's WebSocket endpoint, stripping the Origin header to bypass geth's
   origin check.
2. *Config endpoint* (`/config`) — returns JSON with the proxied WebSocket
   URL so the JS knows where to connect.
3. *Static file server* (`/`) — serves the embedded UI assets (HTML, JS,
   CSS) from `embed.FS`.
4. *Detail page route* (`/tx/`) — serves `detail.html` for standalone
   per-transaction URLs.

**Browser** (`app.js`) maintains all UI state in memory, rebuilt from
scratch on each page load:

| Structure | Contents |
|---|---|
| `txs` (Map: hash → event) | Every subscription event, annotated with `_receivedAt`, `_firstStatus`, `_wasRequested` for Sankey path classification. `peer` and `_txType` are carried forward from earlier events so the deliverer peer remains visible across all lifecycle stages. |
| `topCache` (Map: hash → {info, fetchedAt}) | Cached `txtracker_getTx` results for visible rows, refetched when stale (>5s) |
| `topInflight` (Set) | Hashes currently being fetched to deduplicate RPC calls |
| `visibleHashes` / `topVisibleHashes` | Filtered+sorted hash arrays rebuilt on dirty render |
| `selectedHash` | Currently selected tx for the detail panel |
| `peersData` (Object: peer → PeerStats) | Cached `txtracker_getAllPeerStats` result, refetched every 3s when active |
| `peersSorted` (Array) | Sorted peer entries for virtual scroll rendering |
| `colWidths`, `feedColOrder`, `topColOrder`, `peersColOrder` | Column resize widths and drag-reorder state |
| Status counters | Incremented/decremented per event for the header badges |

The browser has no persistent storage. Refreshing the page resets all
state — only events arriving after the WebSocket connects are visible.

```
┌──────────┐  subscribe/getTx   ┌──────────────┐  /ws proxy   ┌─────────┐
│   geth   │◄──────────────────►│ txview binary │◄────────────►│ browser │
│ txtracker│  (WebSocket RPC)   │  (stateless)  │  (same-origin│  app.js │
└──────────┘                    └──────────────┘   WebSocket)  └─────────┘
                                  │ /config (JSON)      ▲
                                  │ / (static files)    │
                                  │ /tx/:hash (detail)  │
                                  └─────────────────────┘
                                        HTTP
```

### Features

- Dark-themed single-page app with no build tooling (embedded via `//go:embed`)
- **Feed view**: real-time scrollable event stream with virtual scrolling
- **Top view**: sortable table of all tracked txs with on-demand RPC fetch
- **Stats view**: Sankey diagram showing transaction flow through lifecycle stages,
  with now/total labels per node and backward links for reorgs
- **Peers view**: sortable table of per-peer contribution statistics (announced,
  delivered, useful delivery, first announcer) with derived percentage columns
- Filter by hash/peer/status/transaction type, resizable and reorderable columns
- Resizable detail panel with drag handle (200–800px)
- Detachable detail panel: pop out to `/tx/0x...` for side-by-side workflows
- LRU eviction stats panel below the Sankey diagram

#### Sankey Diagram

Each node shows two labels:
- **"X now"**: transactions currently at this state
- **"Y total"**: cumulative count of all txs that ever reached this state (hidden
  when equal to "now")

Forward flows (left to right) are filled bands connecting consecutive states.
Terminal states (Rejected, Dropped) branch downward from their source.

**Included → Finalized flow conservation**: The Finalized node and the band
from Included are sized to equal the Included inflow, not just the
already-finalized count. This reflects that finalization is guaranteed for
included transactions (barring reorgs). Without this, the Finalized node
would appear much smaller than Included because finalization happens in
batches every ~6.4 minutes while inclusion happens every ~12 seconds. A
"N pending" label below the Finalized node shows how many included
transactions are waiting for the next finalization batch.

**Backward links** visualize reorgs (Included → Pooled). These are drawn as
dashed amber arcs below the main flow, labeled with the reorg count. The
`_reorgCount` field on each tx event tracks how many times that transaction
was reverted from included back to the pool.

##### Exponential Smoothing

By default (slider at 0%), the Sankey diagram shows cumulative all-time counts
from page load. Over long sessions, early events dominate and the diagram stops
reflecting current behavior. A **Smoothing** slider (0–100%) in the Stats
filter bar controls exponential smoothing over time-bucketed snapshots.

**Snapshots**: Every 12 seconds (one Ethereum block), the current status
distribution is captured into a ring buffer capped at 7,200 entries (24 hours).
Each snapshot records the same data as the Sankey counting loop: per-status
counts, per-path breakdowns, reorg counts, reason maps, and cumulative totals.

**Smoothing formula**: Given snapshots S[0]…S[n] (oldest to newest) and
coefficient α = slider/100:
```
V[0] = S[0]
V[i] = α × S[i] + (1 − α) × V[i−1]
```
At α=0 (default): V[n] = S[0] ≈ cumulative totals (no smoothing).
At α=1: V[n] = S[n] = only the most recent 12-second window.

Smoothing applies to all numeric fields independently, including per-reason
breakdown maps. Fractional smoothed values work fine for band sizing; display
labels are rounded.

**Implementation**: `computeSankeySnapshot()` extracts the counting loop from
`renderStats()`. `smoothSnapshots()` iterates oldest→newest applying the blend.
`renderStats()` uses the smoothed snapshot when α > 0 and snapshots exist,
otherwise uses a live snapshot (current behavior).

#### Peers Pane

Sortable table showing per-peer transaction contribution statistics. Fetches
data via `txtracker_getAllPeerStats` (single RPC call returning all peers).
Columns: Peer ID, Announced, Delivered, Useful (deliveries accepted into pool),
1st Announce (times peer was first to announce), Included (delivered txs
included on chain), Finalized (delivered txs finalized on chain), Useful %
(UsefulDelivery / Delivered), 1st % (FirstAnnouncer / Announced), Included %
(Included / Delivered), Finalized % (Finalized / Delivered), Incl Share
(peer's included / all peers' included), Final Share (peer's finalized / all
peers' finalized). Auto-refreshes every 3 seconds
when the tab is active. Uses the same virtual scroll infrastructure as Feed
and Top panes.

### Usage

#### Building

```bash
go build -o txview ./cmd/txview
```

#### Running

Start geth with the txtracker API enabled over WebSocket:

```bash
geth --ws --ws.api txtracker
```

Then start txview pointing at geth's WebSocket endpoint:

```bash
./txview --rpc ws://localhost:8546 --addr localhost:8670
```

Open http://localhost:8670 in a browser.

**Flags:**

| Flag     | Default                | Description                    |
|----------|------------------------|--------------------------------|
| `--rpc`  | `ws://localhost:8546`  | Geth WebSocket RPC endpoint    |
| `--addr` | `localhost:8670`       | Local HTTP listen address      |

#### Remote access

txview binds to localhost by default. To view the UI from another machine,
use an SSH tunnel (recommended):

```bash
# From your local machine:
ssh -L 8670:localhost:8670 user@remote-host
# Then open http://localhost:8670 locally
```

Alternatively, bind to all interfaces with `--addr 0.0.0.0:8670`. In that
case geth also needs `--ws.addr 0.0.0.0` and appropriate firewall rules,
since the browser connects directly to geth's WebSocket.

## Changelog

### Code Review Fixes (commit 5)

- **BUG-1**: `txIncludedMeter` was firing for every transaction in every block,
  not just tracked ones. Moved inside the status-update block.
- **BUG-2**: `status < TxIncluded || status == TxIncluded` simplified to
  `status <= TxIncluded`.
- **BUG-3**: `TestPooledToIncluded` was a dead test (hash mismatch between
  `makeHash(5)` and `makeTx(hash).Hash()`). Removed along with unused `makeTx`.
- **IMPROVE-1**: Replaced `containsString` helper with `slices.Contains`.

### Codex Review Fixes (commit 6)

- **BUG-4**: `TxRejected` (iota 6) > `TxIncluded` (4) blocked rejected txs from
  advancing to Included on chain events. Changed guard to `status != TxFinalized`.
- **BUG-5**: `Get`/`Status`/`GetPeerStats` could deadlock if `Stop()` was called
  after the query was sent but before the loop processed it. Added quit select
  around response reads.
- **BUG-6**: Race between `handleNewTxs` and `handleReceive` could misclassify
  remote txs as local. `handleReceive` now repairs local flag when it finds a
  local record with no deliverer.
- **CLEANUP-1**: Removed dead `lastHeadNum` field.

### Receive Event Ordering Fix

`handleTransactions` calls `txpool.Add` which fires `NewTxsEvent`. The
tracker's `handleNewTxs` processes this event and creates a record at
`TxPooled` before `handleReceive` runs, because `NotifyReceived` was called
after `handleTransactions` in `handler_eth.go`. This caused the receive
event to be silently dropped (`rec.status >= TxReceived`), making the
"received" counter always show zero.

**Fix**: Call `NotifyReceived` before `handleTransactions` so the receive
event is queued ahead of the pool event. Also backfill receive metadata
(timestamp, deliverer, local flag) in `handleReceive` when the record has
already advanced past `TxReceived` but has no receive data (using
`rec.received.IsZero()` as the guard instead of the old `rec.local &&
rec.deliverer == ""` check).

### Finalization Timing Fix

`checkFinalization` only ran inside `handleChainEvent`, but `SetFinalized`
is called by the Engine API (`ForkchoiceUpdated`) after the `ChainEvent`
fires. This meant the tracker always read a stale finalized pointer. Under
heavy announce load, the event loop could fall further behind, delaying
finalization indefinitely.

**Fix**: Added a 5-second periodic ticker that calls `checkFinalization`
independently of chain events. Added `lastFinalNum` cache to skip redundant
full map scans when the finalized block hasn't advanced.

### Event Loop Stall Fix (event.Feed.Send blocking)

`event.Feed.Send` blocks until ALL subscribers consume the value. The RPC
subscription (`Events` in `api_txtracker.go`) creates a channel with buffer
128. Under heavy transaction load (~58K txs), events are emitted faster than
the WebSocket can push them to the browser. Once the subscriber's channel
fills, `Send` blocks, which blocks the entire tracker event loop — preventing
the finalize ticker, chain events, and all other processing from running.

This explains why finalization showed 0 despite valid `ForkchoiceUpdated`
calls: the event loop was stuck inside `emitEvent` → `event.Feed.Send`,
waiting for a slow WebSocket subscriber to drain its buffer.

**Fix**: Decoupled event emission from the event loop:
- `emitEvent` now sends to a buffered channel (`emitCh`, capacity 4096) using
  a non-blocking select. If the buffer is full, the event is dropped and
  `txEmitDroppedMeter` is incremented.
- A separate `emitLoop` goroutine drains `emitCh` and calls
  `event.Feed.Send`. If `Send` blocks on a slow subscriber, only the emit
  goroutine stalls — the main event loop continues processing.

### Top View Sort Fix

The Top view's sort depended entirely on `topCache` (RPC-fetched `TxInfo`),
but only ~30 visible rows are fetched at a time. Unfetched rows were pushed to
the sort bottom (`!ca → return 1`), creating a feedback loop where only the
initially cached rows ever appeared when sorted.

**Fix**: For `age` and `status`, sort using event subscription data (`txs` Map)
which is always available for every transaction:
- `age` → `ev._receivedAt` (wall-clock ms when UI received the first event)
- `status` → `statusOrd(ev.newStatus)`

RPC-dependent keys (`nonce`, `value`, `gas`, `gasfeecap`, `gastipcap`) keep
the existing cache-based sort since those values genuinely aren't available
without a fetch.

### Pool Drop Detection

Previously, once a transaction reached `TxPooled` it would stay there
until included or LRU-evicted from the tracker — even if the pool had
long since dropped it.

**New status**: `TxDropped` (iota 8) — transaction was accepted into the pool
but has since been evicted.

**Detection**: Reactive via `RemovedTxsEvent`. Both legacypool and blobpool
now fire this event when transactions are removed. The implementation uses
an accumulate-and-flush pattern: `trackRemoved(hash, reason)` appends to a
buffer during lock-held operations, and `flushRemoved()` sends a single
batched event after the lock is released (same pattern as `NewTxsEvent`).

The tracker subscribes to `RemovedTxsEvent` via `TxPoolReader.
SubscribeRemovedTransactions` and transitions matching `TxPooled` records
to `TxDropped` instantly. Removal events for transactions already at
`TxIncluded` or `TxFinalized` are ignored (pool removal also fires when
transactions are mined into blocks).

**Drop reasons**: Each `trackRemoved` call site provides a human-readable
reason string explaining why the transaction was removed. The reason is
carried through `RemovedTxsEvent.Reasons` (parallel to `Hashes`), stored
in `txRecord.dropReason`, surfaced in `TxInfo.DropReason` and
`TxTrackerEvent.DropReason`, and displayed in the browser UI.

Reason strings shared by both pools:

| Reason          | Trigger                        | legacypool call site           | blobpool call site          |
|-----------------|--------------------------------|--------------------------------|-----------------------------|
| `"replaced"`    | Same-nonce higher-fee tx added | `add`, `enqueueTx`, `promoteTx`| `add` (replacement path)    |
| `"underpriced"` | Tx below minimum gas tip       | `SetGasTip`, `add`, `promoteTx`| `SetGasTip` (+ cascade)     |
| `"nonce expired"`| Nonce already consumed by chain| `demoteUnexecutables`          | `reset` (overlap path)      |
| `"capacity"`    | Pool overflow eviction         | `truncateQueue`                | `drop`                      |

Reason strings specific to legacypool:

| Reason          | Trigger                                            | Call site              |
|-----------------|----------------------------------------------------|------------------------|
| `"expired"`     | Queue entry exceeds lifetime (`Lifetime` config)   | evict timer loop       |
| `"invalid"`     | Fails validation after reorg (balance/nonce shift)  | `promoteExecutables`   |
| `"rate limited"`| Pending count exceeds per-account fairness limit    | `truncatePending`      |
| `"underfunded"` | Balance too low or gas exceeds block limit          | `demoteUnexecutables`  |

Reason strings specific to blobpool:

| Reason          | Trigger                                            | Call site              |
|-----------------|----------------------------------------------------|------------------------|
| `"included"`    | All nonces consumed by chain (filled range)        | `reset` (filled path)  |
| `"nonce gap"`   | Nonce gap above chain state (dangling txs)         | `reset` (gapped path)  |

The pool-specific reasons reflect removal paths that only exist in one
pool. Legacypool has queue lifetime expiry, pending rate limiting,
post-reorg validation, and balance-based demotion. Blobpool has nonce gap
detection and filled-range inclusion cleanup. Both `"included"` and
`"nonce gap"` removals from blobpool are typically invisible to the
tracker because those transactions have already advanced to `TxIncluded`
via `ChainEvent` before the pool removal event arrives.

**Recovery paths from TxDropped**: A dropped transaction can re-enter the
lifecycle:
- Dropped → Requested (peer re-announces, fetcher re-requests)
- Dropped → Received (peer broadcasts again)
- Dropped → Pooled (re-accepted into pool via `NewTxsEvent`)
- Dropped → Included (mined despite being dropped from local pool)
- Dropped → Rejected (re-submitted but now rejected)

**Guard changes**: `handleFetchRequested`, `handleReceive`, `handlePooled`,
and `handleRejected` all accept `TxDropped` as a valid source status.

**UI**: Dropped counter in header badges, dropped filter option, dropped
step in progress pipeline, and a Dropped node in the Sankey diagram
branching off from Pooled (similar to Rejected branching off Received).
The feed error column shows the drop reason alongside reject errors. The
detail panel includes a "Drop Reason" row. The Sankey Dropped node shows
a breakdown label (e.g. "3 replaced, 2 underpriced").

### Transaction Type Filter

`TxType uint8` was added to `TxTrackerEvent` so the browser receives the
Ethereum transaction type (legacy=0, access list=1, dynamic fee=2, blob=3,
set-code=4)
with every subscription event, without needing an RPC fetch.

The browser stores `_txType` on each event (preserving the type from earlier
events if a new event has type 0). Checkbox groups appear in the Feed, Top, and
Stats filter bars. All three share a single `typeFilter` Set — toggling a type
on any pane syncs the checkboxes across all panes and marks all views dirty.

Type 0 serves double duty as both "legacy" and "type not yet known" (e.g.,
announced but metadata not yet received). This is acceptable since legacy
transactions are genuinely type 0.

## Future Work

- Use tracker data for peer scoring (bandwidth waste detection)
- Filter redundant transaction fetches based on tracker state
- Add eviction to txview browser (e.g., drop oldest entries past a threshold)
