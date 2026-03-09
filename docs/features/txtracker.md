# Transaction Tracker

## Overview

The `eth/txtracker` package maintains a per-transaction lifecycle record from
first announcement through finalization. It provides a unified view that was
previously scattered across TxFetcher (fetching state), TxPool (pool status),
and the blockchain (inclusion/finalization).

## Lifecycle State Machine

```
P2P path:    Announced → Received → Pooled → Included → Finalized
                            │          ↑         │
                            │          └─────────┘ (reorg)
                            └──→ Rejected

Local path:  (local submit) → Pooled → Included → Finalized
```

## Architecture

Single-goroutine event loop (same pattern as TxFetcher):
- All state lives in the loop goroutine, no locks needed
- Callers send events via buffered channels (non-blocking)
- Queries use request/response channels (blocking)
- Chain events and pool events come via standard subscriptions

## Key Design Decisions

1. **No modifications to core/ packages**: The tracker subscribes to
   `ChainEvent` and `NewTxsEvent` via standard interfaces. Local transactions
   are detected as txs appearing in `NewTxsEvent` without prior tracker record.

2. **LRU eviction**: Bounded to `maxEntries` (default 65536). Each status
   transition moves the entry to the front. Finalized and rejected txs age out
   naturally since they receive no further updates.

3. **Reorg handling**: When a new block's parent doesn't match the previous
   head, all `TxIncluded` transactions at or above the new block number revert
   to `TxPooled`.

4. **Peer statistics**: Maintained alongside tx records in the event loop.
   Tracks announcements, deliveries, useful deliveries, and first-announcer
   counts per peer. Stats are cleared on peer disconnect.

## Files

- `eth/txtracker/tracker.go` — Core types, event loop, state machine
- `eth/txtracker/metrics.go` — Meter and gauge registrations
- `eth/txtracker/tracker_test.go` — Unit tests
- `eth/handler.go` — Creates, starts, stops tracker; hooks addTxs and peer drop
- `eth/handler_eth.go` — Feeds announcement and receive events to tracker

## Code Review Fixes (commit 5)

- **BUG-1**: `txIncludedMeter` was firing for every transaction in every block,
  not just tracked ones. Moved inside the status-update block.
- **BUG-2**: `status < TxIncluded || status == TxIncluded` simplified to
  `status <= TxIncluded`.
- **BUG-3**: `TestPooledToIncluded` was a dead test (hash mismatch between
  `makeHash(5)` and `makeTx(hash).Hash()`). Removed along with unused `makeTx`.
- **IMPROVE-1**: Replaced `containsString` helper with `slices.Contains`.

## Future Work

- Use tracker data for peer scoring (bandwidth waste detection)
- Filter redundant transaction fetches based on tracker state
- Expose tracker status via debug API
