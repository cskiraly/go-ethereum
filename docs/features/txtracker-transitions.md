# Transaction Tracker — State Transitions

Complete reference of all state transitions in `eth/txtracker/tracker.go`,
annotated with whether each transition is possible in practice.

States: Announced(1), Requested(2), Received(3), Pooled(4), Included(5),
Finalized(6), Rejected(7), Dropped(8).

Terminal states: Dropped, Rejected, Finalized.

## New record creation (oldStatus = 0)

| Handler | → Status | Possible? | Reason |
|---|---|---|---|
| handleAnnounce | → Announced | **Yes** | Normal P2P: peer sends hash |
| handleReceive | → Received | **Yes** | Unsolicited push (body without prior announce) |
| handlePooled | → Pooled | **No** | NotifyReceived always precedes NotifyPooled; record would exist |
| handleRejected | → Rejected | **No** | NotifyReceived always precedes NotifyRejected; record would exist |
| handleChainEvent | → Included | **Yes** | Tx first seen in a synced block (chain-only) |
| handleNewTxs | → Pooled | **Yes** | Local submission via RPC |

## Forward transitions (non-terminal → non-terminal)

| From → To | Handler | Possible? | Reason |
|---|---|---|---|
| Announced → Requested | handleFetchRequested | **Yes** | Normal: announce → request body |
| Announced → Received | handleReceive | **Yes** | Peer B pushes body while peer A only announced |
| Announced → Pooled | handlePooled | **No** | Pool needs full body; not available at Announced |
| Announced → Included | handleChainEvent | **Yes** | Mined before we fetch the body |
| Requested → Received | handleReceive | **Yes** | Normal: request → response |
| Requested → Pooled | handlePooled | **No** | Pool needs body; not received yet at Requested |
| Requested → Included | handleChainEvent | **Yes** | Mined before response arrives |
| Received → Pooled | handlePooled | **Yes** | Normal: body received → pool accepts |
| Received → Included | handleChainEvent | **Yes** | Mined before pool processes it |
| Pooled → Included | handleChainEvent | **Yes** | Normal: pool → block inclusion |
| Included → Finalized | checkFinalization | **Yes** | Normal: block reaches finality |

## Forward to terminal

| From → To | Handler | Possible? | Reason |
|---|---|---|---|
| Announced → Rejected | handleRejected | **No** | Pool needs body to validate/reject |
| Requested → Rejected | handleRejected | **No** | Body not yet received |
| Received → Rejected | handleRejected | **Yes** | Normal: body received, pool rejects |
| Pooled → Dropped | handleRemovedTxs | **Yes** | Normal: evicted from pool |

## Backward (reorg)

| From → To | Handler | Bumps |
|---|---|---|
| Included → Pooled | handleReorg | reorgCount (NOT returns) |

## Terminal → non-terminal (returns bumped)

Note: hot/cold is internal to the tracker and does not affect which
transitions are possible. The rest of the system (eth handler, pool)
is unaware of the tracker's storage tier. `NotifyReceived` always
precedes `NotifyPooled`/`NotifyRejected`, so any path through the
pool goes through Received first.

| From → To | Handler | Possible? | Reason |
|---|---|---|---|
| Dropped → Requested | handleFetchRequested | **Yes** | Re-fetch cycle |
| Dropped → Received | handleReceive | **Yes** | Peer re-sends body; main return entry point |
| Dropped → Pooled | handlePooled | **No** | NotifyReceived precedes NotifyPooled; status already Received |
| Dropped → Pooled | handleNewTxs | **No** | NewTxsEvent fires after pool.Add; NotifyReceived already ran |
| Dropped → Included | handleChainEvent | **Yes** | Miner included it anyway |
| Rejected → Included | handleChainEvent | **Yes** | We rejected, miner accepted |
| Rejected → Pooled | handlePooled | **No** | Same: NotifyReceived precedes NotifyPooled |
| Rejected → Pooled | handleNewTxs | **No** | Same: NewTxsEvent fires after NotifyReceived |
| Finalized → Pooled | handleNewTxs | **No** | Re-submitting finalized tx is nonsensical |

## Terminal → terminal (returns NOT bumped)

| From → To | Handler | Possible? |
|---|---|---|
| Dropped → Rejected | handleRejected | **Yes** |

## No transition (silent)

| Situation | Handler | What happens |
|---|---|---|
| Re-announce of any existing record | handleAnnounce | Adds announcer only, no status change, no event emitted |
