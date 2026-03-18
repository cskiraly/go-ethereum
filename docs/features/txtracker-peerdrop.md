# Peer Drop Protection via Transaction Inclusion Share

## Overview

The dropper (`eth/dropper.go`) periodically disconnects random peers
to create churn and allow new connections. This is blind to peer
quality — a peer that consistently delivers transactions that end up
included on chain is just as likely to be dropped as one that delivers
only spam.

This feature uses the txtracker's per-peer inclusion statistics to
protect high-value peers. Peers contributing to the top 80% of
on-chain inclusions are shielded from random dropping.

## Mechanism

When the dropper fires (`dropRandomPeer`), after building the list of
droppable peers (excluding trusted, static, and recent peers), it
queries `txtracker.GetAllPeerStats()` and computes inclusion share:

1. Split droppable peers into inbound and dialed categories
2. For each category, sort by `Included` descending
3. Protect the top N peers in each category, where
   N = 10% of the category's max peer count
4. Only peers with `Included > 0` are eligible for protection
5. Remove protected peers from the droppable list
6. Drop randomly from the remaining peers

If all droppable peers are protected, the drop is skipped entirely.

## Protection Categories

The dropper supports multiple protection categories. Each independently
selects its top-N peers per inbound/dialed pool; the union of all
selections is protected.

**1. Total inclusions** (`total-included`)
Score: `PeerStats.Included` (cumulative count).
Favors long-lived, consistently productive peers.

**2. Recent inclusions** (`recent-included`)
Score: `PeerStats.RecentIncluded` (EMA, alpha=0.05).
Updated per block for all tracked peers:
```
peer.recentIncluded = 0.95 * peer.recentIncluded + 0.05 * blockInclusions
```
Favors peers delivering txs that make it on-chain in recent blocks.
A newly productive peer gets protection faster than with total count.
An inactive peer's score decays toward zero over ~20 blocks.

Each category protects the top 10% of each pool (inbound / dialed).
With default 50 peers (17 dialed, 33 inbound), each category protects
~1-2 dialed and ~3 inbound peers. The union means up to ~2-4 dialed
and ~3-6 inbound peers are protected (fewer if sets overlap).

## Future Protection Categories

The `protectionCategories` slice is designed for easy extension. Each
category only needs a scoring function and a percentage. Candidates:

### Propagation speed

**First announcer rate** — peers that are frequently the first to tell
us about a tx are our fastest information source. Score:
`FirstAnnouncer / Announced`. Protects peers at the edge of the
network. Data already in `PeerStats`.

**Delivery latency** — peers with the lowest request→delivery latency.
Score: inverse of `AvgLatencyMs`. Fast responders are valuable for
time-sensitive tx fetching. Data already in `PeerStats`.

### Delivery quality

**Useful delivery rate** — peers whose deliveries are actually accepted
by the pool (not duplicates/already-known). Score:
`UsefulDelivery / Delivered`. A peer with 95% useful rate is more
bandwidth-efficient than one at 30%. Data already in `PeerStats`.

**Low rejection rate** — inverse of `Rejected / Delivered`. Peers that
rarely deliver txs our pool rejects are better curated sources. Data
already in `PeerStats`.

### Diversity

**Client diversity** — protect at least one peer of each client type
(Geth, Erigon, Nethermind, Besu, Reth). Different clients may have
different tx propagation behavior and mempool policies. Client name
is available from `PeerFullInfo.Name` via the p2p server. Unlike the
other categories, this would be a binary selection (one per client
type) rather than a top-N ranking.

**Network diversity** — protect peers from different /16 subnets or
ASNs. Reduces single-point-of-failure risk from network partitions.
Remote address is available from `PeerFullInfo.Address`.

### Uniqueness

**Exclusive delivery rate** — fraction of a peer's included txs that
no other peer delivered. If a peer is our only source for certain txs
that end up on chain, losing them means losing those txs. Requires
aggregating per-tx `announcers[]` and `deliverer` data across the
tracker — not currently exposed as a scalar stat.

## Configuration

- `inclusionProtectionPct = 10` — per-category percentage
- `emaAlpha = 0.05` — EMA smoothing factor (in handleChainEvent)

## Metrics

- `eth/dropper/protected` — times a drop was skipped because all
  droppable candidates were inclusion-protected

## Existing protections (unchanged)

- Trusted peers — never dropped
- Static dialed peers — never dropped
- Recent peers (< 10 min lifetime) — not dropped
- Peers in a non-bottleneck category — not dropped

## Files

- `eth/dropper.go` — `filterProtectedPeers`, `getPeerStatsFunc`
- `eth/backend.go` — wires `txTracker.GetAllPeerStats` to dropper
