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
