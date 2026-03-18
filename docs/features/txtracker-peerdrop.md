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

1. Sum `Included` across all peers to get `totalIncluded`
2. Sort peers by `Included` descending
3. Walk the sorted list, accumulating inclusions. Peers that together
   account for the top 80% of inclusions are marked as protected.
4. Remove protected peers from the droppable list
5. Drop randomly from the remaining peers

If all droppable peers are protected, the drop is skipped entirely.

## Configuration

- `inclusionProtectionPct = 80` — the inclusion share threshold.
  Peers contributing to the top 80% of inclusions are protected.

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
