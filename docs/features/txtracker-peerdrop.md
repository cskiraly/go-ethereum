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

## Configuration

- `inclusionProtectionPct = 10` — percentage of each peer category
  (inbound / dialed) to protect. With default 50 peers, this protects
  the top ~1-2 inbound and ~1-2 dialed peers by inclusion count.

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
