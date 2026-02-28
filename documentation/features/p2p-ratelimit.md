# p2p: per-peer, per-message-type token bucket rate limiter

## Problem

go-ethereum has no per-peer, per-message-type rate limiting on incoming P2P
messages. A misbehaving peer can flood arbitrary message types without being
slowed down.

## Design

Intercept at `Peer.handle()` — the single chokepoint where all subprotocol
messages pass through. Each non-trusted peer gets a lazily-initialized set of
token buckets keyed by `"proto/0xNN"`.

When a rate limit is exceeded the `readLoop` goroutine blocks via
`Reserve()` + timed wait, which suspends all protocol handler goroutines for
that peer via TCP backpressure. No messages are discarded and no disconnection
occurs.

### Key decisions

- **`Reserve()` + timer, not `Allow()`**: blocks until token available instead
  of rejecting.
- **No mutex needed**: `handle()` is called from a single goroutine (`readLoop`).
- **Trusted peers exempt**: `rateLimiter` field is nil for trusted peers.
- **Metrics on slow path only**: `p2p/ratelimit/<proto>/<code>` recorded when
  delay > 0.
- **No new dependencies**: `golang.org/x/time/rate` already in go.mod.
- **Hardcoded limits**: generous defaults, configurability can follow later.

## Files

- `p2p/ratelimit.go` — rate limiter implementation
- `p2p/ratelimit_test.go` — unit tests
- `p2p/peer.go` — integration (3 surgical edits)
