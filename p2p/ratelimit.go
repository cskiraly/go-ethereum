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

package p2p

import (
	"fmt"
	"io"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"golang.org/x/time/rate"
)

// msgRateLimit defines a token bucket configuration for a message type.
type msgRateLimit struct {
	rate  rate.Limit
	burst int
}

// defaultMsgRateLimits contains per-message-type rate limits keyed by "proto/0xNN".
// These are generous limits intended to catch flooding, not normal operation.
var defaultMsgRateLimits = map[string]msgRateLimit{
	// eth protocol
	"eth/0x00": {1, 2},   // Status
	"eth/0x01": {5, 10},  // NewBlockHashes
	"eth/0x02": {20, 60}, // Transactions
	"eth/0x03": {20, 40}, // GetBlockHeaders
	"eth/0x04": {20, 40}, // BlockHeaders
	"eth/0x05": {20, 40}, // GetBlockBodies
	"eth/0x06": {20, 40}, // BlockBodies
	"eth/0x07": {10, 20}, // NewBlock
	"eth/0x08": {20, 60}, // NewPooledTransactionHashes
	"eth/0x09": {20, 40}, // GetPooledTransactions
	"eth/0x0a": {20, 40}, // PooledTransactions
	"eth/0x0f": {10, 20}, // GetReceipts
	"eth/0x10": {10, 20}, // Receipts
	"eth/0x11": {20, 40}, // BlockRangeUpdate

	// snap protocol
	"snap/0x00": {20, 40}, // GetAccountRange
	"snap/0x01": {20, 40}, // AccountRange
	"snap/0x02": {20, 40}, // GetStorageRanges
	"snap/0x03": {20, 40}, // StorageRanges
	"snap/0x04": {20, 40}, // GetByteCodes
	"snap/0x05": {20, 40}, // ByteCodes
	"snap/0x06": {20, 40}, // GetTrieNodes
	"snap/0x07": {20, 40}, // TrieNodes
}

// defaultRateLimit is used for message types not in defaultMsgRateLimits.
var defaultRateLimit = msgRateLimit{rate: 30, burst: 60}

// rateLimitKey returns the map key for a protocol name and relative message code.
func rateLimitKey(protoName string, relCode uint64) string {
	return fmt.Sprintf("%s/0x%02x", protoName, relCode)
}

// peerRateLimiter holds per-message-type token buckets for a single peer.
// It is only accessed from the peer's readLoop goroutine, so no mutex is needed.
type peerRateLimiter struct {
	limiters map[string]*rate.Limiter
	logger   log.Logger
}

// newPeerRateLimiter creates a new rate limiter for a peer.
func newPeerRateLimiter(logger log.Logger) *peerRateLimiter {
	return &peerRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		logger:   logger,
	}
}

// getOrCreate returns the limiter for the given message type, creating it lazily.
func (rl *peerRateLimiter) getOrCreate(protoName string, relCode uint64) *rate.Limiter {
	key := rateLimitKey(protoName, relCode)
	if l, ok := rl.limiters[key]; ok {
		return l
	}
	cfg, ok := defaultMsgRateLimits[key]
	if !ok {
		cfg = defaultRateLimit
	}
	l := rate.NewLimiter(cfg.rate, cfg.burst)
	rl.limiters[key] = l
	return l
}

// wait reserves a token for the given message type and blocks until it is
// available or the peer's closed channel fires. It is called from the peer's
// readLoop goroutine.
func (rl *peerRateLimiter) wait(closed <-chan struct{}, protoName string, relCode uint64) error {
	limiter := rl.getOrCreate(protoName, relCode)

	r := limiter.Reserve()
	delay := r.Delay()
	if delay == 0 {
		return nil // fast path: token available immediately
	}
	// Slow path: suspend until token replenishes or peer closes.
	rl.logger.Debug("Rate limited, suspending", "proto", protoName, "code", relCode, "delay", delay)
	if metrics.Enabled() {
		key := rateLimitKey(protoName, relCode)
		m := fmt.Sprintf("p2p/ratelimit/%s", key)
		metrics.GetOrRegisterMeter(m, nil).Mark(1)
	}

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil // token now available, resume processing
	case <-closed:
		r.Cancel()
		return io.EOF // peer shutting down
	}
}
