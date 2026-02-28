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
	"io"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/time/rate"
)

func TestPeerRateLimitKey(t *testing.T) {
	tests := []struct {
		proto string
		code  uint64
		want  string
	}{
		{"eth", 0x00, "eth/0x00"},
		{"eth", 0x08, "eth/0x08"},
		{"snap", 0x07, "snap/0x07"},
		{"foo", 0xff, "foo/0xff"},
	}
	for _, tt := range tests {
		got := rateLimitKey(tt.proto, tt.code)
		if got != tt.want {
			t.Errorf("rateLimitKey(%q, %d) = %q, want %q", tt.proto, tt.code, got, tt.want)
		}
	}
}

func TestPeerRateLimitBurstConsumption(t *testing.T) {
	rl := newPeerRateLimiter(log.New())
	closed := make(chan struct{})

	// eth/0x00 (Status) has burst=2. Consume the entire burst.
	for i := 0; i < 2; i++ {
		if err := rl.wait(closed, "eth", 0x00); err != nil {
			t.Fatalf("wait %d returned error: %v", i, err)
		}
	}
	// Next call should block (delay > 0). Verify by checking the reservation.
	limiter := rl.getOrCreate("eth", 0x00)
	r := limiter.Reserve()
	if r.Delay() == 0 {
		t.Fatal("expected non-zero delay after burst exhaustion")
	}
	r.Cancel()
}

func TestPeerRateLimitIndependentBuckets(t *testing.T) {
	rl := newPeerRateLimiter(log.New())
	closed := make(chan struct{})

	// Exhaust eth/0x00 burst (burst=2).
	for i := 0; i < 2; i++ {
		if err := rl.wait(closed, "eth", 0x00); err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	}
	// eth/0x02 (Transactions, burst=60) should still be available.
	if err := rl.wait(closed, "eth", 0x02); err != nil {
		t.Fatalf("different message type should not be affected: %v", err)
	}
}

func TestPeerRateLimitDefaultFallback(t *testing.T) {
	rl := newPeerRateLimiter(log.New())

	// Unknown protocol/code should use defaultRateLimit (burst=60).
	limiter := rl.getOrCreate("unknown", 0x42)
	if limiter.Burst() != defaultRateLimit.burst {
		t.Errorf("unknown message type burst = %d, want %d", limiter.Burst(), defaultRateLimit.burst)
	}
}

func TestPeerRateLimitTokenRefill(t *testing.T) {
	rl := newPeerRateLimiter(log.New())
	closed := make(chan struct{})

	// eth/0x00: rate=1/s, burst=2. Exhaust burst.
	for i := 0; i < 2; i++ {
		rl.wait(closed, "eth", 0x00)
	}
	// Wait should complete after tokens refill. Use a generous timeout.
	done := make(chan error, 1)
	go func() {
		done <- rl.wait(closed, "eth", 0x00)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait returned error after refill: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wait did not return after token refill")
	}
}

func TestPeerRateLimitCloseDuringWait(t *testing.T) {
	rl := newPeerRateLimiter(log.New())
	closed := make(chan struct{})

	// eth/0x00: rate=1/s, burst=2. Exhaust burst.
	for i := 0; i < 2; i++ {
		rl.wait(closed, "eth", 0x00)
	}
	// Start a wait that will block, then close the peer channel.
	done := make(chan error, 1)
	go func() {
		done <- rl.wait(closed, "eth", 0x00)
	}()
	// Give the goroutine time to enter the select.
	time.Sleep(50 * time.Millisecond)
	close(closed)

	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("wait returned %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not return after close")
	}
}

func TestPeerRateLimitAggregateExhaustion(t *testing.T) {
	rl := newPeerRateLimiter(log.New())
	closed := make(chan struct{})

	// Replace the aggregate limiter with a small one for testing.
	// burst=5, rate=1/s so we can exhaust it quickly without refill issues.
	rl.aggregate = rate.NewLimiter(1, 5)

	// Send 5 messages across different types (each per-type bucket has
	// burst >= 60, so they won't block).
	for i := 0; i < 5; i++ {
		if err := rl.wait(closed, "test", uint64(i)); err != nil {
			t.Fatalf("wait %d returned error: %v", i, err)
		}
	}
	// Aggregate bucket should now be exhausted. Next reservation must block.
	r := rl.aggregate.Reserve()
	if r.Delay() == 0 {
		t.Fatal("expected non-zero delay after aggregate burst exhaustion")
	}
	r.Cancel()
}

func TestPeerRateLimitAggregateIndependence(t *testing.T) {
	rl := newPeerRateLimiter(log.New())
	closed := make(chan struct{})

	// Exhaust a single per-type bucket (eth/0x00: burst=2).
	for i := 0; i < 2; i++ {
		if err := rl.wait(closed, "eth", 0x00); err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	}
	// Per-type bucket for eth/0x00 is exhausted, but aggregate still has plenty.
	// eth/0x02 (burst=60) should still work fine.
	if err := rl.wait(closed, "eth", 0x02); err != nil {
		t.Fatalf("different message type should not be affected: %v", err)
	}
	// Verify aggregate has tokens remaining (we only used 3 of 600).
	r := rl.aggregate.Reserve()
	if r.Delay() != 0 {
		t.Fatal("aggregate bucket should still have tokens")
	}
}
