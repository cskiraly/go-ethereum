// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.

package txtracker

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestBouncingStatsPerReason verifies that insertsByReason is keyed by
// canonical category and counts per-rejection-reason inserts.
func TestBouncingStatsPerReason(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	reasons := []error{
		txpool.ErrTxPoolOverflow,
		core.ErrInsufficientFunds,
		fmt.Errorf("%w: balance 1, tx cost 2", core.ErrInsufficientFunds),
		txpool.ErrAuthorityReserved,
	}
	for _, reason := range reasons {
		key, _ := crypto.GenerateKey()
		tx := signedTx(t, key, 1)
		driveRejection(tr, "peerA", tx, reason)
	}

	stats := tr.BouncingStats()
	if stats.InsertsByReason["txpool_full"] != 1 {
		t.Errorf("txpool_full count = %d, want 1", stats.InsertsByReason["txpool_full"])
	}
	if stats.InsertsByReason["insufficient_funds"] != 2 {
		t.Errorf("insufficient_funds count = %d, want 2 (bare + wrapped)", stats.InsertsByReason["insufficient_funds"])
	}
	if stats.InsertsByReason["authority_reserved"] != 1 {
		t.Errorf("authority_reserved count = %d, want 1", stats.InsertsByReason["authority_reserved"])
	}
	// Reasons that don't enter the bouncing map should NOT appear.
	if _, exists := stats.InsertsByReason["other"]; exists {
		t.Errorf("unexpected 'other' bucket: %d", stats.InsertsByReason["other"])
	}
	if stats.CumInsertsReject != 4 {
		t.Errorf("CumInsertsReject = %d, want 4", stats.CumInsertsReject)
	}
}

// TestBouncingStatsClearPaths verifies that clear-path counters (TTL,
// sender-inclusion, FIFO) increment correctly when entries leave the
// map through each path.
func TestBouncingStatsClearPaths(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	// Insert an entry, then force-clear via sender-inclusion path.
	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveRejection(tr, "peerA", tx, txpool.ErrTxPoolOverflow)
	tr.deleteBouncing(hash, clearSenderInclusion)

	// Insert another and clear via FIFO.
	key2, _ := crypto.GenerateKey()
	tx2 := signedTx(t, key2, 1)
	hash2 := driveRejection(tr, "peerA", tx2, txpool.ErrTxPoolOverflow)
	tr.deleteBouncing(hash2, clearFIFO)

	stats := tr.BouncingStats()
	if stats.ClearsBySenderInclusion != 1 {
		t.Errorf("ClearsBySenderInclusion = %d, want 1", stats.ClearsBySenderInclusion)
	}
	if stats.ClearsByFIFO != 1 {
		t.Errorf("ClearsByFIFO = %d, want 1", stats.ClearsByFIFO)
	}
	if stats.LiveEntries != 0 {
		t.Errorf("LiveEntries = %d, want 0 (both cleared)", stats.LiveEntries)
	}
}
