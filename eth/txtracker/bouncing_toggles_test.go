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

package txtracker

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestBouncingMainToggleOff verifies the master read toggle: with main=false,
// IsBouncing returns false even for a hash currently in the bouncing map,
// and the whatif/suppressed counter increments. Toggling back ON restores
// suppression.
func TestBouncingMainToggleOff(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	key, _ := crypto.GenerateKey()
	tx := signedTx(t, key, 1)
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)
	if !tr.IsBouncing(hash, "") {
		t.Fatalf("baseline: IsBouncing(%v) = false; want true", hash)
	}

	whatifBefore := bouncingWhatifSuppressedMeter.Snapshot().Count()
	if prev, ok := tr.SetBouncingFlag("main", false); !ok || !prev {
		t.Fatalf("SetBouncingFlag(main, false) = (%v, %v); want (true, true)", prev, ok)
	}

	if tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = true with main=false; want false", hash)
	}
	if delta := bouncingWhatifSuppressedMeter.Snapshot().Count() - whatifBefore; delta != 1 {
		t.Errorf("whatif/suppressed delta = %d, want 1", delta)
	}

	// Restore — protection should immediately resume because the entry
	// is still in the map (main flag controls the read side only).
	tr.SetBouncingFlag("main", true)
	if !tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = false after main=true; want true", hash)
	}
}

// TestBouncingDropToggleOff verifies the drop-side insert toggle: with
// drop=false, post-acceptance evictions don't insert into the bouncing
// map but the whatif/inserted_drop counter increments. The reject path
// must remain functional (toggle isolation).
func TestBouncingDropToggleOff(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tr.SetBouncingFlag("drop", false)

	keyDrop, _ := crypto.GenerateKey()
	keyReject, _ := crypto.GenerateKey()
	dropTx := signedTx(t, keyDrop, 1)
	rejectTx := signedTx(t, keyReject, 1)

	whatifDropBefore := bouncingWhatifInsertedDropMeter.Snapshot().Count()
	dropHash := driveBouncing(tr, "peerA", dropTx, core.RemovalRateLimited)
	if tr.IsBouncing(dropHash, "") {
		t.Errorf("IsBouncing(drop) = true with drop=false; want false")
	}
	if delta := bouncingWhatifInsertedDropMeter.Snapshot().Count() - whatifDropBefore; delta != 1 {
		t.Errorf("whatif/inserted_drop delta = %d, want 1", delta)
	}

	// Reject path still inserts.
	rejectHash := driveRejection(tr, "peerB", rejectTx, txpool.ErrTxPoolOverflow)
	if !tr.IsBouncing(rejectHash, "") {
		t.Errorf("IsBouncing(reject) = false with drop=false; want true (reject toggle is independent)")
	}
}

// TestBouncingRejectToggleOff is the symmetric reject-side test: with
// reject=false, submission-time rejections don't insert, but the drop
// path stays functional.
func TestBouncingRejectToggleOff(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tr.SetBouncingFlag("reject", false)

	keyDrop, _ := crypto.GenerateKey()
	keyReject, _ := crypto.GenerateKey()
	dropTx := signedTx(t, keyDrop, 1)
	rejectTx := signedTx(t, keyReject, 1)

	whatifRejectBefore := bouncingWhatifInsertedRejectMeter.Snapshot().Count()
	rejectHash := driveRejection(tr, "peerA", rejectTx, txpool.ErrFutureReplacePending)
	if tr.IsBouncing(rejectHash, "") {
		t.Errorf("IsBouncing(reject) = true with reject=false; want false")
	}
	if delta := bouncingWhatifInsertedRejectMeter.Snapshot().Count() - whatifRejectBefore; delta != 1 {
		t.Errorf("whatif/inserted_reject delta = %d, want 1", delta)
	}

	// Drop path still inserts.
	dropHash := driveBouncing(tr, "peerB", dropTx, core.RemovalRateLimited)
	if !tr.IsBouncing(dropHash, "") {
		t.Errorf("IsBouncing(drop) = false with reject=false; want true (drop toggle is independent)")
	}
}

// TestBouncingFeeGateToggleOff verifies that with fee_gate=false the
// per-block sweep clears entries on sender inclusion regardless of fee
// (stage-1 behaviour), and the whatif/gate_blocked counter records the
// would-have-blocked event.
func TestBouncingFeeGateToggleOff(t *testing.T) {
	tr := New()
	tr.SetPoolFloor(stubPoolFloor{floor: 100}) // would normally block fee=50
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tr.SetBouncingFlag("fee_gate", false)

	key, _ := crypto.GenerateKey()
	tx := signedTxWithGasPrice(t, key, 1, 50) // tip 50 < floor 100
	hash := driveBouncing(tr, "peerA", tx, core.RemovalRateLimited)
	if !tr.IsBouncing(hash, "") {
		t.Fatalf("baseline: IsBouncing(%v) = false; want true", hash)
	}

	whatifGateBefore := bouncingWhatifGateBlockedMeter.Snapshot().Count()
	clearedBefore := bouncingClearedMeter.Snapshot().Count()
	gateBlockedBefore := bouncingGateBlockedMeter.Snapshot().Count()

	included := signedTxWithGasPrice(t, key, 99, 50)
	chain.addBlockAtHeight(1, 0, []*types.Transaction{included}, true)
	chain.sendHead(1)
	waitStep(t, tr)

	if tr.IsBouncing(hash, "") {
		t.Errorf("IsBouncing(%v) = true after include with fee_gate=false; want false (gate disabled, entry should clear)", hash)
	}
	if delta := bouncingWhatifGateBlockedMeter.Snapshot().Count() - whatifGateBefore; delta != 1 {
		t.Errorf("whatif/gate_blocked delta = %d, want 1", delta)
	}
	if delta := bouncingClearedMeter.Snapshot().Count() - clearedBefore; delta != 1 {
		t.Errorf("cleared delta = %d, want 1 (gate disabled, normal clear path fires)", delta)
	}
	if delta := bouncingGateBlockedMeter.Snapshot().Count() - gateBlockedBefore; delta != 0 {
		t.Errorf("gate_blocked delta = %d, want 0 (real meter must NOT fire when toggle off)", delta)
	}
}

// TestBouncingFlagsRPC exercises the txtracker_bouncingFlags and
// txtracker_setBouncingFlag round-trip through the API surface that
// geth attach uses, including the unknown-flag error path.
func TestBouncingFlagsRPC(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	api := NewAPI(tr)
	ctx := context.Background()

	// Defaults: all true.
	flags := api.BouncingFlags(ctx)
	for _, name := range BouncingFlagNames {
		if !flags[name] {
			t.Errorf("default flags[%s] = false, want true", name)
		}
	}

	// Flip "drop" off; previous value should be true.
	prev, err := api.SetBouncingFlag(ctx, "drop", false)
	if err != nil {
		t.Fatalf("SetBouncingFlag(drop, false): %v", err)
	}
	if !prev {
		t.Errorf("previous drop value = false, want true")
	}

	flags = api.BouncingFlags(ctx)
	if flags["drop"] != false {
		t.Errorf("after set drop=false: flags[drop] = %v, want false", flags["drop"])
	}
	for _, other := range []string{"main", "reject", "fee_gate"} {
		if !flags[other] {
			t.Errorf("after set drop=false: flags[%s] = false, want true (independence)", other)
		}
	}

	// Unknown flag returns an error.
	if _, err := api.SetBouncingFlag(ctx, "bogus", false); err == nil {
		t.Errorf("SetBouncingFlag(bogus, false): expected error, got nil")
	}
}

// Compile-time assertion that the BouncingFlagNames list stays in sync
// with the SetBouncingFlag dispatch. Adding a new flag in one place
// without the other will trip this test.
func TestBouncingFlagNamesAccepted(t *testing.T) {
	tr := New()
	for _, name := range BouncingFlagNames {
		if _, ok := tr.SetBouncingFlag(name, true); !ok {
			t.Errorf("BouncingFlagNames lists %q but SetBouncingFlag does not accept it", name)
		}
	}
}
