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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestCheckUpdate decodes a light client update served by Lodestar (mainnet, Fulu, the
// update of period 1873; its JSON without the committee's 512 keys) and checks that a
// forged next sync committee or finalized header doesn't pass checkUpdate.
func TestCheckUpdate(t *testing.T) {
	ssz, err := os.ReadFile("testdata/mainnet-update.ssz")
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile("testdata/mainnet-update.json")
	if err != nil {
		t.Fatal(err)
	}
	var want struct {
		Version string `json:"version"`
		Data    struct {
			Attested  jsonHeader `json:"attested_header"`
			Finalized jsonHeader `json:"finalized_header"`
			Committee struct {
				Aggregate hexutil.Bytes `json:"aggregate_pubkey"`
			} `json:"next_sync_committee"`
			SignatureSlot string `json:"signature_slot"`
		} `json:"data"`
	}
	if err := json.Unmarshal(js, &want); err != nil {
		t.Fatal(err)
	}
	u, committee, err := DecodeUpdate(want.Version, ssz)
	if err != nil {
		t.Fatal(err)
	}
	if u.AttestedHeader.Header.Slot != want.Data.Attested.slot(t) || u.FinalizedHeader == nil || u.FinalizedHeader.Slot != want.Data.Finalized.slot(t) {
		t.Fatalf("decoded slots %d/%v, want %d/%d", u.AttestedHeader.Header.Slot, u.FinalizedHeader, want.Data.Attested.slot(t), want.Data.Finalized.slot(t))
	}
	if s, _ := strconv.ParseUint(want.Data.SignatureSlot, 10, 64); u.AttestedHeader.SignatureSlot != s {
		t.Fatalf("signature slot %d, want %d", u.AttestedHeader.SignatureSlot, s)
	}
	if agg := committee[len(committee)-params.BLSPubkeySize:]; !bytes.Equal(agg, want.Data.Committee.Aggregate) {
		t.Fatalf("aggregate key %x, want %x", agg, want.Data.Committee.Aggregate)
	}
	if err := checkUpdate(u, 1873); err != nil {
		t.Fatalf("valid update: %v", err)
	}
	if err := checkUpdate(u, 1874); err == nil {
		t.Fatal("update accepted for another period")
	}

	// A different next committee (one byte of a key) under the same signed header.
	forged := bytes.Clone(ssz)
	forged[4+100] ^= 1
	u, _, err = DecodeUpdate(want.Version, forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkUpdate(u, 1873); err == nil {
		t.Fatal("update with a forged next sync committee accepted")
	}
	// A different finalized header (its slot).
	forged = bytes.Clone(ssz)
	pOffFin := 4 + types.SerializedSyncCommitteeSize + committeeBranchDepth(want.Version)*32
	offF := binary.LittleEndian.Uint32(forged[pOffFin:])
	forged[offF] ^= 1
	u, _, err = DecodeUpdate(want.Version, forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkUpdate(u, 1873); err == nil {
		t.Fatal("update with a forged finalized header accepted")
	}
	// A finalized header of the period before: kept without its finality (its committee
	// proof still checked).
	forged = bytes.Clone(ssz)
	binary.LittleEndian.PutUint64(forged[offF:], 1873*8192-32)
	u, _, err = DecodeUpdate(want.Version, forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkUpdate(u, 1873); err != nil || u.FinalizedHeader != nil {
		t.Fatalf("update with an older finalized header: err %v, finalized header %v", err, u.FinalizedHeader)
	}
	forged[4+100] ^= 1
	u, _, _ = DecodeUpdate(want.Version, forged)
	if err := checkUpdate(u, 1873); err == nil {
		t.Fatal("update with a forged next sync committee accepted")
	}
}

// TestCheckGossiped checks that the gossiped updates of the test data (mainnet: Fulu; a
// devnet: Gloas) pass checkOptimistic and checkFinality, and forged ones don't.
func TestCheckGossiped(t *testing.T) {
	for _, net := range []string{"mainnet", "devnet"} {
		ssz, js := load(t, net+"-optimistic_update")
		opt, err := DecodeOptimisticUpdate(js.Version, ssz)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkOptimistic(&opt); err != nil {
			t.Errorf("%s optimistic update: %v", net, err)
		}
		opt.Attested.BodyRoot[0] ^= 1
		if err := checkOptimistic(&opt); err == nil {
			t.Errorf("%s optimistic update with a forged body root accepted", net)
		}

		ssz, js = load(t, net+"-finality_update")
		fin, err := DecodeFinalityUpdate(js.Version, ssz)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkFinality(&fin); err != nil {
			t.Errorf("%s finality update: %v", net, err)
		}
		fin.Finalized.Slot++
		if err := checkFinality(&fin); err == nil {
			t.Errorf("%s finality update with a forged finalized header accepted", net)
		}
	}
}

// gossipTester is a node without a network, for the gossip rules.
type gossipTester struct {
	*Node
	verify     func(types.SignedHeader) (bool, error)
	nOpt, nFin int // updates handed on
}

func newGossipTester() *gossipTester {
	gt := &gossipTester{Node: new(Node)}
	gt.cfg = Config{
		GenesisTime:  params.MainnetLightConfig.GenesisTime,
		VerifyHeader: func(h types.SignedHeader) (bool, error) { return gt.verify(h) },
	}
	gt.verify = func(types.SignedHeader) (bool, error) { return true, nil }
	gt.callbacks.Store(&callbacks{
		optimistic: func(peer.ID, types.OptimisticUpdate) { gt.nOpt++ },
		finality:   func(peer.ID, types.FinalityUpdate) { gt.nFin++ },
	})
	return gt
}

func expect(t *testing.T, what string, got, want pubsub.ValidationResult) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: validation result %v, want %v", what, got, want)
	}
}

func TestGossipRules(t *testing.T) {
	ssz, _ := load(t, "mainnet-finality_update")
	var digest [4]byte
	gt := newGossipTester()

	// Forged finalized headers: far ahead, which used to block forwarding the real ones,
	// is malformed (rejected); one slot off fails its proof (ignored, not forwarded).
	offF := binary.LittleEndian.Uint32(ssz[4:8])
	forged := bytes.Clone(ssz)
	binary.LittleEndian.PutUint64(forged[offF:], 1<<40)
	res, _ := gt.onFinality("", "fulu", digest, forged)
	expect(t, "finalized far ahead", res, pubsub.ValidationReject)
	forged = bytes.Clone(ssz)
	forged[offF] ^= 1
	res, _ = gt.onFinality("", "fulu", digest, forged)
	expect(t, "finalized header forged", res, pubsub.ValidationIgnore)
	if gt.nFin != 0 {
		t.Fatal("forged finality update handed on")
	}

	// Fewer than two thirds of the committee: forwarded, handed on once.
	p := 8 + finalityBranchDepth("fulu")*32
	weak := bytes.Clone(ssz)
	clear(weak[p : p+32])
	res, _ = gt.onFinality("", "fulu", digest, weak)
	expect(t, "weak finality", res, pubsub.ValidationAccept)
	res, _ = gt.onFinality("", "fulu", digest, weak)
	expect(t, "weak finality again", res, pubsub.ValidationIgnore)
	// The same finalized header with a supermajority: forwarded too, then no more.
	res, _ = gt.onFinality("", "fulu", digest, ssz)
	expect(t, "supermajority finality", res, pubsub.ValidationAccept)
	res, _ = gt.onFinality("", "fulu", digest, ssz)
	expect(t, "supermajority finality again", res, pubsub.ValidationIgnore)
	if gt.nFin != 2 || !bytes.Equal(gt.finality.served.ssz, ssz) {
		t.Fatalf("handed on %d finality updates (want 2), serving the last: %v", gt.nFin, bytes.Equal(gt.finality.served.ssz, ssz))
	}

	// Optimistic updates: fewer signers than required are ignored, a bad signature too
	// (geth's signature domain can differ from the spec's around a fork), and neither is
	// handed on.
	ssz, _ = load(t, "mainnet-optimistic_update")
	gt.cfg.MinSigners = params.SyncCommitteeSize + 1
	res, _ = gt.onOptimistic("", "fulu", digest, ssz)
	expect(t, "too few signers", res, pubsub.ValidationIgnore)
	gt.cfg.MinSigners = 0
	gt.verify = func(types.SignedHeader) (bool, error) { return false, nil }
	res, err := gt.onOptimistic("", "fulu", digest, ssz)
	expect(t, "bad signature", res, pubsub.ValidationIgnore)
	if !errors.Is(err, errBadSignature) || gt.nOpt != 0 {
		t.Fatalf("bad signature: err %v, handed on %d", err, gt.nOpt)
	}
	// Without the committee of its period: handed on once, not forwarded or served.
	gt.verify = func(types.SignedHeader) (bool, error) { return false, errors.New("missing committee") }
	res, _ = gt.onOptimistic("", "fulu", digest, ssz)
	expect(t, "unknown committee", res, pubsub.ValidationIgnore)
	res, _ = gt.onOptimistic("", "fulu", digest, ssz)
	expect(t, "unknown committee again", res, pubsub.ValidationIgnore)
	if gt.nOpt != 1 || gt.optimistic.served.ssz != nil {
		t.Fatalf("unverified update: handed on %d times (want 1), served %v", gt.nOpt, gt.optimistic.served.ssz != nil)
	}
	// Once verifiable: forwarded and handed on.
	gt.verify = func(types.SignedHeader) (bool, error) { return true, nil }
	res, _ = gt.onOptimistic("", "fulu", digest, ssz)
	expect(t, "verified", res, pubsub.ValidationAccept)
	// A newer one signed in a slot not a third over yet: ignored.
	early := bytes.Clone(ssz)
	now := uint64(time.Now().Unix()-int64(gt.cfg.GenesisTime)) / 12
	binary.LittleEndian.PutUint64(early[optimisticFixedSize:], now+5)                                        // attested slot
	binary.LittleEndian.PutUint64(early[4+params.SyncCommitteeBitmaskSize+params.BLSSignatureSize:], now+10) // signature slot
	res, _ = gt.onOptimistic("", "fulu", digest, early)
	expect(t, "early", res, pubsub.ValidationIgnore)
	if gt.nOpt != 2 {
		t.Fatalf("handed on %d optimistic updates, want 2", gt.nOpt)
	}
}
