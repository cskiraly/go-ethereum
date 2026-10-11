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

package blsync

import (
	"fmt"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/rpc"
)

// gloasEngine is a fakeEngine that answers Gloas's engine API methods as the earlier ones.
type gloasEngine struct{ *fakeEngine }

func (g gloasEngine) ForkchoiceUpdatedV4(update engine.ForkchoiceStateV1, attr *engine.PayloadAttributes) (engine.ForkChoiceResponse, error) {
	return g.ForkchoiceUpdatedV3(update, attr)
}

func (g gloasEngine) NewPayloadV5(data engine.ExecutableData, hashes []common.Hash, root *common.Hash, requests []string) (engine.PayloadStatusV1, error) {
	return g.NewPayloadV4(data, hashes, root, requests)
}

// newGloasEngineTest is newEngineTest (with P2PBlocks) on a chain whose test slots are
// after Gloas.
func newGloasEngineTest(t *testing.T) *engineTest {
	clock := &timerClock{Simulated: new(mclock.Simulated)}
	et := &engineTest{
		t:         t,
		clock:     clock,
		fake:      &fakeEngine{clock: clock, fcuStatus: engine.SYNCING},
		headCh:    make(chan types.ChainHeadEvent, 4),
		p2pBlocks: true,
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName("engine", gloasEngine{et.fake}); err != nil {
		t.Fatal(err)
	}
	client := rpc.DialInProc(srv)
	chain := (&params.ChainConfig{}).
		AddFork("GENESIS", 0, []byte{0, 0, 0, 0}).
		AddFork("ELECTRA", 0, []byte{5, 0, 0, 0}).
		AddFork("GLOAS", 364032, []byte{7, 0, 0, 0})
	config := &params.ClientConfig{ChainConfig: *chain}
	ec := startEngineClientWithClock(config, client, et.headCh, et.fetchBlock, clock)
	t.Cleanup(func() {
		ec.stop()
		client.Close()
		srv.Stop()
	})
	return et
}

// TestEngineClientFallbackGloas tests the fallback from Gloas on, where a head's light client
// header proves its parent's payload: the head's block is fetched from the parent beacon
// block, and when blsync skipped a head, the missing block before it too, back to the
// latest VALID head's payload, which is a slot before that head.
func TestEngineClientFallbackGloas(t *testing.T) {
	et := newGloasEngineTest(t)
	chain, byRoot := testChain(6, 0)
	et.chain = byRoot
	et.fake.known = map[common.Hash]bool{chain[0].ExecHash: true, chain[1].ExecHash: true}

	// headAt sends the head of chain[i]'s beacon block, which names chain[i-1]'s payload
	headAt := func(i int) common.Hash {
		hash := chain[i-1].ExecHash
		et.headCh <- types.ChainHeadEvent{
			BeaconHead: chain[i].BeaconHead,
			ExecHash:   hash,
			Finalized:  common.Hash{0xf, hash[0]},
		}
		return hash
	}
	headAt(2) // VALID: names chain[1]'s payload
	et.wait(1, 0, 0)
	head := headAt(4) // names chain[3]'s; the head naming chain[2]'s was skipped
	et.wait(2, 0, 0)
	et.fallback()
	et.wait(6, 2, 2)
	et.expectNoRepeat()
	et.expectFcu(head, 0, 1, 2, 4, 4)
	if have, want := et.fetched(), []common.Hash{chain[3].ExecHash, chain[2].ExecHash}; !slices.Equal(have, want) {
		t.Fatalf("fetched %x, want %x", have, want)
	}
	if have, want := et.sent(), []common.Hash{chain[2].ExecHash, chain[3].ExecHash}; !slices.Equal(have, want) {
		t.Fatalf("newPayload order %x, want %x", have, want)
	}
	et.settle(6, 2, 2)
}

// TestEngineClientFallbackGloasDepth tests that from Gloas on, too, blocks are fetched back
// to the latest VALID head's payload only if at most p2pFallbackDepth slots lie between it
// and the head's payload (each a slot before its head).
func TestEngineClientFallbackGloasDepth(t *testing.T) {
	for _, gap := range []int{p2pFallbackDepth, p2pFallbackDepth + 1} {
		t.Run(fmt.Sprint(gap), func(t *testing.T) {
			et := newGloasEngineTest(t)
			chain, byRoot := testChain(gap+3, 0)
			et.chain = byRoot
			et.fake.known = map[common.Hash]bool{chain[0].ExecHash: true}

			headAt := func(i int) {
				et.headCh <- types.ChainHeadEvent{BeaconHead: chain[i].BeaconHead, ExecHash: chain[i-1].ExecHash}
			}
			headAt(1) // VALID: names chain[0]'s payload
			et.wait(1, 0, 0)
			headAt(gap + 2) // names chain[gap+1]'s: gap slots between the payloads
			et.wait(2, 0, 0)
			et.fallback()
			if gap <= p2pFallbackDepth {
				et.wait(6, gap+1, gap+1)
				et.settle(6, gap+1, gap+1)
				return
			}
			et.wait(6, 1, 1) // only the head's block: paused
			et.head(9)
			et.wait(7, 1, 1)
			et.fallback()
			et.wait(10, 1, 1)
		})
	}
}
