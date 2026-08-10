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

package catalyst

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/txtracker"
	"github.com/ethereum/go-ethereum/miner"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
)

// startNetworkedSimulatedBeaconNode spins up an in-process geth node
// with networking enabled (unlike startSimulatedBeaconEthService which
// uses MaxPeers=0) and a SimulatedBeacon driving block production. The
// caller wires inter-node peering via Server().AddPeer.
func startNetworkedSimulatedBeaconNode(t *testing.T, genesis *core.Genesis, period uint64) (*node.Node, *eth.Ethereum) {
	t.Helper()
	n, err := node.New(&node.Config{
		P2P: p2p.Config{
			ListenAddr:  "127.0.0.1:0",
			NoDiscovery: true,
			MaxPeers:    5,
			NoDial:      false,
		},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	cfg := &ethconfig.Config{
		Genesis:        genesis,
		SyncMode:       ethconfig.FullSync,
		TrieTimeout:    time.Minute,
		TrieDirtyCache: 256,
		TrieCleanCache: 256,
		Miner:          miner.DefaultConfig,
	}
	ethservice, err := eth.New(n, cfg)
	if err != nil {
		t.Fatalf("create eth service: %v", err)
	}
	sim, err := NewSimulatedBeacon(period, common.Address{}, ethservice)
	if err != nil {
		t.Fatalf("create simulated beacon: %v", err)
	}
	n.RegisterLifecycle(sim)
	if err := n.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}
	ethservice.SetSynced()
	return n, ethservice
}

// TestLensdevTwoNodeTxPropagation is the live two-binary smoke test
// the txtracker-wire branch could not run because post-merge geth
// gates BroadcastTransactions on synced=true and only a CL can flip
// that. Here we build two in-process geth nodes, give each a
// SimulatedBeacon (so each marks itself synced and drives its own
// chain), peer them, submit a tx on node A, and assert that node B's
// tracker observes the tx propagating peer-to-peer all the way to
// StatusPooled.
//
// This exercises every wired tracker producer on the *receiving*
// side: NotifyAnnounced (incoming NewPooledTransactionHashes),
// NotifyFetchRequested (sink fetcher requesting body), NotifyReceived
// (incoming PooledTransactions reply), NotifyAccepted (sink pool
// acceptance via the fetcher's onAccepted callback). Reaching
// StatusPooled on node B implies all four fired correctly.
func TestLensdevTwoNodeTxPropagation(t *testing.T) {
	t.Parallel()

	testKey, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	const gasLimit uint64 = 10_000_000
	genesis := core.DeveloperGenesisBlock(gasLimit, &testAddr)

	// Both nodes share the same genesis — required for the eth/68
	// handshake to succeed (genesis-hash check on the wire).
	nodeA, ethA := startNetworkedSimulatedBeaconNode(t, genesis, 1)
	defer nodeA.Close()
	nodeB, ethB := startNetworkedSimulatedBeaconNode(t, genesis, 1)
	defer nodeB.Close()

	// Peer them. Server().AddPeer is the in-process equivalent of
	// admin_addPeer.
	nodeA.Server().AddPeer(nodeB.Server().Self())

	// Wait for the eth/68 handshake to complete — both sides should
	// see one peer once the connection is established.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.Server().PeerCount() > 0 && nodeB.Server().PeerCount() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if nodeA.Server().PeerCount() == 0 || nodeB.Server().PeerCount() == 0 {
		t.Fatalf("peers never connected: A=%d B=%d", nodeA.Server().PeerCount(), nodeB.Server().PeerCount())
	}

	// Subscribe to node B's tracker BEFORE submitting on A so we don't
	// miss the StateChange.
	stateCh := make(chan txtracker.StateChange, 32)
	stateSub := ethB.TxTracker().SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	// Submit a tx on node A. The local-submission path puts it in A's
	// pool; A's broadcast loop forwards to peers (B); B receives via
	// the wired handler paths.
	signer := types.NewEIP155Signer(ethA.BlockChain().Config().ChainID)
	tx, err := types.SignTx(types.NewTransaction(0, common.Address{}, big.NewInt(1), 21000, big.NewInt(1_000_000_000), nil), signer, testKey)
	if err != nil {
		t.Fatalf("sign tx: %v", err)
	}
	if err := ethA.APIBackend.SendTx(context.Background(), tx); err != nil {
		t.Fatalf("submit tx on node A: %v", err)
	}

	// Wait for node B's tracker to observe StatusPooled for the tx.
	deadlineCh := time.After(10 * time.Second)
	reached := false
	for !reached {
		select {
		case ev := <-stateCh:
			if ev.TxHash != tx.Hash() {
				continue
			}
			if ev.NewStatus == txtracker.StatusPooled {
				reached = true
			}
		case <-deadlineCh:
			t.Fatalf("node B tracker never reached StatusPooled for tx %x", tx.Hash())
		}
	}

	info := ethB.TxTracker().GetTx(tx.Hash())
	if info == nil {
		t.Fatal("node B tracker has no TxInfo for the propagated tx")
	}
	if info.Local {
		t.Error("expected Local=false on peer-driven tx")
	}
	if info.Deliverer == "" {
		t.Error("expected Deliverer set on peer-driven tx")
	}
}
