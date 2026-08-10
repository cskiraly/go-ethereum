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

package eth

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/eth/txtracker"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// TestTrackerObservesPeerDrivenLifecycle is a wire-level integration
// test for the txtracker producer wiring. A source handler pushes a
// signed tx into its pool; a peered sink handler should see the tx
// flow through its tracker reach StatusPooled, and the corresponding
// peer-driven Notify* call sites should fire on the sink's side
// (NotifyAnnounced / NotifyFetchRequested / NotifyReceived /
// NotifyAccepted, depending on whether the source uses direct
// broadcast or hash announcement).
//
// We don't assert the exact set of intermediate states because the
// broadcaster's eth/68 direct-vs-announce choice is non-deterministic
// for a small peer count. We do assert that the sink tracker reaches
// StatusPooled, that the TxInfo.Local flag stays false (this is
// peer-driven, not local), and that the TxInfo.Deliverer matches the
// source peer's enode ID.
func TestTrackerObservesPeerDrivenLifecycle(t *testing.T) {
	t.Parallel()

	source := newTestHandler(ethconfig.FullSync)
	defer source.close()
	sink := newTestHandler(ethconfig.FullSync)
	defer sink.close()
	sink.handler.synced.Store(true) // accept inbound txs

	// Start the sink's tracker so subscriptions deliver events.
	sink.handler.txTracker.Start(sink.chain, nil)
	defer sink.handler.txTracker.Stop()

	stateCh := make(chan txtracker.StateChange, 32)
	stateSub := sink.handler.txTracker.SubscribeStateChanges(stateCh)
	defer stateSub.Unsubscribe()

	// Connect a peer pair using an in-process pipe.
	//
	// Naming mirrors TestTransactionPropagation69: each side of the
	// pipe gets an independent enode.ID, which from the OTHER side's
	// handler is the identity of "the remote peer". sinkRemoteID is
	// what the sink will report as Deliverer, since from the sink's
	// perspective the source delivered the body.
	const protocol = eth.ETH69
	sourcePipe, sinkPipe := p2p.MsgPipe()
	defer sourcePipe.Close()
	defer sinkPipe.Close()

	sourceRemoteID := enode.ID{1} // ID source's handler sees for the sink
	sinkRemoteID := enode.ID{2}   // ID sink's handler sees for the source
	sourcePeer := eth.NewPeer(protocol, p2p.NewPeerPipe(sourceRemoteID, "", nil, sourcePipe), sourcePipe, source.txpool, source.blobpool, nil)
	sinkPeer := eth.NewPeer(protocol, p2p.NewPeerPipe(sinkRemoteID, "", nil, sinkPipe), sinkPipe, sink.txpool, sink.blobpool, nil)
	defer sourcePeer.Close()
	defer sinkPeer.Close()

	go source.handler.runEthPeer(sourcePeer, func(peer *eth.Peer) error {
		return eth.Handle((*ethHandler)(source.handler), peer)
	})
	go sink.handler.runEthPeer(sinkPeer, func(peer *eth.Peer) error {
		return eth.Handle((*ethHandler)(sink.handler), peer)
	})

	// Push a signed tx into the source's pool. The handler's broadcast
	// loop forwards it to the sink, which dispatches through the same
	// case-statements in handler_eth.go that we want to verify.
	tx := types.NewTransaction(0, common.Address{}, big.NewInt(0), 100000, big.NewInt(0), nil)
	tx, _ = types.SignTx(tx, types.HomesteadSigner{}, testKey)

	if errs := source.txpool.Add([]*types.Transaction{tx}, false); errs[0] != nil {
		t.Fatalf("source pool add: %v", errs[0])
	}

	// Wait for the sink's tracker to reach StatusPooled.
	deadline := time.After(5 * time.Second)
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
		case <-deadline:
			t.Fatalf("sink tracker never reached StatusPooled within timeout")
		}
	}

	info := sink.handler.txTracker.GetTx(tx.Hash())
	if info == nil {
		t.Fatal("sink tracker has no TxInfo for the propagated tx")
	}
	if info.Local {
		t.Error("expected Local=false on peer-driven tx")
	}
	if info.Deliverer != sinkRemoteID.String() {
		t.Errorf("Deliverer: got %q, want %q", info.Deliverer, sinkRemoteID.String())
	}
	if info.Status < txtracker.StatusPooled {
		t.Errorf("Status: got %v, want >= StatusPooled", info.Status)
	}
	if info.Pooled.IsZero() {
		t.Error("expected Pooled timestamp to be set")
	}
}
