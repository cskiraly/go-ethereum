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
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/txtracker"
	"github.com/ethereum/go-ethereum/rpc"
)

// TxTrackerAPI provides RPC access to the transaction lifecycle tracker.
type TxTrackerAPI struct {
	tracker *txtracker.Tracker
}

// NewTxTrackerAPI creates a new TxTrackerAPI.
func NewTxTrackerAPI(tracker *txtracker.Tracker) *TxTrackerAPI {
	return &TxTrackerAPI{tracker: tracker}
}

// GetTx returns lifecycle info for a tracked transaction.
func (api *TxTrackerAPI) GetTx(hash common.Hash) *txtracker.TxInfo {
	return api.tracker.Get(hash)
}

// GetPeerStats returns peer contribution statistics.
func (api *TxTrackerAPI) GetPeerStats(peer string) txtracker.PeerStats {
	return api.tracker.GetPeerStats(peer)
}

// Events creates a subscription for live state transition events.
func (api *TxTrackerAPI) Events(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		events := make(chan txtracker.TxTrackerEvent, 128)
		sub := api.tracker.SubscribeEvents(events)
		defer sub.Unsubscribe()

		for {
			select {
			case ev := <-events:
				notifier.Notify(rpcSub.ID, ev)
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
