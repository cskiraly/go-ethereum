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
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
)

// TxTrackerAPI provides RPC access to the transaction lifecycle tracker.
type TxTrackerAPI struct {
	tracker *txtracker.Tracker
	server  *p2p.Server
}

// NewTxTrackerAPI creates a new TxTrackerAPI.
func NewTxTrackerAPI(tracker *txtracker.Tracker, server *p2p.Server) *TxTrackerAPI {
	return &TxTrackerAPI{tracker: tracker, server: server}
}

// GetTx returns lifecycle info for a tracked transaction.
func (api *TxTrackerAPI) GetTx(hash common.Hash) *txtracker.TxInfo {
	return api.tracker.Get(hash)
}

// GetPeerStats returns peer contribution statistics.
func (api *TxTrackerAPI) GetPeerStats(peer string) txtracker.PeerStats {
	return api.tracker.GetPeerStats(peer)
}

// GetStats returns tracker-wide statistics including eviction counters.
func (api *TxTrackerAPI) GetStats() txtracker.TrackerStats {
	return api.tracker.GetStats()
}

// GetAllPeerStats returns contribution statistics for all connected peers.
func (api *TxTrackerAPI) GetAllPeerStats() map[string]txtracker.PeerStats {
	return api.tracker.GetAllPeerStats()
}

// PeerFullInfo combines tracker statistics with P2P identity for a peer.
type PeerFullInfo struct {
	txtracker.PeerStats
	Name    string `json:"name,omitempty"`
	Address string `json:"address,omitempty"`
	Inbound bool   `json:"inbound,omitempty"`
}

// GetAllPeerInfo returns tracker stats merged with P2P identity for all peers.
func (api *TxTrackerAPI) GetAllPeerInfo() map[string]*PeerFullInfo {
	stats := api.tracker.GetAllPeerStats()
	result := make(map[string]*PeerFullInfo, len(stats))
	for id, s := range stats {
		result[id] = &PeerFullInfo{PeerStats: s}
	}
	// Merge P2P identity from connected peers. Also include peers that
	// have no tracker stats yet (just connected, or no tx activity).
	if api.server != nil {
		for _, p := range api.server.PeersInfo() {
			fi, ok := result[p.ID]
			if !ok {
				fi = &PeerFullInfo{}
				result[p.ID] = fi
			}
			fi.Name = p.Name
			fi.Address = p.Network.RemoteAddress
			fi.Inbound = p.Network.Inbound
		}
	}
	return result
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
				if err := notifier.Notify(rpcSub.ID, ev); err != nil {
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
