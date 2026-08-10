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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
)

// API exposes the tracker over JSON-RPC under the "txtracker" namespace.
// Construct via NewAPI(tracker) and register from Ethereum.APIs().
type API struct {
	tracker *Tracker
}

// NewAPI returns a new API bound to the given tracker.
func NewAPI(tracker *Tracker) *API {
	return &API{tracker: tracker}
}

// StateChanges streams every Level-2 StateChange the tracker emits
// to the caller via WS/IPC subscription. Invoked from the client as
// txtracker_subscribe("stateChanges"). The subscription terminates
// when the client disconnects or unsubscribes.
//
// Notifications carry a JSON array of StateChange values rather than a
// single value: producer-side calls like NotifyAnnounced naturally
// arrive as batches (a single eth/68 NewPooledTransactionHashes frame
// commonly carries hundreds of hashes), and the forwarder opportunistically
// drains anything queued on the per-sub channel so a burst lands as one
// JSON-RPC notification + one WS frame instead of one-per-event. At idle
// rates the channel is empty after the first read and each batch contains
// a single element.
//
// The tracker drops events on its internal emit buffer if the consumer
// is too slow (counted by stateDropped on the tracker); on the RPC
// side, a slow client is bounded by the RPC notifier's send buffer
// and may see fewer events than the tracker emitted. Use
// txtracker_diagnostics to observe drop counters.
func (api *API) StateChanges(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()
	ch := make(chan StateChange, 4096)
	sub := api.tracker.SubscribeStateChanges(ch)

	go func() {
		defer sub.Unsubscribe()
		for {
			select {
			case ev := <-ch:
				batch := []StateChange{ev}
			drain:
				for len(batch) < 1024 {
					select {
					case ev2 := <-ch:
						batch = append(batch, ev2)
					default:
						break drain
					}
				}
				notifier.Notify(rpcSub.ID, batch)
			case <-rpcSub.Err():
				return
			case <-sub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// Observations streams every Level-1 Observation. Invoked from the
// client as txtracker_subscribe("observations"). Same backpressure
// and batching semantics as StateChanges — notifications carry an
// array of Observation values.
func (api *API) Observations(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()
	ch := make(chan Observation, 8192)
	sub := api.tracker.SubscribeObservations(ch)

	go func() {
		defer sub.Unsubscribe()
		for {
			select {
			case ev := <-ch:
				batch := []Observation{ev}
			drain:
				for len(batch) < 2048 {
					select {
					case ev2 := <-ch:
						batch = append(batch, ev2)
					default:
						break drain
					}
				}
				notifier.Notify(rpcSub.ID, batch)
			case <-rpcSub.Err():
				return
			case <-sub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// GetTx returns the current tracked snapshot for hash, or nil if the
// hash is not tracked. The returned TxInfo is a deep-copied snapshot
// (pointer fields are independent of tracker state).
func (api *API) GetTx(_ context.Context, hash common.Hash) *TxInfo {
	return api.tracker.GetTx(hash)
}

// Diagnostics describes per-feed drop counters and FIFO-eviction
// stats for the tracker. A non-zero ObsDropped/StateDropped indicates
// subscribers (or the tracker's own internal forwarder) could not
// keep up with the producer rate at least once since startup.
// EvictedByStatus is keyed by the lowercase status label and counts
// txs that left the hot map at that status because the map exceeded
// maxTracked; zero buckets are omitted.
type Diagnostics struct {
	ObsDropped      uint64            `json:"obsDropped"`
	StateDropped    uint64            `json:"stateDropped"`
	EvictedByStatus map[string]uint64 `json:"evictedByStatus,omitempty"`
}

// Diagnostics returns the current per-feed drop counters and the
// per-status FIFO-eviction histogram.
func (api *API) Diagnostics(_ context.Context) Diagnostics {
	hist := api.tracker.EvictedByStatus()
	out := make(map[string]uint64)
	for status, count := range hist {
		if count == 0 {
			continue
		}
		out[TxStatus(status).String()] = count
	}
	return Diagnostics{
		ObsDropped:      api.tracker.ObsDropped(),
		StateDropped:    api.tracker.StateDropped(),
		EvictedByStatus: out,
	}
}

// CaptureInfo returns the tracker's capture-sink state. When
// `--txtracker.capture <path>` was set on geth startup, the
// returned struct describes the active NDJSON file (path, size,
// rotation history) plus drop counters specific to the capture
// goroutine. When capture is disabled, only Enabled=false is set.
//
// Lens reports embed this so a diagnostician can locate the
// corresponding NDJSON log on disk and grep for the tx hash.
func (api *API) CaptureInfo(_ context.Context) CaptureInfo {
	return api.tracker.CaptureInfo()
}
