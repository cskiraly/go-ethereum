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

package peerstats

import (
	"context"
)

// API exposes per-peer EMA quality stats over JSON-RPC under the
// "peerstats" namespace. Construct with NewAPI(stats) and register
// from Ethereum.APIs() alongside other service APIs.
type API struct {
	stats            *Stats
	protectedSetFunc ProtectedSetFunc
}

// ProtectedSetFunc returns the IDs of peers currently shielded from
// drop by inclusion / latency protection. Optional; nil disables
// the GetProtectedSet RPC (returns an empty map). The dropper in
// eth/dropper.go provides this via its GetProtectedSet method.
type ProtectedSetFunc func() map[string]bool

// NewAPI returns an API bound to the given Stats aggregator.
func NewAPI(stats *Stats) *API {
	return &API{stats: stats}
}

// SetProtectedSetFunc installs a callback that returns the dropper's
// current protected-peer set. After this is called, GetProtectedSet
// returns the live set; before it's called, GetProtectedSet returns
// an empty map. Set once at registration time — the API holds the
// callback for the life of the process.
func (api *API) SetProtectedSetFunc(f ProtectedSetFunc) {
	api.protectedSetFunc = f
}

// GetAllPeerStats returns the current per-peer quality snapshot keyed
// by peer ID. RequestLatencyEMA is encoded as nanoseconds (the
// Go-default time.Duration JSON shape).
func (api *API) GetAllPeerStats(_ context.Context) map[string]PeerStats {
	return api.stats.GetAllPeerStats()
}

// GetPeerStats returns the snapshot for a single peer ID, or nil if
// the peer is not tracked. Cheaper than GetAllPeerStats for UIs that
// only watch a small number of peers.
func (api *API) GetPeerStats(_ context.Context, peer string) *PeerStats {
	all := api.stats.GetAllPeerStats()
	if ps, ok := all[peer]; ok {
		return &ps
	}
	return nil
}

// GetProtectedSet returns the IDs of peers currently shielded from
// the dropper by inclusion / latency protection (the union of
// top-10% per category, see eth/dropper.go protectionCategories).
// The map values are always true; callers can use either the
// keys or `m[id]` for membership checks.
//
// Returns an empty map (never nil) when no protected-set callback
// has been installed (e.g. dropper not yet started). Computed on
// demand — reflects the live peer roster + latest peerstats EMAs
// at call time.
func (api *API) GetProtectedSet(_ context.Context) map[string]bool {
	if api.protectedSetFunc == nil {
		return map[string]bool{}
	}
	return api.protectedSetFunc()
}
