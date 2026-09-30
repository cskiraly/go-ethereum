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
	"slices"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// TestOrderCandidates checks that peers listing the protocol come first, peers without
// a known protocol list next, and peers known not to support it not at all.
func TestOrderCandidates(t *testing.T) {
	protos := map[peer.ID][]protocol.ID{
		"serving1": {protoStatus2, protoLCBootstrap},
		"serving2": {protoLCBootstrap},
		"other":    {protoStatus2, protoPing}, // e.g. a node without a light client server
		"unknown":  nil,                       // identify not finished
	}
	peers := []peer.ID{"other", "unknown", "serving1", "serving2"}
	for i := 0; i < 20; i++ { // the order within each group is random
		got := orderCandidates(peers, func(id peer.ID) []protocol.ID { return protos[id] }, protoLCBootstrap)
		if len(got) != 3 || got[2] != "unknown" || !slices.Contains(got[:2], "serving1") || !slices.Contains(got[:2], "serving2") {
			t.Fatalf("got %v, want serving1 and serving2 (any order), then unknown", got)
		}
	}
}
