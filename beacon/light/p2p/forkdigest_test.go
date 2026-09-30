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
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/beacon/params"
)

// TestMainnetForkDigest checks the digests mainnet uses (seen in ENRs and gossip topics).
func TestMainnetForkDigest(t *testing.T) {
	for _, tt := range []struct {
		epoch uint64
		want  string
	}{
		{412672, "cb0d1acc"}, // BPO1
		{479000, "8c9f62fe"}, // BPO2 (current, 2026-09)
	} {
		d, _ := ForkDigest(params.MainnetLightConfig, MainnetBlobSchedule, MainnetElectraBlobs, tt.epoch)
		if got := hex.EncodeToString(d[:]); got != tt.want {
			t.Errorf("epoch %d: digest %s, want %s", tt.epoch, got, tt.want)
		}
	}
}
