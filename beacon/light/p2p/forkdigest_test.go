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

// TestSepoliaForkDigest checks Sepolia's current digest and next fork (Gloas), as in the
// ENR of Lodestar's Sepolia node on 2026-10-01: 74d01459 90000076 0x56300.
func TestSepoliaForkDigest(t *testing.T) {
	net := Networks[params.SepoliaLightConfig.GenesisValidatorsRoot]
	const epoch = 350000
	d, _ := ForkDigest(params.SepoliaLightConfig, net.BlobSchedule, net.ElectraBlobs, epoch)
	if got := hex.EncodeToString(d[:]); got != "74d01459" {
		t.Errorf("digest %s, want 74d01459", got)
	}
	v, e := NextFork(params.SepoliaLightConfig, epoch)
	if got := hex.EncodeToString(v[:]); got != "90000076" || e != 353024 {
		t.Errorf("next fork %s at %d, want 90000076 at 353024", got, e)
	}
}
