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
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/beacon/params"
)

func hexDigests(ds [][4]byte) []string {
	var s []string
	for _, d := range ds {
		s = append(s, hex.EncodeToString(d[:]))
	}
	slices.Sort(s)
	return s
}

// TestSepoliaGloasTransition checks the digest and subscriptions around Sepolia's Gloas
// fork (epoch 353024): the new topics from 2 epochs before, the old ones until 2 after.
func TestSepoliaGloasTransition(t *testing.T) {
	sched := digestSchedule(params.SepoliaLightConfig, Networks[params.SepoliaLightConfig.GenesisValidatorsRoot])
	const (
		fulu  = "74d01459" // Fulu with BPO2
		gloas = "669e6c11"
	)
	for _, tt := range []struct {
		epoch   uint64
		current string
		fork    string
		want    []string
	}{
		{353021, fulu, "fulu", []string{fulu}},
		{353022, fulu, "fulu", []string{gloas, fulu}},
		{353023, fulu, "fulu", []string{gloas, fulu}},
		{353024, gloas, "gloas", []string{gloas, fulu}},
		{353025, gloas, "gloas", []string{gloas, fulu}},
		{353026, gloas, "gloas", []string{gloas}},
	} {
		cur, want := topicsAt(sched, tt.epoch)
		if got := hex.EncodeToString(cur.digest[:]); got != tt.current || cur.fork != tt.fork {
			t.Errorf("epoch %d: current %s (%s), want %s (%s)", tt.epoch, got, cur.fork, tt.current, tt.fork)
		}
		if got := hexDigests(want); !slices.Equal(got, tt.want) {
			t.Errorf("epoch %d: topics %v, want %v", tt.epoch, got, tt.want)
		}
	}
}

// TestHoodiGloasTransition checks the digest and subscriptions around Hoodi's Gloas fork (epoch
// 132352). The Gloas digest is computed (fork data root of 0x80000910 XOR BPO2's parameters), not
// yet seen in a live record on 2026-10-10.
func TestHoodiGloasTransition(t *testing.T) {
	sched := digestSchedule(params.HoodiLightConfig, Networks[params.HoodiLightConfig.GenesisValidatorsRoot])
	const (
		fulu  = "c6ecb76c" // Fulu with BPO2
		gloas = "5ad30129"
	)
	for _, tt := range []struct {
		epoch   uint64
		current string
		fork    string
		want    []string
	}{
		{132349, fulu, "fulu", []string{fulu}},
		{132350, fulu, "fulu", []string{gloas, fulu}},
		{132352, gloas, "gloas", []string{gloas, fulu}},
		{132354, gloas, "gloas", []string{gloas}},
	} {
		cur, want := topicsAt(sched, tt.epoch)
		if got := hex.EncodeToString(cur.digest[:]); got != tt.current || cur.fork != tt.fork {
			t.Errorf("epoch %d: current %s (%s), want %s (%s)", tt.epoch, got, cur.fork, tt.current, tt.fork)
		}
		if got := hexDigests(want); !slices.Equal(got, tt.want) {
			t.Errorf("epoch %d: topics %v, want %v", tt.epoch, got, tt.want)
		}
	}
}

// TestMainnetDigestSchedule checks that the blob parameter changes (BPO1, BPO2) are digest
// changes of their own, within Fulu.
func TestMainnetDigestSchedule(t *testing.T) {
	sched := digestSchedule(params.MainnetLightConfig, Networks[params.MainnetLightConfig.GenesisValidatorsRoot])
	var got []string
	for _, fd := range sched {
		if fd.epoch >= 411392 { // Fulu
			got = append(got, fd.fork+":"+hex.EncodeToString(fd.digest[:]))
		}
	}
	if len(got) != 3 || got[1] != "fulu:cb0d1acc" || got[2] != "fulu:8c9f62fe" {
		t.Errorf("Fulu digests %v, want Fulu, then BPO1 cb0d1acc and BPO2 8c9f62fe", got)
	}
}

// TestParseNetwork reads a blob schedule from a beacon chain config file.
func TestParseNetwork(t *testing.T) {
	net, err := ParseNetwork([]byte(`
ELECTRA_FORK_EPOCH: 0
MAX_BLOBS_PER_BLOCK_ELECTRA: 9
BLOB_SCHEDULE:
  - EPOCH: 0
    MAX_BLOBS_PER_BLOCK: 21
  - EPOCH: 1200
    MAX_BLOBS_PER_BLOCK: 48
`))
	if err != nil {
		t.Fatal(err)
	}
	if net.ElectraBlobs != (BlobParams{0, 9}) || !slices.Equal(net.BlobSchedule, []BlobParams{{0, 21}, {1200, 48}}) {
		t.Errorf("got %+v", net)
	}
}
