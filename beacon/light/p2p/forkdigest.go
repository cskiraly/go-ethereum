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
	"crypto/sha256"
	"encoding/binary"
	"math"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum/go-ethereum/beacon/params"
)

// BlobParams is an entry of the blob schedule (EIP-7892): from Epoch on, blocks carry at
// most MaxBlobs blobs. From Fulu on, the fork digest mixes in the current entry.
type BlobParams struct{ Epoch, MaxBlobs uint64 }

// MainnetBlobSchedule is mainnet's BLOB_SCHEDULE (BPO1, BPO2), and the Electra defaults.
var (
	MainnetBlobSchedule = []BlobParams{{412672, 15}, {419072, 21}}
	MainnetElectraBlobs = BlobParams{364032, 9}
)

// Network holds what the light client needs to know of a network beyond its beacon chain
// config: where to start discovery, and the blob schedule (for the fork digest).
type Network struct {
	Name         string
	Bootnodes    []string
	BlobSchedule []BlobParams
	ElectraBlobs BlobParams // MAX_BLOBS_PER_BLOCK_ELECTRA from ELECTRA_FORK_EPOCH
}

// Networks are the built-in networks, by genesis validators root.
var Networks = map[common.Hash]Network{
	params.MainnetLightConfig.GenesisValidatorsRoot: {
		Name: "mainnet", Bootnodes: MainnetBootnodes,
		BlobSchedule: MainnetBlobSchedule, ElectraBlobs: MainnetElectraBlobs,
	},
	params.SepoliaLightConfig.GenesisValidatorsRoot: {
		Name: "sepolia", Bootnodes: SepoliaBootnodes,
		BlobSchedule: []BlobParams{{274176, 15}, {275712, 21}}, ElectraBlobs: BlobParams{222464, 9},
	},
	params.HoodiLightConfig.GenesisValidatorsRoot: {
		Name: "hoodi", Bootnodes: HoodiBootnodes,
		BlobSchedule: []BlobParams{{52480, 15}, {54016, 21}}, ElectraBlobs: BlobParams{2048, 9},
	},
}

// NextFork returns the version and epoch of the first fork after epoch in the config, for
// the ENR's `eth2` entry (the current version and FAR_FUTURE_EPOCH if there is none).
func NextFork(config *params.ChainConfig, epoch uint64) (version [4]byte, forkEpoch uint64) {
	copy(version[:], config.ForkAtEpoch(epoch).Version)
	forkEpoch = math.MaxUint64
	for _, f := range config.Forks {
		if f.Epoch > epoch && f.Epoch < forkEpoch {
			forkEpoch = f.Epoch
			copy(version[:], f.Version)
		}
	}
	return version, forkEpoch
}

// ForkDigest computes the fork digest at an epoch: the first 4 bytes of the fork data root
// (fork version, genesis validators root), from Fulu on XORed with the hash of the blob
// parameters in force.
func ForkDigest(config *params.ChainConfig, schedule []BlobParams, electra BlobParams, epoch uint64) (digest, version [4]byte) {
	fork := config.ForkAtEpoch(epoch)
	copy(version[:], fork.Version)
	var data [64]byte
	copy(data[:4], fork.Version)
	copy(data[32:], config.GenesisValidatorsRoot[:])
	base := sha256.Sum256(data[:])
	if forkIndex(fork.Name) >= forkIndex("fulu") {
		bp := electra
		for _, p := range schedule {
			if p.Epoch <= epoch && p.Epoch >= bp.Epoch {
				bp = p
			}
		}
		var b [16]byte
		binary.LittleEndian.PutUint64(b[0:8], bp.Epoch)
		binary.LittleEndian.PutUint64(b[8:16], bp.MaxBlobs)
		h := sha256.Sum256(b[:])
		for i := range base {
			base[i] ^= h[i]
		}
	}
	copy(digest[:], base[:4])
	return digest, version
}

var forkOrder = []string{"genesis", "altair", "bellatrix", "capella", "deneb", "electra", "fulu", "gloas"}

func forkIndex(name string) int {
	for i, n := range forkOrder {
		if strings.EqualFold(n, name) {
			return i
		}
	}
	return -1
}
