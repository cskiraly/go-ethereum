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
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common"
)

// jsonHeader is the part of a light client header's beacon API JSON the tests compare.
type jsonHeader struct {
	Beacon struct {
		Slot string `json:"slot"`
	} `json:"beacon"`
	Execution *struct {
		BlockHash common.Hash `json:"block_hash"`
	} `json:"execution"`
	ExecutionBlockHash *common.Hash `json:"execution_block_hash"` // gloas
}

func (h jsonHeader) slot(t *testing.T) uint64 {
	s, err := strconv.ParseUint(h.Beacon.Slot, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (h jsonHeader) hash() common.Hash {
	if h.ExecutionBlockHash != nil {
		return *h.ExecutionBlockHash
	}
	return h.Execution.BlockHash
}

type jsonUpdate struct {
	Version string `json:"version"`
	Data    struct {
		Attested      jsonHeader `json:"attested_header"`
		Finalized     jsonHeader `json:"finalized_header"`
		SignatureSlot string     `json:"signature_slot"`
	} `json:"data"`
}

func load(t *testing.T, name string) ([]byte, jsonUpdate) {
	ssz, err := os.ReadFile("testdata/" + name + ".ssz")
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var u jsonUpdate
	if err := json.Unmarshal(js, &u); err != nil {
		t.Fatal(err)
	}
	return ssz, u
}

func checkHeader(t *testing.T, config *params.ChainConfig, what string, got types.HeaderWithExecProof, want jsonHeader) {
	t.Helper()
	if got.Slot != want.slot(t) {
		t.Errorf("%s slot %d, want %d", what, got.Slot, want.slot(t))
	}
	if got.BlockHash() != want.hash() {
		t.Errorf("%s execution hash %x, want %x", what, got.BlockHash(), want.hash())
	}
	if err := got.Validate(config); err != nil {
		t.Errorf("%s execution proof: %v", what, err)
	}
}

// testConfig returns the chain config of a test network: mainnet's, or for the devnet
// (Gloas well before its updates were taken) one with every fork from genesis.
func testConfig(net string) *params.ChainConfig {
	if net == "mainnet" {
		return params.MainnetLightConfig
	}
	config := new(params.ChainConfig)
	for i, fork := range []string{"GENESIS", "ALTAIR", "BELLATRIX", "CAPELLA", "DENEB", "ELECTRA", "FULU", "GLOAS"} {
		config.AddFork(fork, 0, []byte{byte(i), 0, 0, 0})
	}
	return config
}

// TestDecodeUpdates decodes light client updates served as SSZ by Lodestar (mainnet: Fulu;
// a Kurtosis devnet: Gloas) and compares them with the JSON of the same objects.
func TestDecodeUpdates(t *testing.T) {
	for _, net := range []string{"mainnet", "devnet"} {
		config := testConfig(net)
		ssz, js := load(t, net+"-optimistic_update")
		opt, err := DecodeOptimisticUpdate(js.Version, ssz)
		if err != nil {
			t.Fatalf("%s optimistic (%s): %v", net, js.Version, err)
		}
		checkHeader(t, config, net+" optimistic attested", opt.Attested, js.Data.Attested)
		if s, _ := strconv.ParseUint(js.Data.SignatureSlot, 10, 64); opt.SignatureSlot != s {
			t.Errorf("%s optimistic signature slot %d, want %d", net, opt.SignatureSlot, s)
		}

		ssz, js = load(t, net+"-finality_update")
		fin, err := DecodeFinalityUpdate(js.Version, ssz)
		if err != nil {
			t.Fatalf("%s finality (%s): %v", net, js.Version, err)
		}
		checkHeader(t, config, net+" finality attested", fin.Attested, js.Data.Attested)
		checkHeader(t, config, net+" finality finalized", fin.Finalized, js.Data.Finalized)
	}
}
