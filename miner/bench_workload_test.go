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

package miner

import (
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"github.com/holiman/uint256"
	"math/big"
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/params"
)

// Contracts used by the synthetic workloads.
var (
	// benchTokenContract moves one unit from the caller to the address in the
	// first calldata word. Balances live in the slot named by the address:
	// sstore(caller, sload(caller)-1); sstore(to, sload(to)+1)
	benchTokenContract = common.HexToAddress("0x00000000000000000000000000000000000b0000")
	benchTokenCode     = common.FromHex("0x33546001900333556000358054600101905500")

	// benchCounterContract increments slot 0: every call conflicts.
	benchCounterContract = common.HexToAddress("0x00000000000000000000000000000000000c0000")
	benchCounterCode     = common.FromHex("0x60016000540160005500")

	// benchComputeContract hashes a memory word 2000 times (~150k gas) and
	// touches no storage.
	benchComputeContract = common.HexToAddress("0x00000000000000000000000000000000000d0000")
	benchComputeCode     = common.FromHex("0x6107d05b60206000206000526001900380600357" + "00")

	// benchCoinbaseRevertContract sends 1 wei to the coinbase and then
	// reverts: call(gas, coinbase, 1, 0, 0, 0, 0); revert(0, 0)
	benchCoinbaseRevertContract = common.HexToAddress("0x00000000000000000000000000000000000e0000")
	benchCoinbaseRevertCode     = common.FromHex("0x6000600060006000600141" + "5af1" + "50" + "60006000fd")
)

const (
	benchBlockGas   = 60_000_000
	benchMaxBlobs   = 9 // blobs per block in the test chain's blob schedule (Prague's)
	benchTokenGas   = 60_000
	benchCounterGas = 60_000
	benchComputeGas = 200_000

	// Rough gas each candidate actually uses, for sizing the candidate set.
	benchTransferUse = params.TxGas
	benchTokenUse    = 32_000
	benchCounterUse  = 27_000
	benchComputeUse  = 170_000
)

// benchWorkload is a deterministic synthetic block-building input.
type benchWorkload struct {
	name        string
	description string
	generate    func(g *benchGen)
}

var benchWorkloads = []benchWorkload{
	{"transfer", "ETH transfers between distinct existing accounts, no conflicts", func(g *benchGen) {
		for gas := uint64(0); gas < g.candidateGas; gas += benchTransferUse {
			g.transfer(g.newSender(), g.newAccount())
		}
	}},
	{"transfer-hot", "ETH transfers from distinct senders to one recipient", func(g *benchGen) {
		hot := g.newAccount()
		for gas := uint64(0); gas < g.candidateGas; gas += benchTransferUse {
			g.transfer(g.newSender(), hot)
		}
	}},
	{"chains", "200 senders with long nonce chains of transfers", func(g *benchGen) {
		senders := make([]int, 200)
		for i := range senders {
			senders[i] = g.newSender()
		}
		for gas := uint64(0); gas < g.candidateGas; {
			for _, s := range senders {
				g.transfer(s, g.newAccount())
				gas += benchTransferUse
			}
		}
	}},
	{"token", "token transfers, recipients uniform over all holders (low contention)", func(g *benchGen) {
		n := int(g.candidateGas / benchTokenUse)
		senders := g.tokenHolders(n)
		for _, s := range senders {
			g.tokenTransfer(s, g.addrs[senders[g.rng.Intn(len(senders))]])
		}
	}},
	{"token-hot", "token transfers to a hot set of 8 recipients", func(g *benchGen) {
		n := int(g.candidateGas / benchTokenUse)
		senders := g.tokenHolders(n)
		hot := senders[:8]
		for _, s := range senders {
			g.tokenTransfer(s, g.addrs[hot[g.rng.Intn(len(hot))]])
		}
	}},
	{"counter", "every tx increments the same storage slot (inherently serial)", func(g *benchGen) {
		g.deploy(benchCounterContract, benchCounterCode, nil)
		for gas := uint64(0); gas < g.candidateGas; gas += benchCounterUse {
			g.call(g.newSender(), benchCounterContract, nil, benchCounterGas)
		}
	}},
	{"compute", "CPU-heavy calls touching no shared state", func(g *benchGen) {
		g.deploy(benchComputeContract, benchComputeCode, nil)
		for gas := uint64(0); gas < g.candidateGas; gas += benchComputeUse {
			g.call(g.newSender(), benchComputeContract, nil, benchComputeGas)
		}
	}},
	{"coinbase-revert", "calls that pay the coinbase in a call that reverts, among transfers", func(g *benchGen) {
		g.alloc[benchCoinbaseRevertContract] = types.Account{Nonce: 1, Code: benchCoinbaseRevertCode, Balance: benchFunds}
		for gas := uint64(0); gas < g.candidateGas; {
			if g.rng.Intn(4) == 0 {
				g.call(g.newSender(), benchCoinbaseRevertContract, nil, 100_000)
				gas += 40_000
			} else {
				g.transfer(g.newSender(), g.newAccount())
				gas += benchTransferUse
			}
		}
	}},
	{"create", "senders deploying contracts in nonce chains, some destroyed in their constructor, among transfers", func(g *benchGen) {
		for gas := uint64(0); gas < g.candidateGas; {
			s := g.newSender()
			for range 1 + g.rng.Intn(4) {
				if g.rng.Intn(3) == 0 {
					g.create(s, benchDestroyInit)
				} else {
					g.create(s, benchStoreInit)
				}
				gas += benchCreateUse
			}
			g.transfer(g.newSender(), g.newAccount())
			gas += benchTransferUse
		}
	}},
	{"beacon-read", "calls storing the beacon root that the block's system call writes, among transfers", func(g *benchGen) {
		g.deploy(benchBeaconReader, benchBeaconReaderCode, nil)
		for gas := uint64(0); gas < g.candidateGas; {
			if g.rng.Intn(3) == 0 {
				g.call(g.newSender(), benchBeaconReader, nil, 100_000)
				gas += 50_000
			} else {
				g.transfer(g.newSender(), g.newAccount())
				gas += benchTransferUse
			}
		}
	}},
	{"blobs", "transfers with 3 blob transactions of 3 blobs each per block of candidates, as on mainnet", func(g *benchGen) {
		inbox := g.newAccount()
		for range max(1, int(3*g.blocks())) {
			g.blobTx(g.newSender(), inbox, 3)
		}
		for gas := uint64(0); gas < g.candidateGas; gas += benchTransferUse {
			g.transfer(g.newSender(), g.newAccount())
		}
	}},
	{"blob-many", "transfers with twice as many single-blob transactions as blocks hold blobs", func(g *benchGen) {
		inbox := g.newAccount()
		for range max(2, int(2*benchMaxBlobs*g.blocks())) {
			g.blobTx(g.newSender(), inbox, 1)
		}
		for gas := uint64(0); gas < g.candidateGas; gas += benchTransferUse {
			g.transfer(g.newSender(), g.newAccount())
		}
	}},
	{"mixed", "50% transfers, 30% token, 10% token-hot, 10% compute", func(g *benchGen) {
		g.deploy(benchComputeContract, benchComputeCode, nil)
		holders := g.tokenHolders(2000)
		hot := holders[:8]
		for gas := uint64(0); gas < g.candidateGas; {
			switch k := g.rng.Intn(10); {
			case k < 5:
				g.transfer(g.newSender(), g.newAccount())
				gas += benchTransferUse
			case k < 8:
				g.tokenTransfer(holders[g.rng.Intn(len(holders))], g.addrs[holders[g.rng.Intn(len(holders))]])
				gas += benchTokenUse
			case k < 9:
				g.tokenTransfer(holders[g.rng.Intn(len(holders))], g.addrs[hot[g.rng.Intn(len(hot))]])
				gas += benchTokenUse
			default:
				g.call(g.newSender(), benchComputeContract, nil, benchComputeGas)
				gas += benchComputeUse
			}
		}
	}},
}

func findBenchWorkload(name string) (benchWorkload, error) {
	for _, w := range benchWorkloads {
		if w.name == name {
			return w, nil
		}
	}
	return benchWorkload{}, fmt.Errorf("unknown workload %q", name)
}

// benchGen accumulates the genesis allocation and candidate transactions of a
// workload. Everything is derived from the seed.
type benchGen struct {
	config *params.ChainConfig
	signer types.Signer
	rng    *rand.Rand
	seed   int64

	candidateGas uint64 // approximate gas of the candidate set to generate

	alloc  types.GenesisAlloc
	keys   []*ecdsa.PrivateKey // nil for accounts nobody signs for
	addrs  []common.Address
	nonces []uint64
	txs    []*types.Transaction
}

func newBenchGen(config *params.ChainConfig, seed int64, candidateGas uint64) *benchGen {
	return &benchGen{
		config:       config,
		candidateGas: candidateGas,
		signer:       types.LatestSigner(config),
		rng:          rand.New(rand.NewSource(seed)),
		seed:         seed,
		alloc:        make(types.GenesisAlloc),
	}
}

var benchFunds = new(big.Int).Mul(big.NewInt(1000), big.NewInt(params.Ether))

// newSender creates a funded account with a key and returns its index.
func (g *benchGen) newSender() int {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(g.seed))
	binary.BigEndian.PutUint64(buf[8:], uint64(len(g.addrs)))
	key, err := crypto.ToECDSA(crypto.Keccak256(buf[:]))
	if err != nil {
		panic(err)
	}
	addr := crypto.PubkeyToAddress(key.PublicKey)
	g.alloc[addr] = types.Account{Balance: benchFunds}
	g.keys = append(g.keys, key)
	g.addrs = append(g.addrs, addr)
	g.nonces = append(g.nonces, 0)
	return len(g.addrs) - 1
}

// newAccount creates an existing keyless account and returns its address.
func (g *benchGen) newAccount() common.Address {
	var buf [17]byte
	buf[0] = 0xac
	binary.BigEndian.PutUint64(buf[1:9], uint64(g.seed))
	binary.BigEndian.PutUint64(buf[9:], uint64(len(g.alloc)))
	addr := common.BytesToAddress(crypto.Keccak256(buf[:]))
	g.alloc[addr] = types.Account{Balance: big.NewInt(1)}
	return addr
}

func (g *benchGen) deploy(addr common.Address, code []byte, storage map[common.Hash]common.Hash) {
	g.alloc[addr] = types.Account{Nonce: 1, Code: code, Storage: storage}
}

// tokenHolders deploys the token contract with n funded holders, who are also
// senders, and returns their indices.
func (g *benchGen) tokenHolders(n int) []int {
	storage := make(map[common.Hash]common.Hash, n)
	holders := make([]int, n)
	for i := range holders {
		holders[i] = g.newSender()
		storage[common.BytesToHash(g.addrs[holders[i]].Bytes())] = common.BigToHash(big.NewInt(1_000_000))
	}
	g.deploy(benchTokenContract, benchTokenCode, storage)
	return holders
}

// sign creates the next transaction of sender with a random tip, so that the
// price ordering interleaves senders.
func (g *benchGen) sign(sender int, to common.Address, value *big.Int, data []byte, gas uint64) {
	tip := big.NewInt(int64(1+g.rng.Intn(20)) * params.GWei / 2)
	if value == nil {
		value = new(big.Int)
	}
	tx := types.MustSignNewTx(g.keys[sender], g.signer, &types.DynamicFeeTx{
		ChainID:   g.config.ChainID,
		Nonce:     g.nonces[sender],
		GasTipCap: tip,
		GasFeeCap: big.NewInt(100 * params.GWei),
		Gas:       gas,
		To:        &to,
		Value:     value,
		Data:      data,
	})
	g.nonces[sender]++
	g.txs = append(g.txs, tx)
}

// create signs a contract creation running init.
func (g *benchGen) create(sender int, init []byte) {
	tx := types.MustSignNewTx(g.keys[sender], g.signer, &types.DynamicFeeTx{
		ChainID:   g.config.ChainID,
		Nonce:     g.nonces[sender],
		GasTipCap: big.NewInt(int64(1+g.rng.Intn(20)) * params.GWei / 2),
		GasFeeCap: big.NewInt(100 * params.GWei),
		Gas:       benchCreateGas,
		Data:      init,
	})
	g.nonces[sender]++
	g.txs = append(g.txs, tx)
}

// blocks returns how many blocks' worth of gas the candidate set is.
func (g *benchGen) blocks() float64 {
	return float64(g.candidateGas) / benchBlockGas
}

// blobTx signs a blob transaction carrying blobs random blobs to to, with a
// version 1 sidecar (cell proofs), as the blob pool takes them since Osaka.
func (g *benchGen) blobTx(sender int, to common.Address, blobs int) {
	sidecar := benchBlobSidecar(g.rng, blobs)
	tx := types.MustSignNewTx(g.keys[sender], g.signer, &types.BlobTx{
		ChainID: uint256.MustFromBig(g.config.ChainID),
		Nonce:   g.nonces[sender],
		// a tip no other transaction has: the blob pool stamps arrival times
		// itself, so equal tips could be ordered differently on every build
		GasTipCap:  uint256.NewInt(uint64(1+g.rng.Intn(20))*params.GWei/2 + uint64(len(g.txs)) + 1),
		GasFeeCap:  uint256.NewInt(100 * params.GWei),
		Gas:        50_000,
		To:         to,
		Value:      new(uint256.Int),
		BlobFeeCap: uint256.NewInt(params.GWei),
		BlobHashes: sidecar.BlobHashes(),
		Sidecar:    sidecar,
	})
	g.nonces[sender]++
	g.txs = append(g.txs, tx)
}

// benchBlobSidecar returns a sidecar of n random blobs with their commitments
// and cell proofs. Computing the proofs takes a while; it is done once per case.
func benchBlobSidecar(rng *rand.Rand, n int) *types.BlobTxSidecar {
	var (
		blobs       = make([]kzg4844.Blob, n)
		commitments []kzg4844.Commitment
		proofs      []kzg4844.Proof
	)
	for i := range blobs {
		// every 32-byte field element starts with a zero byte, so it is below
		// the field modulus
		for j := 0; j < len(blobs[i]); j += 32 {
			rng.Read(blobs[i][j+1 : j+32])
		}
		c, err := kzg4844.BlobToCommitment(&blobs[i])
		if err != nil {
			panic(err)
		}
		p, err := kzg4844.ComputeCellProofs(&blobs[i])
		if err != nil {
			panic(err)
		}
		commitments = append(commitments, c)
		proofs = append(proofs, p...)
	}
	return types.NewBlobTxSidecar(types.BlobSidecarVersion1, blobs, commitments, proofs)
}

func (g *benchGen) transfer(sender int, to common.Address) {
	g.sign(sender, to, big.NewInt(1), nil, params.TxGas)
}

func (g *benchGen) tokenTransfer(sender int, to common.Address) {
	g.sign(sender, benchTokenContract, nil, common.BytesToHash(to.Bytes()).Bytes(), benchTokenGas)
}

func (g *benchGen) call(sender int, to common.Address, data []byte, gas uint64) {
	g.sign(sender, to, nil, data, gas)
}

const (
	benchCreateGas = 120_000
	benchCreateUse = 80_000
)

var (
	// benchStoreInit stores 1 in slot 0 and deploys a one-byte contract.
	benchStoreInit = common.FromHex("600160005560016000f3")
	// benchDestroyInit destroys the contract in its constructor, sending
	// nothing to the coinbase.
	benchDestroyInit = common.FromHex("41ff")

	benchBeaconReader = common.HexToAddress("0xbeac04")
	// benchBeaconReaderCode stores, under the caller, the beacon root of the
	// block's timestamp, which the block's pre-execution system call writes:
	// mstore(0, timestamp); staticcall(gas, beaconRoots, 0, 32, 0, 32);
	// sstore(caller, mload(0)).
	benchBeaconReaderCode = common.FromHex("4260005260206000602060007300" + "0f3df6d732807ef1319fb7b8bb8522d0beac02" + "5afa50600051335500")
)
