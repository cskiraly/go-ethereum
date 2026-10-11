package parallel

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

// TestAheadGetHash checks BLOCKHASH for a header ahead of its parent: the
// parent's chain up to it, zero for the blocks in between, and no walk back
// through the chain (core.GetHashFn would take the parent to be at number-1).
func TestAheadGetHash(t *testing.T) {
	gspec := &core.Genesis{Config: params.TestChainConfig, BaseFee: big.NewInt(params.InitialBaseFee)}
	engine := ethash.NewFaker()
	_, blocks, _ := core.GenerateChainWithGenesis(gspec, engine, 10, func(int, *core.BlockGen) {})
	chain, err := core.NewBlockChain(rawdb.NewMemoryDatabase(), gspec, engine, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer chain.Stop()
	if _, err := chain.InsertChain(blocks); err != nil {
		t.Fatal(err)
	}
	parent := blocks[5].Header() // number 6
	header := &types.Header{ParentHash: parent.Hash(), Number: big.NewInt(9)}
	get := (&execEnv{header: header, parent: parent}).aheadGetHash(chain)
	for n := uint64(0); n <= 10; n++ {
		var want common.Hash
		if n >= 1 && n <= 6 {
			want = blocks[n-1].Hash()
		}
		if n == 0 {
			want = chain.Genesis().Hash()
		}
		if have := get(n); have != want {
			t.Errorf("BLOCKHASH(%d): have %x, want %x", n, have, want)
		}
	}
}
