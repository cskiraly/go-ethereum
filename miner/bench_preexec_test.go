package miner

import (
	"testing"
)

// TestPreexecExact builds every synthetic workload with the engine starting
// from pre-executed results, in each way it can get them, and requires the
// block a sequential build makes.
func TestPreexecExact(t *testing.T) {
	if testing.Short() {
		t.Skip("builds every workload several times")
	}
	defer func(candidates float64, rebase bool, miss float64, par int) {
		*buildBenchCandidates, *buildBenchPreexecRebase, *buildBenchPreexecMiss, *buildBenchPreexecPar = candidates, rebase, miss, par
	}(*buildBenchCandidates, *buildBenchPreexecRebase, *buildBenchPreexecMiss, *buildBenchPreexecPar)
	*buildBenchCandidates = 0.25

	variants := []struct {
		name     string
		strategy string
		rebase   bool
		miss     float64
		par      int
	}{
		{"alone", "preexec-w4", false, 0, 1},
		{"shared-reads", "preexec-w2-c", false, 0, 4},
		{"rebase", "preexec-w8-c", true, 0, 1},
		{"missing", "preexec-w4-c", false, 0.3, 2},
		{"rebase-missing", "preexec-w4", true, 0.3, 1},
		{"store-validation", "preexecv-w4-c", false, 0, 1},
		{"inline", "preexec-w4-g30000", false, 0, 1},
	}
	for _, w := range benchWorkloads {
		t.Run(w.name, func(t *testing.T) {
			env, err := newBenchEnv(w, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer env.close()
			seq, err := env.newMiner("sequential")
			if err != nil {
				t.Fatal(err)
			}
			res, _ := buildOnce(env, seq, nil)
			if res.err != nil {
				t.Fatal(res.err)
			}
			reference := res.block
			if err := env.verify(reference); err != nil {
				t.Fatalf("invalid reference block: %v", err)
			}
			for _, v := range variants {
				*buildBenchPreexecRebase, *buildBenchPreexecMiss, *buildBenchPreexecPar = v.rebase, v.miss, v.par
				env.preexecuted = nil // pre-execute again for this variant
				m, err := env.newMiner(v.strategy)
				if err != nil {
					t.Fatalf("%s: %v", v.name, err)
				}
				for rep := 0; rep < 2; rep++ {
					res, rec := buildOnce(env, m, nil)
					if res.err != nil {
						t.Fatalf("%s: build failed: %v", v.name, res.err)
					}
					if res.block.Hash() != reference.Hash() {
						t.Fatalf("%s rep %d: block %x differs from sequential %x%s", v.name, rep, res.block.Hash(), reference.Hash(), env.blockDiff(reference, res.block))
					}
					if v.miss == 0 && rec.Engine["seeded"].(int) == 0 && len(reference.Transactions()) > 0 && v.strategy != "preexec-w4-g30000" {
						t.Errorf("%s: no transaction started from a pre-executed result", v.name)
					}
				}
			}
		})
	}
}
