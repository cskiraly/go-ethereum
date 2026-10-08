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

// Paired block building benchmark. Every strategy builds the same block (same
// parent, candidates and header fields) many times, in a rotating order, and
// the built blocks are compared across strategies.
//
//	go test ./miner -run TestBuildBench -count=1 -timeout=0 -v -args \
//	    -buildbench -buildbench.strategies=sequential,parallel-w8 \
//	    -buildbench.reps=20 -buildbench.out=/tmp/build.jsonl
//
// The same builds are available as standard benchmarks for benchstat:
//
//	go test ./miner -run '^$' -bench BenchmarkBuild -count=10

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	buildBenchFlag       = flag.Bool("buildbench", false, "run the paired block building benchmark (TestBuildBench)")
	buildBenchWorkloads  = flag.String("buildbench.workloads", "", "comma separated workloads (default: all)")
	buildBenchStrategies = flag.String("buildbench.strategies", "sequential,parallel-w8", "comma separated strategies; the first is the baseline")
	buildBenchReps       = flag.Int("buildbench.reps", 10, "builds per strategy and workload")
	buildBenchOut        = flag.String("buildbench.out", "", "JSONL file receiving one record per build")
	buildBenchSeed       = flag.Int64("buildbench.seed", 1, "workload generation seed")
	buildBenchCandidates = flag.Float64("buildbench.candidates", 2, "candidate set size, in blocks worth of gas")
	buildBenchGC         = flag.Bool("buildbench.gc", true, "run a GC before every build")
	buildBenchSpans      = flag.Bool("buildbench.spans", false, "record miner telemetry spans for a per-phase breakdown (adds overhead)")
)

// benchEnv is a chain at genesis plus a pool holding a workload's candidates.
type benchEnv struct {
	workload benchWorkload
	config   *params.ChainConfig
	engine   consensus.Engine
	chain    *core.BlockChain
	pool     *txpool.TxPool
	txs      int
}

func (e *benchEnv) BlockChain() *core.BlockChain { return e.chain }
func (e *benchEnv) TxPool() *txpool.TxPool       { return e.pool }

func (e *benchEnv) close() {
	e.pool.Close()
	e.chain.Stop()
}

func newBenchEnv(w benchWorkload, seed int64) (*benchEnv, error) {
	config := params.MergedTestChainConfig
	gen := newBenchGen(config, seed, uint64(*buildBenchCandidates*benchBlockGas))
	w.generate(gen)

	gspec := &core.Genesis{
		Config:   config,
		GasLimit: benchBlockGas,
		BaseFee:  big.NewInt(params.InitialBaseFee),
		Alloc:    gen.alloc,
	}
	engine := beacon.New(ethash.NewFaker())
	chain, err := core.NewBlockChain(rawdb.NewMemoryDatabase(), gspec, engine, core.DefaultConfig().WithStateScheme(rawdb.PathScheme))
	if err != nil {
		return nil, err
	}
	fixed, err := newFixedPool(gen.signer, gen.txs)
	if err != nil {
		chain.Stop()
		return nil, err
	}
	pool, err := txpool.New(0, chain, []txpool.SubPool{fixed})
	if err != nil {
		chain.Stop()
		return nil, err
	}
	return &benchEnv{workload: w, config: config, engine: engine, chain: chain, pool: pool, txs: len(gen.txs)}, nil
}

// newMiner returns a miner configured for strategy. The recommit deadline is
// set far away so that builds always run to completion.
func (e *benchEnv) newMiner(strategy string) (*Miner, error) {
	cfg := DefaultConfig
	cfg.GasCeil = benchBlockGas
	cfg.Recommit = time.Hour
	if err := applyBenchStrategy(strategy, &cfg); err != nil {
		return nil, err
	}
	return New(e, cfg, e.engine), nil
}

func (e *benchEnv) params() *generateParams {
	parent := e.chain.CurrentBlock()
	beaconRoot := common.Hash{0x01}
	slot := uint64(1)
	return &generateParams{
		timestamp:   parent.Time + 12,
		forceTime:   true,
		parentHash:  parent.Hash(),
		coinbase:    common.HexToAddress("0xc0ffee"),
		random:      common.Hash{0x02},
		withdrawals: types.Withdrawals{},
		beaconRoot:  &beaconRoot,
		slotNum:     &slot,
	}
}

// benchRecord is one build.
type benchRecord struct {
	Workload   string           `json:"workload"`
	Strategy   string           `json:"strategy"`
	Rep        int              `json:"rep"`
	Slot       int              `json:"slot"` // position of this build within the rep
	Seed       int64            `json:"seed"`
	Depth      float64          `json:"candidateBlocks"` // -buildbench.candidates
	GOMAXPROCS int              `json:"gomaxprocs"`
	WallNs     int64            `json:"wallNs"`
	Txs        int              `json:"txs"`
	Candidates int              `json:"candidates"`
	GasUsed    uint64           `json:"gasUsed"`
	Fees       string           `json:"fees"`
	BlockHash  common.Hash      `json:"blockHash"`
	StateRoot  common.Hash      `json:"stateRoot"`
	Match      bool             `json:"match"` // block identical to the baseline strategy's
	Error      string           `json:"error,omitempty"`
	Engine     map[string]any   `json:"engine,omitempty"`
	PhasesNs   map[string]int64 `json:"phasesNs,omitempty"`
}

// spanRecorder captures miner telemetry spans when phase timing is enabled.
type spanRecorder struct {
	rec      *tracetest.SpanRecorder
	provider *sdktrace.TracerProvider
}

// dropPerTx skips the per-transaction span of the sequential path, which
// would otherwise bias the comparison against it.
type dropPerTx struct{}

func (dropPerTx) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Name == "miner.commitTransaction" {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
}
func (dropPerTx) Description() string { return "dropPerTx" }

func newSpanRecorder() *spanRecorder {
	rec := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(dropPerTx{}), sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(provider)
	return &spanRecorder{rec: rec, provider: provider}
}

// build runs one build and returns the root context to use and a function
// collecting the per-phase durations.
func (r *spanRecorder) start() (context.Context, func() map[string]int64) {
	if r == nil {
		return context.Background(), func() map[string]int64 { return nil }
	}
	ctx, root := r.provider.Tracer("buildbench").Start(context.Background(), "buildbench")
	return ctx, func() map[string]int64 {
		root.End()
		phases := make(map[string]int64)
		for _, s := range r.rec.Ended() {
			if s.Name() != "buildbench" {
				phases[strings.TrimPrefix(s.Name(), "miner.")] += s.EndTime().Sub(s.StartTime()).Nanoseconds()
			}
		}
		r.rec.Reset()
		return phases
	}
}

// buildOnce builds one block with m and measures it.
func buildOnce(env *benchEnv, m *Miner, spans *spanRecorder) (*newPayloadResult, benchRecord) {
	if *buildBenchGC {
		runtime.GC()
	}
	resetBenchStats(m)
	ctx, phases := spans.start()
	start := time.Now()
	res := m.generateWork(ctx, env.params(), false)
	wall := time.Since(start)

	rec := benchRecord{
		Workload:   env.workload.name,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		WallNs:     wall.Nanoseconds(),
		Candidates: env.txs,
		Engine:     benchStats(m),
		PhasesNs:   phases(),
	}
	if res.err != nil {
		rec.Error = res.err.Error()
		return res, rec
	}
	rec.Txs = len(res.block.Transactions())
	rec.GasUsed = res.block.GasUsed()
	rec.Fees = res.fees.String()
	rec.BlockHash = res.block.Hash()
	rec.StateRoot = res.block.Root()
	return res, rec
}

func splitList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func selectedWorkloads() ([]benchWorkload, error) {
	names := splitList(*buildBenchWorkloads)
	if len(names) == 0 {
		return benchWorkloads, nil
	}
	var ws []benchWorkload
	for _, n := range names {
		w, err := findBenchWorkload(n)
		if err != nil {
			return nil, err
		}
		ws = append(ws, w)
	}
	return ws, nil
}

// TestBuildBench runs the paired benchmark. It is skipped unless -buildbench
// is given.
func TestBuildBench(t *testing.T) {
	if !*buildBenchFlag {
		t.Skip("enable with -buildbench")
	}
	workloads, err := selectedWorkloads()
	if err != nil {
		t.Fatal(err)
	}
	strategies := splitList(*buildBenchStrategies)
	if len(strategies) == 0 {
		t.Fatal("no strategies")
	}
	var out *json.Encoder
	if *buildBenchOut != "" {
		f, err := os.Create(*buildBenchOut)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		out = json.NewEncoder(f)
	}
	var spans *spanRecorder
	if *buildBenchSpans {
		spans = newSpanRecorder()
	}
	for _, w := range workloads {
		t.Run(w.name, func(t *testing.T) {
			env, err := newBenchEnv(w, *buildBenchSeed)
			if err != nil {
				t.Fatal(err)
			}
			defer env.close()

			miners := make([]*Miner, len(strategies))
			for i, s := range strategies {
				if miners[i], err = env.newMiner(s); err != nil {
					t.Fatal(err)
				}
			}
			// Warm up: one unrecorded build per strategy, which also fixes
			// the reference block.
			var reference common.Hash
			for i, m := range miners {
				res, rec := buildOnce(env, m, spans)
				if res.err != nil {
					t.Fatalf("%s: warm-up build failed: %v", strategies[i], res.err)
				}
				if i == 0 {
					reference = rec.BlockHash
				}
			}
			records := make(map[string][]benchRecord)
			for rep := 0; rep < *buildBenchReps; rep++ {
				// Rotate the order every rep so each strategy runs in every
				// position equally often.
				for slot := range strategies {
					i := (slot + rep) % len(strategies)
					_, rec := buildOnce(env, miners[i], spans)
					rec.Strategy, rec.Rep, rec.Slot, rec.Seed, rec.Depth = strategies[i], rep, slot, *buildBenchSeed, *buildBenchCandidates
					rec.Match = rec.Error == "" && rec.BlockHash == reference
					if !rec.Match {
						t.Errorf("%s rep %d: block %x differs from %s block %x (err: %s)",
							strategies[i], rep, rec.BlockHash, strategies[0], reference, rec.Error)
					}
					records[rec.Strategy] = append(records[rec.Strategy], rec)
					if out != nil {
						if err := out.Encode(rec); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			t.Log(summarizeBench(w, strategies, records))
		})
	}
}

// summarizeBench renders per-strategy medians and the median per-rep speedup
// over the first (baseline) strategy.
func summarizeBench(w benchWorkload, strategies []string, records map[string][]benchRecord) string {
	var b strings.Builder
	base := records[strategies[0]]
	first := base[0]
	fmt.Fprintf(&b, "\n%s: %s\n  block: %d/%d txs, %.1f Mgas\n", w.name, w.description, first.Txs, first.Candidates, float64(first.GasUsed)/1e6)
	fmt.Fprintf(&b, "  %-14s %10s %10s %10s %9s %9s\n", "strategy", "median", "min", "max", "Mgas/s", "speedup")
	for _, s := range strategies {
		recs := records[s]
		walls := make([]float64, len(recs))
		ratios := make([]float64, len(recs))
		for i, r := range recs {
			walls[i] = float64(r.WallNs)
			ratios[i] = float64(base[i].WallNs) / float64(r.WallNs)
		}
		med := median(walls)
		fmt.Fprintf(&b, "  %-14s %10s %10s %10s %9.0f %8.2fx\n", s,
			time.Duration(med).Round(10*time.Microsecond),
			time.Duration(slices.Min(walls)).Round(10*time.Microsecond),
			time.Duration(slices.Max(walls)).Round(10*time.Microsecond),
			float64(first.GasUsed)/1e6/(med/1e9), median(ratios))
	}
	return b.String()
}

func median(xs []float64) float64 {
	s := slices.Clone(xs)
	slices.Sort(s)
	n := len(s)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

var (
	benchEnvsMu sync.Mutex
	benchEnvs   = make(map[string]*benchEnv)
)

// cachedBenchEnv shares workload setups between benchmarks, as generating and
// signing the candidates dominates short benchmark runs.
func cachedBenchEnv(b *testing.B, w benchWorkload) *benchEnv {
	benchEnvsMu.Lock()
	defer benchEnvsMu.Unlock()
	if env, ok := benchEnvs[w.name]; ok {
		return env
	}
	env, err := newBenchEnv(w, *buildBenchSeed)
	if err != nil {
		b.Fatal(err)
	}
	benchEnvs[w.name] = env
	return env
}

// BenchmarkBuild builds each workload's block with each strategy.
func BenchmarkBuild(b *testing.B) {
	workloads, err := selectedWorkloads()
	if err != nil {
		b.Fatal(err)
	}
	for _, w := range workloads {
		for _, s := range splitList(*buildBenchStrategies) {
			b.Run(fmt.Sprintf("workload=%s/strategy=%s", w.name, s), func(b *testing.B) {
				env := cachedBenchEnv(b, w)
				m, err := env.newMiner(s)
				if err != nil {
					b.Fatal(err)
				}
				var gas uint64
				for b.Loop() {
					res := m.generateWork(context.Background(), env.params(), false)
					if res.err != nil {
						b.Fatal(res.err)
					}
					gas += res.block.GasUsed()
				}
				b.ReportMetric(float64(gas)/1e6/b.Elapsed().Seconds(), "Mgas/s")
			})
		}
	}
}
