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
	"math"
	"math/big"
	"os"
	"runtime"
	"runtime/pprof"
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
	buildBenchExact      = flag.Bool("buildbench.exact", false, "fail when a valid block differs from the baseline strategy's")
	buildBenchProfile    = flag.String("buildbench.profile", "", "write CPU, block and mutex profiles of the measured builds to <prefix>-<workload>.{cpu,block,mutex}")
	buildBenchSpans      = flag.Bool("buildbench.spans", false, "record miner telemetry spans for a per-phase breakdown (adds overhead)")
)

// benchEnv is a chain plus a pool holding one case's candidates: a synthetic
// workload on its own genesis, or a mainnet block's successors on its parent.
type benchEnv struct {
	name        string
	description string
	depth       float64 // candidate set size, in blocks
	gasCeil     uint64
	params      func() *generateParams

	config *params.ChainConfig
	engine consensus.Engine
	chain  *core.BlockChain
	pool   *txpool.TxPool
	txs    int
	signer types.Signer

	check    func(*types.Block) error // executes and validates a built block
	verified map[common.Hash]error    // check results by block hash
	closers  []func()
}

func (e *benchEnv) BlockChain() *core.BlockChain { return e.chain }
func (e *benchEnv) TxPool() *txpool.TxPool       { return e.pool }

func (e *benchEnv) close() {
	e.pool.Close()
	for i := len(e.closers) - 1; i >= 0; i-- {
		e.closers[i]()
	}
}

// verify executes and validates block as a node receiving it would. Results
// are cached by block hash.
func (e *benchEnv) verify(block *types.Block) error {
	if err, ok := e.verified[block.Hash()]; ok {
		return err
	}
	err := e.check(block)
	e.verified[block.Hash()] = err
	return err
}

var benchCoinbase = common.HexToAddress("0xc0ffee")

func newBenchEnv(w benchWorkload, seed int64) (*benchEnv, error) {
	config := params.MergedTestChainConfig
	gen := newBenchGen(config, seed, uint64(*buildBenchCandidates*benchBlockGas))
	w.generate(gen)
	// The system contracts of the active forks, as on mainnet.
	gen.deploy(params.BeaconRootsAddress, params.BeaconRootsCode, nil)
	gen.deploy(params.HistoryStorageAddress, params.HistoryStorageCode, nil)
	gen.deploy(params.WithdrawalQueueAddress, params.WithdrawalQueueCode, nil)
	gen.deploy(params.ConsolidationQueueAddress, params.ConsolidationQueueCode, nil)
	// The fee recipient exists, as it does on mainnet. Otherwise the first
	// transaction creates it, which conflicts with every later one in engines
	// that track account existence.
	gen.alloc[benchCoinbase] = types.Account{Balance: big.NewInt(params.Ether)}

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
	env := &benchEnv{
		name:        w.name,
		description: w.description,
		depth:       *buildBenchCandidates,
		gasCeil:     benchBlockGas,
		config:      config,
		engine:      engine,
		chain:       chain,
		txs:         len(gen.txs),
		signer:      gen.signer,
		verified:    make(map[common.Hash]error),
		closers:     []func(){chain.Stop},
	}
	env.params = func() *generateParams {
		parent := chain.CurrentBlock()
		beaconRoot := common.Hash{0x01}
		slot := uint64(1)
		return &generateParams{
			timestamp:   parent.Time + 12,
			forceTime:   true,
			parentHash:  parent.Hash(),
			coinbase:    benchCoinbase,
			random:      common.Hash{0x02},
			withdrawals: types.Withdrawals{},
			beaconRoot:  &beaconRoot,
			slotNum:     &slot,
		}
	}
	// Built blocks are imported into a separate chain on the same genesis.
	var verifier *core.BlockChain
	env.check = func(block *types.Block) error {
		if verifier == nil {
			if verifier, err = core.NewBlockChain(rawdb.NewMemoryDatabase(), gspec, engine, core.DefaultConfig().WithStateScheme(rawdb.PathScheme)); err != nil {
				return err
			}
			env.closers = append(env.closers, verifier.Stop)
		}
		_, err := verifier.InsertChain(types.Blocks{block})
		return err
	}
	if env.pool, err = newBenchPool(chain, gen.signer, gen.txs); err != nil {
		env.close()
		return nil, err
	}
	return env, nil
}

// newBenchPool returns a pool serving txs as candidates.
func newBenchPool(chain *core.BlockChain, signer types.Signer, txs []*types.Transaction) (*txpool.TxPool, error) {
	fixed, err := newFixedPool(signer, txs)
	if err != nil {
		return nil, err
	}
	return txpool.New(0, chain, []txpool.SubPool{fixed})
}

// blockDiff describes where got's transactions first diverge from ref's.
func (e *benchEnv) blockDiff(ref, got *types.Block) string {
	if got == nil {
		return ""
	}
	describe := func(txs types.Transactions, i int) string {
		if i >= len(txs) {
			return "none"
		}
		tx := txs[i]
		from, _ := types.Sender(e.signer, tx)
		return fmt.Sprintf("%x from %x nonce %d gas %d tip %v", tx.Hash().Bytes()[:4], from.Bytes()[:4], tx.Nonce(), tx.Gas(), tx.GasTipCap())
	}
	a, b := ref.Transactions(), got.Transactions()
	i := 0
	for i < len(a) && i < len(b) && a[i].Hash() == b[i].Hash() {
		i++
	}
	if i == len(a) && i == len(b) {
		return fmt.Sprintf("\n\tsame %d txs; gas used %d vs %d, root %x vs %x", len(a), ref.GasUsed(), got.GasUsed(), ref.Root(), got.Root())
	}
	inRef := make(map[common.Hash]bool, len(a))
	for _, tx := range a {
		inRef[tx.Hash()] = true
	}
	var extra int
	for _, tx := range b {
		if !inRef[tx.Hash()] {
			extra++
		}
	}
	return fmt.Sprintf("\n\t%d vs %d txs (%d not in reference), first divergence at index %d:\n\t  reference: %s\n\t  got:       %s",
		len(a), len(b), extra, i, describe(a, i), describe(b, i))
}

// newMiner returns a miner configured for strategy. The recommit deadline is
// set far away so that builds always run to completion.
func (e *benchEnv) newMiner(strategy string) (*Miner, error) {
	cfg := DefaultConfig
	cfg.GasCeil = e.gasCeil
	cfg.Recommit = time.Hour
	if err := applyBenchStrategy(strategy, &cfg); err != nil {
		return nil, err
	}
	return New(e, cfg, e.engine), nil
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
	Invalid    string           `json:"invalid,omitempty"` // why the verifier rejected the block
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

// startBuildProfile starts profiling the builds of a workload if
// -buildbench.profile is set, and returns the function that stops it. Block and
// mutex profiles are cumulative over the process, so profile one workload per
// run; don't combine with go test's own -cpuprofile.
func startBuildProfile(t *testing.T, workload string) func() {
	if *buildBenchProfile == "" {
		return func() {}
	}
	prefix := *buildBenchProfile + "-" + workload
	cpu, err := os.Create(prefix + ".cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		t.Fatal(err)
	}
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	return func() {
		pprof.StopCPUProfile()
		cpu.Close()
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
		for _, name := range []string{"block", "mutex"} {
			f, err := os.Create(prefix + "." + name)
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
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
		Workload:   env.name,
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
	cases, err := selectedCases(t)
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
	if *buildBenchTrace != "" {
		f, err := os.Create(*buildBenchTrace)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		traceOut = json.NewEncoder(f)
	}
	var spans *spanRecorder
	if *buildBenchSpans {
		spans = newSpanRecorder()
	}
	speedups := make(map[string][]float64) // per strategy, the median speedup of each case
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env, err := c.open()
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
			var reference *types.Block
			for i, m := range miners {
				res, _ := buildOnce(env, m, spans)
				if res.err != nil {
					t.Fatalf("%s: warm-up build failed: %v", strategies[i], res.err)
				}
				if i == 0 {
					reference = res.block
				}
			}
			if err := env.verify(reference); err != nil {
				t.Fatalf("%s: invalid reference block: %v", strategies[0], err)
			}
			if traceOut != nil {
				trace, err := traceReference(env, reference, 3)
				if err != nil {
					t.Fatalf("tracing the reference block: %v", err)
				}
				if err := traceOut.Encode(trace); err != nil {
					t.Fatal(err)
				}
			}
			// Build first and check afterwards, so that profiles cover only
			// the builds.
			type build struct {
				rec   benchRecord
				block *types.Block
			}
			var builds []build
			stopProfile := startBuildProfile(t, env.name)
			for rep := 0; rep < *buildBenchReps; rep++ {
				// Rotate the order every rep so each strategy runs in every
				// position equally often.
				for slot := range strategies {
					i := (slot + rep) % len(strategies)
					res, rec := buildOnce(env, miners[i], spans)
					rec.Strategy, rec.Rep, rec.Slot, rec.Seed, rec.Depth = strategies[i], rep, slot, *buildBenchSeed, env.depth
					builds = append(builds, build{rec, res.block})
				}
			}
			stopProfile()

			// Engines need not build the baseline's block. A block that
			// differs is imported to check that it is valid, and only the
			// first difference per strategy is shown unless -buildbench.exact.
			shown := make(map[string]bool)
			records := make(map[string][]benchRecord)
			for _, b := range builds {
				rec := b.rec
				rec.Match = rec.Error == "" && rec.BlockHash == reference.Hash()
				switch {
				case rec.Error != "":
					t.Errorf("%s rep %d: build failed: %s", rec.Strategy, rec.Rep, rec.Error)
				case rec.Match:
				case env.verify(b.block) != nil:
					rec.Invalid = env.verify(b.block).Error()
					t.Errorf("%s rep %d: invalid block %x: %s", rec.Strategy, rec.Rep, rec.BlockHash, rec.Invalid)
				case *buildBenchExact:
					t.Errorf("%s rep %d: block %x differs from %s block %x%s",
						rec.Strategy, rec.Rep, rec.BlockHash, strategies[0], reference.Hash(), env.blockDiff(reference, b.block))
				case !shown[rec.Strategy]:
					shown[rec.Strategy] = true
					t.Logf("%s rep %d: valid block %x differs from %s block %x%s",
						rec.Strategy, rec.Rep, rec.BlockHash, strategies[0], reference.Hash(), env.blockDiff(reference, b.block))
				}
				records[rec.Strategy] = append(records[rec.Strategy], rec)
				if out != nil {
					if err := out.Encode(rec); err != nil {
						t.Fatal(err)
					}
				}
			}
			t.Log(summarizeBench(env, strategies, records))
			for _, s := range strategies {
				speedups[s] = append(speedups[s], medianSpeedup(records[strategies[0]], records[s]))
			}
		})
	}
	if len(cases) > 1 {
		t.Log(summarizeCases(strategies, speedups))
	}
}

// benchCase is one input to build blocks from.
type benchCase struct {
	name string
	open func() (*benchEnv, error)
}

// selectedCases returns the mainnet blocks if -buildbench.mainnet is set, the
// synthetic workloads otherwise.
func selectedCases(t *testing.T) ([]benchCase, error) {
	if *buildBenchMainnet != "" {
		return mainnetCases(t)
	}
	workloads, err := selectedWorkloads()
	if err != nil {
		return nil, err
	}
	cases := make([]benchCase, len(workloads))
	for i, w := range workloads {
		cases[i] = benchCase{name: w.name, open: func() (*benchEnv, error) { return newBenchEnv(w, *buildBenchSeed) }}
	}
	return cases, nil
}

// medianSpeedup is the median per-rep ratio of base's build time to recs'.
func medianSpeedup(base, recs []benchRecord) float64 {
	ratios := make([]float64, len(recs))
	for i, r := range recs {
		ratios[i] = float64(base[i].WallNs) / float64(r.WallNs)
	}
	return median(ratios)
}

// summarizeCases renders, per strategy, the geometric mean, median and range
// of the cases' median speedups.
func summarizeCases(strategies []string, speedups map[string][]float64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\nall %d cases: median speedup per case\n", len(speedups[strategies[0]]))
	fmt.Fprintf(&b, "  %-18s %8s %8s %8s %8s\n", "strategy", "geomean", "median", "min", "max")
	for _, s := range strategies {
		xs := speedups[s]
		logs := 0.0
		for _, x := range xs {
			logs += math.Log(x)
		}
		fmt.Fprintf(&b, "  %-18s %7.2fx %7.2fx %7.2fx %7.2fx\n", s, math.Exp(logs/float64(len(xs))), median(xs), slices.Min(xs), slices.Max(xs))
	}
	return b.String()
}

// summarizeBench renders per-strategy medians and the median per-rep speedup
// over the first (baseline) strategy.
func summarizeBench(env *benchEnv, strategies []string, records map[string][]benchRecord) string {
	var b strings.Builder
	base := records[strategies[0]]
	first := base[0]
	fmt.Fprintf(&b, "\n%s: %s\n  block: %d/%d txs, %.1f Mgas\n", env.name, env.description, first.Txs, first.Candidates, float64(first.GasUsed)/1e6)
	fmt.Fprintf(&b, "  %-14s %10s %10s %10s %9s %9s %7s %6s %8s\n", "strategy", "median", "min", "max", "Mgas/s", "speedup", "same", "txs", "fees")
	baseFees := median(benchFees(base))
	for _, s := range strategies {
		recs := records[s]
		walls := make([]float64, len(recs))
		ratios := make([]float64, len(recs))
		gas := make([]float64, len(recs))
		txs := make([]float64, len(recs))
		same := 0
		for i, r := range recs {
			walls[i] = float64(r.WallNs)
			ratios[i] = float64(base[i].WallNs) / float64(r.WallNs)
			gas[i] = float64(r.GasUsed)
			txs[i] = float64(r.Txs)
			if r.Match {
				same++
			}
		}
		med := median(walls)
		fmt.Fprintf(&b, "  %-14s %10s %10s %10s %9.0f %8.2fx %7s %6.0f %+7.3f%%\n", s,
			time.Duration(med).Round(10*time.Microsecond),
			time.Duration(slices.Min(walls)).Round(10*time.Microsecond),
			time.Duration(slices.Max(walls)).Round(10*time.Microsecond),
			median(gas)/1e6/(med/1e9), median(ratios),
			fmt.Sprintf("%d/%d", same, len(recs)), median(txs),
			100*(median(benchFees(recs))/baseFees-1))
	}
	b.WriteString("  (same: builds identical to the first strategy's block; fees: median relative to it)\n")
	return b.String()
}

// benchFees returns the fees of each record, in wei.
func benchFees(recs []benchRecord) []float64 {
	fees := make([]float64, len(recs))
	for i, r := range recs {
		if f, ok := new(big.Float).SetString(r.Fees); ok {
			fees[i], _ = f.Float64()
		}
	}
	return fees
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
