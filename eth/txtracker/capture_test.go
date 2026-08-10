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

package txtracker

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// TestCaptureRoundTripFile verifies that single-file capture mode
// writes one NDJSON line per emitted event with the right "kind" +
// payload shape, and that CaptureInfo() reflects the active file.
func TestCaptureRoundTripFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "capture.ndjson")

	tr := New()
	if err := tr.SetCapturePath(path); err != nil {
		t.Fatalf("SetCapturePath: %v", err)
	}
	chain := newMockChain()
	tr.Start(chain, nil)

	// One announce + one accept = two state changes (Announced and
	// Pooled). Plus a handful of Observations from the same calls.
	tx := makeTx(1)
	tr.NotifyAnnounced("peerA", []common.Hash{tx.Hash()}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})

	// Give the capture goroutine time to drain; flush is per-line so
	// once the goroutine pops the channel and writes, the file is
	// readable. 200ms is generous on this kind of CI.
	deadline := time.Now().Add(2 * time.Second)
	var lines []string
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil && len(raw) > 0 {
			lines = strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
			if len(lines) >= 4 { // 2 obs + 2 state at minimum
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Snapshot CaptureInfo BEFORE Stop — Stop closes the file so a
	// post-stop Stat() returns 0 size.
	info := tr.CaptureInfo()
	tr.Stop()

	if len(lines) < 4 {
		t.Fatalf("captured lines: got %d, want at least 4 (had: %v)", len(lines), lines)
	}
	var obs, state int
	for _, ln := range lines {
		if ln == "" {
			continue
		}
		var entry struct {
			T       time.Time       `json:"t"`
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal([]byte(ln), &entry); err != nil {
			t.Errorf("bad line %q: %v", ln, err)
			continue
		}
		switch entry.Kind {
		case "obs":
			obs++
		case "state":
			state++
		default:
			t.Errorf("unexpected kind %q in line %q", entry.Kind, ln)
		}
		if entry.T.IsZero() {
			t.Errorf("missing timestamp in line %q", ln)
		}
	}
	if obs == 0 {
		t.Error("no obs lines captured")
	}
	if state == 0 {
		t.Error("no state lines captured")
	}

	if !info.Enabled {
		t.Error("CaptureInfo.Enabled = false, want true")
	}
	if info.Path != path {
		t.Errorf("CaptureInfo.Path = %q, want %q", info.Path, path)
	}
	if info.CurrentSize <= 0 {
		t.Error("CaptureInfo.CurrentSize is zero")
	}
}

// TestCaptureRoundTripDir verifies directory mode creates a
// txtracker-capture.<unix-ns>.ndjson file inside the directory and
// CaptureInfo reflects it.
func TestCaptureRoundTripDir(t *testing.T) {
	dir := t.TempDir()
	tr := New()
	if err := tr.SetCapturePath(dir); err != nil {
		t.Fatalf("SetCapturePath dir: %v", err)
	}
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	tx := makeTx(1)
	tr.NotifyAnnounced("peerA", []common.Hash{tx.Hash()}, nil, nil)
	time.Sleep(150 * time.Millisecond)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var found string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "txtracker-capture.") &&
			strings.HasSuffix(e.Name(), ".ndjson") {
			found = filepath.Join(dir, e.Name())
			break
		}
	}
	if found == "" {
		t.Fatal("no txtracker-capture.*.ndjson file in directory")
	}
	info := tr.CaptureInfo()
	if info.Path != found {
		t.Errorf("CaptureInfo.Path = %q, want %q", info.Path, found)
	}
}

// TestCaptureDisabledByDefault verifies CaptureInfo reports
// Enabled=false when SetCapturePath was never called.
// TestCaptureInfoSurfacesWriteErrs verifies the write-failure counter
// is copied into the CaptureInfo snapshot (white-box: the counter is
// bumped by the sink's write paths on I/O errors, each of which also
// logs; here we bump it directly and assert the RPC-visible plumbing).
func TestCaptureInfoSurfacesWriteErrs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "capture.ndjson")

	tr := New()
	if err := tr.SetCapturePath(path); err != nil {
		t.Fatalf("SetCapturePath: %v", err)
	}
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	atomic.AddUint64(&tr.capture.writeErrs, 3)
	if got := tr.CaptureInfo().WriteErrs; got != 3 {
		t.Fatalf("CaptureInfo.WriteErrs: got %d, want 3", got)
	}
}

func TestCaptureDisabledByDefault(t *testing.T) {
	tr := New()
	chain := newMockChain()
	tr.Start(chain, nil)
	defer tr.Stop()

	info := tr.CaptureInfo()
	if info.Enabled {
		t.Errorf("CaptureInfo.Enabled = true with no path; want false")
	}
}

// TestCaptureNDJSONIsParseable confirms each captured line decodes
// cleanly as a CaptureLine with a valid Observation OR StateChange
// payload (depending on Kind), so external consumers (the lens, my
// own offline analysis) can stream-parse the file.
func TestCaptureNDJSONIsParseable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.ndjson")

	tr := New()
	if err := tr.SetCapturePath(path); err != nil {
		t.Fatalf("SetCapturePath: %v", err)
	}
	chain := newMockChain()
	tr.Start(chain, nil)

	tx := makeTx(7)
	tr.NotifyAnnounced("peerA", []common.Hash{tx.Hash()}, nil, nil)
	tr.NotifyAccepted("peerA", []common.Hash{tx.Hash()})
	time.Sleep(150 * time.Millisecond)
	tr.Stop()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var n int
	for sc.Scan() {
		ln := sc.Bytes()
		var head struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(ln, &head); err != nil {
			t.Fatalf("line %d outer parse: %v", n, err)
		}
		switch head.Kind {
		case "obs":
			var entry struct {
				Payload Observation `json:"payload"`
			}
			if err := json.Unmarshal(ln, &entry); err != nil {
				t.Errorf("line %d obs payload: %v", n, err)
			}
		case "state":
			var entry struct {
				Payload StateChange `json:"payload"`
			}
			if err := json.Unmarshal(ln, &entry); err != nil {
				t.Errorf("line %d state payload: %v", n, err)
			}
		default:
			t.Errorf("line %d unexpected kind %q", n, head.Kind)
		}
		n++
	}
	if err := sc.Err(); err != nil {
		t.Errorf("scanner: %v", err)
	}
	if n < 4 {
		t.Errorf("only %d parseable lines, want >= 4", n)
	}
}
