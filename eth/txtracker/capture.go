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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
)

// captureMaxBytes and captureKeepFiles configure the rotating
// NDJSON capture file. 50 MiB × 5 files ≈ 250 MiB total budget;
// at ~10 KB/s on a busy node that's roughly 7 hours of coverage.
// Hardcoded for v1 — promote to a flag if real deployments need
// different sizing.
const (
	captureMaxBytes  = 50 << 20
	captureKeepFiles = 5
)

// CaptureInfo is the snapshot returned by Tracker.CaptureInfo()
// (and the txtracker_captureInfo RPC). Lens reports embed it so
// I (the diagnostician) can find the corresponding NDJSON file.
type CaptureInfo struct {
	Enabled      bool      `json:"enabled"`
	Path         string    `json:"path,omitempty"`
	Since        time.Time `json:"since,omitempty"`
	CurrentSize  int64     `json:"currentSize,omitempty"`
	RotatedFiles []string  `json:"rotatedFiles,omitempty"`
	ObsDropped   uint64    `json:"obsDropped,omitempty"`
	StateDropped uint64    `json:"stateDropped,omitempty"`
	WriteErrs    uint64    `json:"writeErrs,omitempty"` // failed NDJSON writes (each also logged)
}

// captureLine is the on-disk shape of one captured event. The
// "kind" discriminator tells consumers which payload schema to
// expect (Observation vs StateChange).
type captureLine struct {
	T       time.Time `json:"t"`
	Kind    string    `json:"kind"` // "obs" or "state"
	Payload any       `json:"payload"`
}

// captureSink subscribes to the tracker's Observation and
// StateChange feeds and persists every event as one JSON line in
// a rotating NDJSON file. Used for offline diagnosis: the lens
// references this file when generating bug reports.
//
// Thread model: the loop goroutine drains the channels passed to
// the tracker's Subscribe* methods. Backpressure on a slow disk
// shows up as a full subscription channel; the existing emit
// loops drop on full upstream channels (`obsDropped` /
// `stateDropped`) so the tracker's hot path is never blocked.
type captureSink struct {
	root  string // user-supplied path (file or directory)
	isDir bool
	since time.Time

	mu      sync.Mutex
	file    *os.File
	bw      *bufio.Writer
	written int64    // bytes written to current file
	rotated []string // history of rotated-out file paths (most recent last)

	writeErrs    uint64 // saturating: any failed write bumps this
	obsDropped   uint64 // atomic — slow-loop drops (parity with tracker)
	stateDropped uint64 // atomic

	obsCh    chan Observation
	stateCh  chan StateChange
	obsSub   event.Subscription
	stateSub event.Subscription
	quit     chan struct{}
	wg       sync.WaitGroup
}

// newCaptureSink constructs a sink for the given path. If the
// path exists as (or ends in `/`) a directory, files of the form
// `txtracker-capture.<unix-ns>.ndjson` are created inside, with
// rotation. Otherwise the path is treated as a single file (no
// rotation) and opened in append mode.
func newCaptureSink(path string) (*captureSink, error) {
	if path == "" {
		return nil, fmt.Errorf("txtracker: capture path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("txtracker: capture path: %w", err)
	}
	s := &captureSink{
		root:    abs,
		since:   time.Now(),
		obsCh:   make(chan Observation, emitBuffer),
		stateCh: make(chan StateChange, emitBuffer),
		quit:    make(chan struct{}),
	}
	// Decide directory vs single-file mode.
	if info, err := os.Stat(abs); err == nil {
		s.isDir = info.IsDir()
	} else if os.IsNotExist(err) {
		// Heuristic: trailing slash, or no extension, ⇒ directory.
		if len(abs) > 0 && (abs[len(abs)-1] == os.PathSeparator || filepath.Ext(abs) == "") {
			s.isDir = true
			if err := os.MkdirAll(abs, 0o755); err != nil {
				return nil, fmt.Errorf("txtracker: capture mkdir: %w", err)
			}
		}
	} else {
		return nil, fmt.Errorf("txtracker: capture stat: %w", err)
	}
	if err := s.openLocked(); err != nil {
		return nil, err
	}
	// In dir-mode, populate the rotated[] list with existing
	// capture files so the keep-N policy holds across restarts.
	if s.isDir {
		entries, err := os.ReadDir(s.root)
		if err == nil {
			var existing []string
			for _, e := range entries {
				if !e.IsDir() && filepath.Ext(e.Name()) == ".ndjson" &&
					len(e.Name()) > len("txtracker-capture.") &&
					e.Name()[:len("txtracker-capture.")] == "txtracker-capture." &&
					filepath.Join(s.root, e.Name()) != s.file.Name() {
					existing = append(existing, filepath.Join(s.root, e.Name()))
				}
			}
			sort.Strings(existing)
			s.rotated = existing
		}
	}
	return s, nil
}

// openLocked opens the next capture file in dir-mode or the
// single file in file-mode. Caller must NOT hold s.mu (we lock
// internally) — name reflects that it leaves s.mu held briefly.
func (s *captureSink) openLocked() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var fname string
	if s.isDir {
		fname = filepath.Join(s.root, fmt.Sprintf("txtracker-capture.%d.ndjson", time.Now().UnixNano()))
	} else {
		fname = s.root
	}
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("txtracker: capture open: %w", err)
	}
	s.file = f
	s.bw = bufio.NewWriter(f)
	if fi, err := f.Stat(); err == nil {
		s.written = fi.Size()
	}
	return nil
}

// rotateLocked closes the current file and opens a new one; the
// caller holds s.mu.
func (s *captureSink) rotateLocked() {
	if !s.isDir {
		return
	}
	if s.bw != nil {
		_ = s.bw.Flush()
	}
	old := ""
	if s.file != nil {
		old = s.file.Name()
		_ = s.file.Close()
	}
	if old != "" {
		s.rotated = append(s.rotated, old)
		// Cap to keepFiles-1 (the new current file counts as 1).
		for len(s.rotated) > captureKeepFiles-1 {
			rm := s.rotated[0]
			s.rotated = s.rotated[1:]
			_ = os.Remove(rm)
		}
	}
	fname := filepath.Join(s.root, fmt.Sprintf("txtracker-capture.%d.ndjson", time.Now().UnixNano()))
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Warn("txtracker capture rotate failed", "err", err)
		atomic.AddUint64(&s.writeErrs, 1)
		s.file = nil
		s.bw = nil
		return
	}
	s.file = f
	s.bw = bufio.NewWriter(f)
	s.written = 0
}

func (s *captureSink) writeLine(line captureLine) {
	b, err := json.Marshal(line)
	if err != nil {
		atomic.AddUint64(&s.writeErrs, 1)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bw == nil {
		return
	}
	n, err := s.bw.Write(b)
	if err != nil {
		atomic.AddUint64(&s.writeErrs, 1)
		return
	}
	n2, err := s.bw.WriteString("\n")
	if err != nil {
		atomic.AddUint64(&s.writeErrs, 1)
		return
	}
	if err := s.bw.Flush(); err != nil {
		atomic.AddUint64(&s.writeErrs, 1)
		return
	}
	s.written += int64(n + n2)
	if s.isDir && s.written >= captureMaxBytes {
		s.rotateLocked()
	}
}

func (s *captureSink) loop() {
	defer s.wg.Done()
	for {
		select {
		case obs := <-s.obsCh:
			s.writeLine(captureLine{T: obs.Timestamp, Kind: "obs", Payload: obs})
		case ch := <-s.stateCh:
			s.writeLine(captureLine{T: ch.Timestamp, Kind: "state", Payload: ch})
		case <-s.quit:
			return
		}
	}
}

// start subscribes to the tracker's feeds and launches the
// writer goroutine. Idempotent only in the sense that calling it
// twice will start two goroutines — callers should ensure single
// invocation (Tracker.Start does).
func (s *captureSink) start(t *Tracker) {
	s.obsSub = t.SubscribeObservations(s.obsCh)
	s.stateSub = t.SubscribeStateChanges(s.stateCh)
	s.wg.Add(1)
	go s.loop()
}

// stop unsubscribes, drains pending events, and closes the file.
func (s *captureSink) stop() {
	if s.obsSub != nil {
		s.obsSub.Unsubscribe()
	}
	if s.stateSub != nil {
		s.stateSub.Unsubscribe()
	}
	close(s.quit)
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bw != nil {
		_ = s.bw.Flush()
	}
	if s.file != nil {
		_ = s.file.Close()
	}
}

// info returns a snapshot suitable for the txtracker_captureInfo
// RPC reply.
func (s *captureSink) info() CaptureInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := CaptureInfo{
		Enabled:      true,
		Since:        s.since,
		ObsDropped:   atomic.LoadUint64(&s.obsDropped),
		StateDropped: atomic.LoadUint64(&s.stateDropped),
		WriteErrs:    atomic.LoadUint64(&s.writeErrs),
	}
	if s.file != nil {
		out.Path = s.file.Name()
		if fi, err := s.file.Stat(); err == nil {
			out.CurrentSize = fi.Size()
		}
	}
	if len(s.rotated) > 0 {
		out.RotatedFiles = append([]string(nil), s.rotated...)
	}
	return out
}
