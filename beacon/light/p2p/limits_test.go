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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/golang/snappy"
)

func TestReadSSZLimits(t *testing.T) {
	for _, size := range []int{0, 1, 92, 70000} {
		var buf bytes.Buffer
		payload := bytes.Repeat([]byte{7}, size)
		if err := writeSSZ(&buf, payload); err != nil {
			t.Fatal(err)
		}
		got, err := readSSZ(bufio.NewReader(&buf), size)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("size %d: read back %d bytes, err %v", size, len(got), err)
		}
	}
	// Larger than the limit: refused before reading it.
	var buf bytes.Buffer
	writeSSZ(&buf, make([]byte, maxStatusSize+1))
	if _, err := readSSZ(bufio.NewReader(&buf), maxStatusSize); err == nil {
		t.Fatal("oversized payload read")
	}
	// Padding chunks without end: read no further than the payload's size allows.
	buf.Reset()
	buf.Write(binary.AppendUvarint(nil, 8))
	buf.Write([]byte{0xff, 6, 0, 0, 's', 'N', 'a', 'P', 'p', 'Y'}) // stream identifier
	for i := 0; i < 1000; i++ {
		buf.Write([]byte{0xfe, 16, 0, 0}) // padding chunk of 16 bytes
		buf.Write(make([]byte, 16))
	}
	total := buf.Len()
	r := bufio.NewReader(&buf)
	if _, err := readSSZ(r, maxPingSize); err == nil {
		t.Fatal("endless padding read as a payload")
	}
	if read := total - buf.Len() - r.Buffered(); int64(read) > 1+maxFramedSize(8) {
		t.Fatalf("read %d bytes for an 8-byte payload", read)
	}
}

func TestDecodeGossipLimit(t *testing.T) {
	if _, err := decodeGossip(snappy.Encode(nil, make([]byte, maxGossipSize))); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGossip(snappy.Encode(nil, make([]byte, maxGossipSize+1))); err == nil {
		t.Fatal("oversized gossip message decoded")
	}
}

func TestTopicsRegexp(t *testing.T) {
	cfg := params.MainnetLightConfig
	sched := digestSchedule(cfg, Networks[cfg.GenesisValidatorsRoot])
	rx := topicsRegexp(sched)
	d := sched[len(sched)-1].digest
	for _, name := range lightClientTopics {
		if topic := fmt.Sprintf("/eth2/%x/%s/ssz_snappy", d, name); !rx.MatchString(topic) {
			t.Errorf("%s not matched", topic)
		}
	}
	for _, topic := range []string{
		fmt.Sprintf("/eth2/%x/beacon_block/ssz_snappy", d),
		"/eth2/00000000/light_client_finality_update/ssz_snappy",
		fmt.Sprintf("/eth2/%x/light_client_finality_update/ssz_snappy/x", d),
	} {
		if rx.MatchString(topic) {
			t.Errorf("%s matched", topic)
		}
	}
}

func TestRecovered(t *testing.T) {
	var failed bool
	func() {
		defer recovered("test", func() { failed = true })
		panic("boom")
	}()
	if !failed {
		t.Fatal("fail not called")
	}
}

// TestStartStop starts a node on localhost without peers (the gossipsub options, score
// parameters and subscriptions included) and stops it.
func TestStartStop(t *testing.T) {
	cfg := params.MainnetLightConfig
	n, err := New(Config{Chain: cfg, Network: Networks[cfg.GenesisValidatorsRoot], GenesisTime: cfg.GenesisTime})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if len(n.subs) == 0 {
		t.Error("no gossip subscriptions")
	}
	n.Stop()
}
