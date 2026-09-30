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
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/beacon/params"
	"gopkg.in/yaml.v3"
)

// Around a digest change (a fork, or from Fulu on a blob parameter change), a node
// subscribes to the new digest's gossip topics this many epochs before it, and keeps the
// old ones this many epochs after it (the consensus p2p spec's transition window).
const (
	subscribeEpochsBefore  = 2
	unsubscribeEpochsAfter = 2
	farFutureEpoch         = math.MaxUint64
)

// forkDigest is a digest and the epoch from which it applies, with the fork's name (for
// the layout of the light client messages under it).
type forkDigest struct {
	epoch  uint64
	digest [4]byte
	fork   string
}

// digestSchedule lists the digests of a network in order: one from genesis, and one from
// every scheduled fork and (from Fulu on) blob schedule entry that changes it.
func digestSchedule(cfg *params.ChainConfig, net Network) []forkDigest {
	epochs := []uint64{0}
	for _, f := range cfg.Forks {
		if f.Epoch != farFutureEpoch {
			epochs = append(epochs, f.Epoch)
		}
	}
	for _, bp := range net.BlobSchedule {
		if forkIndex(cfg.ForkAtEpoch(bp.Epoch).Name) >= forkIndex("fulu") {
			epochs = append(epochs, bp.Epoch)
		}
	}
	slices.Sort(epochs)
	epochs = slices.Compact(epochs)

	var sched []forkDigest
	for _, e := range epochs {
		d, _ := ForkDigest(cfg, net.BlobSchedule, net.ElectraBlobs, e)
		if len(sched) > 0 && sched[len(sched)-1].digest == d {
			continue
		}
		sched = append(sched, forkDigest{epoch: e, digest: d, fork: strings.ToLower(cfg.ForkAtEpoch(e).Name)})
	}
	return sched
}

// topicsAt returns the digest in force at an epoch, and the digests whose gossip topics a
// node should be subscribed to then: the current one, the next one from
// subscribeEpochsBefore epochs before it applies, the previous one until
// unsubscribeEpochsAfter epochs after the current one began.
func topicsAt(sched []forkDigest, epoch uint64) (current forkDigest, want [][4]byte) {
	i := 0
	for j, fd := range sched {
		if fd.epoch <= epoch {
			i = j
		}
	}
	current = sched[i]
	want = append(want, current.digest)
	if i > 0 && epoch < current.epoch+unsubscribeEpochsAfter {
		want = append(want, sched[i-1].digest)
	}
	if i+1 < len(sched) && sched[i+1].epoch <= epoch+subscribeEpochsBefore {
		want = append(want, sched[i+1].digest)
	}
	return current, want
}

// ParseNetwork reads the blob schedule of a network from its beacon chain config file
// (config.yaml: BLOB_SCHEDULE, ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA).
func ParseNetwork(file []byte) (Network, error) {
	var cfg struct {
		ElectraEpoch any `yaml:"ELECTRA_FORK_EPOCH"`
		ElectraBlobs any `yaml:"MAX_BLOBS_PER_BLOCK_ELECTRA"`
		Schedule     []struct {
			Epoch    any `yaml:"EPOCH"`
			MaxBlobs any `yaml:"MAX_BLOBS_PER_BLOCK"`
		} `yaml:"BLOB_SCHEDULE"`
	}
	if err := yaml.Unmarshal(file, &cfg); err != nil {
		return Network{}, err
	}
	var net Network
	var err error
	num := func(v any) uint64 {
		if err != nil || v == nil {
			return 0
		}
		var n uint64
		n, err = strconv.ParseUint(fmt.Sprint(v), 10, 64)
		return n
	}
	net.ElectraBlobs = BlobParams{num(cfg.ElectraEpoch), num(cfg.ElectraBlobs)}
	for _, s := range cfg.Schedule {
		net.BlobSchedule = append(net.BlobSchedule, BlobParams{num(s.Epoch), num(s.MaxBlobs)})
	}
	return net, err
}
