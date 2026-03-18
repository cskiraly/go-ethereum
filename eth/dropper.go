// Copyright 2025 The go-ethereum Authors
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

package eth

import (
	mrand "math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/eth/txtracker"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p"
)

const (
	// Interval between peer drop events (uniform between min and max)
	peerDropIntervalMin = 3 * time.Minute
	// Interval between peer drop events (uniform between min and max)
	peerDropIntervalMax = 7 * time.Minute
	// Avoid dropping peers for some time after connection
	doNotDropBefore = 10 * time.Minute
	// How close to max should we initiate the drop timer. O should be fine,
	// dropping when no more peers can be added. Larger numbers result in more
	// aggressive drop behavior.
	peerDropThreshold = 0
	// Fraction of total on-chain inclusions that protects a peer from being
	// dropped. Peers contributing to the top inclusionProtectionPct% of
	// inclusions are shielded.
	inclusionProtectionPct = 80
)

var (
	// droppedInbound is the number of inbound peers dropped
	droppedInbound = metrics.NewRegisteredMeter("eth/dropper/inbound", nil)
	// droppedOutbound is the number of outbound peers dropped
	droppedOutbound = metrics.NewRegisteredMeter("eth/dropper/outbound", nil)
	// droppedProtected is the number of times a drop was skipped because
	// all droppable peers were protected by inclusion share.
	droppedProtected = metrics.NewRegisteredMeter("eth/dropper/protected", nil)
)

// Callback type to get per-peer transaction statistics.
type getPeerStatsFunc func() map[string]txtracker.PeerStats

// dropper monitors the state of the peer pool and makes changes as follows:
//   - during sync the Downloader handles peer connections, so dropper is disabled
//   - if not syncing and the peer count is close to the limit, it drops peers
//     randomly every peerDropInterval to make space for new peers
//   - peers are dropped separately from the inboud pool and from the dialed pool
type dropper struct {
	maxDialPeers    int // maximum number of dialed peers
	maxInboundPeers int // maximum number of inbound peers
	peersFunc       getPeersFunc
	syncingFunc     getSyncingFunc
	peerStatsFunc   getPeerStatsFunc // optional: tx inclusion stats for protection

	// peerDropTimer introduces churn if we are close to limit capacity.
	// We handle Dialed and Inbound connections separately
	peerDropTimer *time.Timer

	wg         sync.WaitGroup // wg for graceful shutdown
	shutdownCh chan struct{}
}

// Callback type to get the list of connected peers.
type getPeersFunc func() []*p2p.Peer

// Callback type to get syncing status.
// Returns true while syncing, false when synced.
type getSyncingFunc func() bool

func newDropper(maxDialPeers, maxInboundPeers int) *dropper {
	cm := &dropper{
		maxDialPeers:    maxDialPeers,
		maxInboundPeers: maxInboundPeers,
		peerDropTimer:   time.NewTimer(randomDuration(peerDropIntervalMin, peerDropIntervalMax)),
		shutdownCh:      make(chan struct{}),
	}
	if peerDropIntervalMin > peerDropIntervalMax {
		panic("peerDropIntervalMin duration must be less than or equal to peerDropIntervalMax duration")
	}
	return cm
}

// Start the dropper. peerStatsFunc is optional (nil disables inclusion
// protection).
func (cm *dropper) Start(srv *p2p.Server, syncingFunc getSyncingFunc, peerStatsFunc getPeerStatsFunc) {
	cm.peersFunc = srv.Peers
	cm.syncingFunc = syncingFunc
	cm.peerStatsFunc = peerStatsFunc
	cm.wg.Add(1)
	go cm.loop()
}

// Stop the dropper.
func (cm *dropper) Stop() {
	cm.peerDropTimer.Stop()
	close(cm.shutdownCh)
	cm.wg.Wait()
}

// dropRandomPeer selects one of the peers randomly and drops it from the peer pool.
func (cm *dropper) dropRandomPeer() bool {
	peers := cm.peersFunc()
	var numInbound int
	for _, p := range peers {
		if p.Inbound() {
			numInbound++
		}
	}
	numDialed := len(peers) - numInbound

	selectDoNotDrop := func(p *p2p.Peer) bool {
		// Avoid dropping trusted and static peers, or recent peers.
		// Only drop peers if their respective category (dialed/inbound)
		// is close to limit capacity.
		return p.Trusted() || p.StaticDialed() ||
			p.Lifetime() < mclock.AbsTime(doNotDropBefore) ||
			(p.DynDialed() && cm.maxDialPeers-numDialed > peerDropThreshold) ||
			(p.Inbound() && cm.maxInboundPeers-numInbound > peerDropThreshold)
	}

	droppable := slices.DeleteFunc(peers, selectDoNotDrop)
	if len(droppable) == 0 {
		return false
	}
	// Protect peers that contribute to the top inclusionProtectionPct%
	// of on-chain transaction inclusions.
	if cm.peerStatsFunc != nil {
		droppable = cm.filterProtectedPeers(droppable)
		if len(droppable) == 0 {
			droppedProtected.Mark(1)
			return false
		}
	}
	p := droppable[mrand.Intn(len(droppable))]
	log.Debug("Dropping random peer", "inbound", p.Inbound(),
		"id", p.ID(), "duration", common.PrettyDuration(p.Lifetime()), "peercountbefore", len(peers))
	p.Disconnect(p2p.DiscUselessPeer)
	if p.Inbound() {
		droppedInbound.Mark(1)
	} else {
		droppedOutbound.Mark(1)
	}
	return true
}

// filterProtectedPeers removes peers from the droppable list that contribute
// to the top inclusionProtectionPct% of on-chain inclusions. Returns the
// filtered list.
func (cm *dropper) filterProtectedPeers(droppable []*p2p.Peer) []*p2p.Peer {
	stats := cm.peerStatsFunc()
	if len(stats) == 0 {
		return droppable
	}
	// Compute total inclusions across all peers.
	var totalIncluded int64
	for _, s := range stats {
		totalIncluded += s.Included
	}
	if totalIncluded == 0 {
		return droppable // no inclusion data yet
	}
	// Sort peer IDs by Included descending.
	type peerIncl struct {
		id       string
		included int64
	}
	sorted := make([]peerIncl, 0, len(stats))
	for id, s := range stats {
		if s.Included > 0 {
			sorted = append(sorted, peerIncl{id, s.Included})
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].included > sorted[j].included
	})
	// Walk sorted list, accumulating until threshold reached.
	threshold := totalIncluded * inclusionProtectionPct / 100
	protected := make(map[string]struct{})
	var accumulated int64
	for _, pi := range sorted {
		if accumulated >= threshold {
			break
		}
		protected[pi.id] = struct{}{}
		accumulated += pi.included
	}
	if len(protected) == 0 {
		return droppable
	}
	log.Debug("Protecting high-inclusion peers from drop",
		"protected", len(protected), "droppable", len(droppable),
		"threshold", threshold, "total", totalIncluded)

	// Filter out protected peers.
	result := droppable[:0]
	for _, p := range droppable {
		if _, ok := protected[p.ID().String()]; !ok {
			result = append(result, p)
		}
	}
	return result
}

// randomDuration generates a random duration between min and max.
func randomDuration(min, max time.Duration) time.Duration {
	if min > max {
		panic("min duration must be less than or equal to max duration")
	}
	if min == max {
		return min
	}
	return time.Duration(mrand.Int63n(int64(max-min)) + int64(min))
}

// loop is the main loop of the connection dropper.
func (cm *dropper) loop() {
	defer cm.wg.Done()

	for {
		select {
		case <-cm.peerDropTimer.C:
			// Drop a random peer if we are not syncing and the peer count is close to the limit.
			if !cm.syncingFunc() {
				cm.dropRandomPeer()
			}
			cm.peerDropTimer.Reset(randomDuration(peerDropIntervalMin, peerDropIntervalMax))
		case <-cm.shutdownCh:
			return
		}
	}
}
