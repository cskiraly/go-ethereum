// Copyright 2026 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// lcp2p is a probe: it joins the consensus layer's libp2p network as a light client
// and logs the light client updates gossiped there.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/ethereum/go-ethereum/beacon/light/p2p"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/libp2p/go-libp2p/core/peer"
)

func main() {
	var (
		port     = flag.Int("port", 9111, "TCP (libp2p) and UDP (discv5) port")
		peers    = flag.Int("peers", 20, "target peer count")
		fork     = flag.String("fork", "fulu", "current fork name, for decoding updates")
		duration = flag.Duration("duration", 10*time.Minute, "how long to run")
		verb     = flag.Int("verbosity", 3, "log level")
	)
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.FromLegacyLevel(*verb), true)))

	var boot []*enode.Node
	for _, s := range p2p.MainnetBootnodes {
		if n, err := enode.Parse(enode.ValidSchemes, s); err == nil {
			boot = append(boot, n)
		}
	}
	start := time.Now()
	var opt, fin int
	node, err := p2p.New(p2p.Config{
		Bootnodes: boot, ListenPort: *port, TargetPeers: *peers, ForkName: *fork,
		OnOptimisticUpdate: func(from peer.ID, u types.OptimisticUpdate) {
			opt++
			log.Info("Optimistic update", "attested", u.Attested.Slot, "signature", u.SignatureSlot,
				"exec", u.Attested.BlockHash(), "signers", u.Signature.SignerCount(), "from", from.ShortString())
		},
		OnFinalityUpdate: func(from peer.ID, u types.FinalityUpdate) {
			fin++
			log.Info("Finality update", "attested", u.Attested.Slot, "finalized", u.Finalized.Slot,
				"exec", u.Finalized.BlockHash(), "from", from.ShortString())
		},
	})
	if err != nil {
		log.Crit("Failed to create node", "err", err)
	}
	if err := node.Start(); err != nil {
		log.Crit("Failed to start node", "err", err)
	}
	stats := func() {
		in, out := node.Bandwidth()
		log.Info("Stats", "elapsed", time.Since(start).Round(time.Second), "peers", node.Peers(),
			"digest", fmt.Sprintf("%x", node.Digest()), "optimistic", opt, "finality", fin,
			"gossipKB", node.GossipBytes()/1024, "inKB", in>>10, "outKB", out>>10)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	tick := time.NewTicker(30 * time.Second)
	end := time.After(*duration)
loop:
	for {
		select {
		case <-tick.C:
			stats()
		case <-sig:
			break loop
		case <-end:
			break loop
		}
	}
	stats()
	node.Stop()
}
