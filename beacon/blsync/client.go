// Copyright 2024 The go-ethereum Authors
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

package blsync

import (
	"errors"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/beacon/light"
	"github.com/ethereum/go-ethereum/beacon/light/api"
	"github.com/ethereum/go-ethereum/beacon/light/p2p"
	"github.com/ethereum/go-ethereum/beacon/light/request"
	"github.com/ethereum/go-ethereum/beacon/light/sync"
	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	urls         []string
	customHeader map[string]string
	config       *params.ClientConfig
	scheduler    *request.Scheduler
	blockSync    *beaconBlockSync
	committees   *light.CommitteeChain
	engineRPC    *rpc.Client

	chainHeadSub event.Subscription
	engineClient *engineClient
	p2pNode      *p2p.Node
}

func NewClient(config params.ClientConfig) *Client {
	// create data structures
	var (
		db             = memorydb.New()
		committeeChain = light.NewCommitteeChain(db, &config.ChainConfig, config.Threshold, !config.NoFilter)
		headTracker    = light.NewHeadTracker(committeeChain, config.Threshold, func(checkpoint common.Hash) {
			if saved, err := config.SaveCheckpointToFile(checkpoint); saved {
				log.Debug("Saved beacon checkpoint", "file", config.CheckpointFile, "checkpoint", checkpoint)
			} else if err != nil {
				log.Error("Failed to save beacon checkpoint", "file", config.CheckpointFile, "checkpoint", checkpoint, "error", err)
			}
		})
	)
	headSync := sync.NewHeadSync(headTracker, committeeChain)

	// set up scheduler and sync modules
	scheduler := request.NewScheduler()
	checkpointInit := sync.NewCheckpointInit(committeeChain, config.Checkpoint)
	forwardSync := sync.NewForwardUpdateSync(committeeChain)
	beaconBlockSync := newBeaconBlockSync(headTracker, config.P2PBlocks)
	beaconBlockSync.prefetch = beforeGloas(&config.ChainConfig)
	scheduler.RegisterTarget(headTracker)
	scheduler.RegisterTarget(committeeChain)
	scheduler.RegisterModule(checkpointInit, "checkpointInit")
	scheduler.RegisterModule(forwardSync, "forwardSync")
	scheduler.RegisterModule(headSync, "headSync")
	scheduler.RegisterModule(beaconBlockSync, "beaconBlockSync")

	return &Client{
		scheduler:    scheduler,
		urls:         config.Apis,
		customHeader: config.CustomHeader,
		config:       &config,
		blockSync:    beaconBlockSync,
		committees:   committeeChain,
	}
}

func (c *Client) SetEngineRPC(engine *rpc.Client) {
	c.engineRPC = engine
}

func (c *Client) Start() error {
	headCh := make(chan types.ChainHeadEvent, 16)
	c.chainHeadSub = c.blockSync.SubscribeChainHead(headCh)
	c.engineClient = startEngineClient(c.config, c.engineRPC, headCh)

	c.scheduler.Start()
	for _, url := range c.urls {
		beaconApi := api.NewBeaconLightApi(url, c.customHeader)
		c.scheduler.RegisterServer(request.NewServer(api.NewApiServer(beaconApi), &mclock.System{}))
	}
	if c.config.P2P {
		if err := c.startP2P(); err != nil {
			return err
		}
	}
	return nil
}

// startP2P joins the consensus layer's libp2p network and registers it as a server.
func (c *Client) startP2P() error {
	var boot []*enode.Node
	if c.config.GenesisValidatorsRoot == params.MainnetLightConfig.GenesisValidatorsRoot {
		for _, s := range p2p.MainnetBootnodes {
			if n, err := enode.Parse(enode.ValidSchemes, s); err == nil {
				boot = append(boot, n)
			}
		}
	}
	if len(boot) == 0 {
		return errors.New("--beacon.p2p: no consensus bootnodes for this network (mainnet only so far)")
	}
	epoch := uint64(time.Now().Unix()-int64(c.config.GenesisTime)) / 12 / params.EpochLength
	digest, version := p2p.ForkDigest(&c.config.ChainConfig, p2p.MainnetBlobSchedule, p2p.MainnetElectraBlobs, epoch)
	node, err := p2p.New(p2p.Config{
		Bootnodes:   boot,
		ListenPort:  c.config.P2PPort,
		ForkName:    strings.ToLower(c.config.ForkAtEpoch(epoch).Name),
		ForkDigest:  digest,
		ForkVersion: version,
		VerifyHeader: func(h types.SignedHeader) (bool, error) {
			ok, _, err := c.committees.VerifySignedHeader(h)
			return ok, err
		},
		GenesisTime: c.config.GenesisTime,
	})
	if err != nil {
		return err
	}
	c.scheduler.RegisterServer(request.NewServer(p2p.NewServer(node), &mclock.System{}))
	if err := node.Start(); err != nil {
		return err
	}
	c.p2pNode = node
	log.Info("Following the consensus layer's libp2p network", "port", c.config.P2PPort)
	return nil
}

func (c *Client) Stop() error {
	if c.p2pNode != nil {
		c.p2pNode.Stop()
	}
	c.engineClient.stop()
	c.chainHeadSub.Unsubscribe()
	c.scheduler.Stop()
	return nil
}

// beforeGloas returns whether a slot is before the Gloas fork, from which on the execution
// payload of a block is published separately and later.
func beforeGloas(config *params.ChainConfig) func(slot uint64) bool {
	for _, fork := range config.Forks {
		if strings.EqualFold(fork.Name, "gloas") {
			gloas := fork.Epoch
			return func(slot uint64) bool { return slot/params.EpochLength < gloas }
		}
	}
	return nil
}
