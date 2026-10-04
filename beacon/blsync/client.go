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
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"
	"strings"

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
	if prefetch := beaconBlockSync.prefetch; prefetch != nil {
		beaconBlockSync.payloadOfParent = func(slot uint64) bool { return !prefetch(slot) }
	}
	beaconBlockSync.trigger = scheduler.Trigger
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
	var fetchBlock func(types.ChainHeadEvent)
	if c.config.P2PBlocks {
		fetchBlock = c.blockSync.fetchBlock
	}
	c.engineClient = startEngineClient(c.config, c.engineRPC, headCh, fetchBlock)

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
	network, known := p2p.Networks[c.config.GenesisValidatorsRoot]
	enrs := c.config.P2PBootnodes
	if len(enrs) == 0 {
		enrs = network.Bootnodes
	}
	{
		for _, s := range enrs {
			if n, err := enode.Parse(enode.ValidSchemes, s); err == nil {
				boot = append(boot, n)
			}
		}
	}
	if len(boot) == 0 {
		return errors.New("--beacon.p2p: no consensus bootnodes for this network (built in for mainnet and sepolia; --beacon.p2p.bootnodes)")
	}
	if !known && c.config.ChainConfigFile != "" {
		// a custom network: its blob schedule from its config file
		file, err := os.ReadFile(c.config.ChainConfigFile)
		if err != nil {
			return err
		}
		net, err := p2p.ParseNetwork(file)
		if err != nil {
			return fmt.Errorf("--beacon.p2p: blob schedule of %s: %v", c.config.ChainConfigFile, err)
		}
		network.BlobSchedule, network.ElectraBlobs, known = net.BlobSchedule, net.ElectraBlobs, true
	}
	var override [4]byte
	if len(c.config.P2PDigest) == 4 {
		copy(override[:], c.config.P2PDigest)
	} else if !known {
		return errors.New("--beacon.p2p: unknown network, its blob schedule is needed for the fork digest (--beacon.config, or --beacon.p2p.forkdigest)")
	}
	var key *ecdsa.PrivateKey
	if c.config.P2PKeyFile != "" {
		var err error
		if key, err = p2p.LoadOrCreateKey(c.config.P2PKeyFile); err != nil {
			return fmt.Errorf("--beacon.p2p node key: %v", err)
		}
	}
	node, err := p2p.New(p2p.Config{
		PrivateKey:     key,
		Bootnodes:      boot,
		ListenPort:     c.config.P2PPort,
		TargetPeers:    c.config.P2PPeers,
		Chain:          &c.config.ChainConfig,
		Network:        network,
		DigestOverride: override,
		VerifyHeader: func(h types.SignedHeader) (bool, error) {
			ok, _, err := c.committees.VerifySignedHeader(h)
			return ok, err
		},
		GenesisTime: c.config.GenesisTime,
	})
	if err != nil {
		return err
	}
	server := request.NewServer(p2p.NewServer(node), &mclock.System{})
	c.scheduler.RegisterServer(server)
	if err := node.Start(); err != nil {
		c.scheduler.UnregisterServer(server)
		node.Stop()
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
