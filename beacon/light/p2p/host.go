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

// Package p2p follows the consensus layer's libp2p network as a light client: it
// finds consensus nodes with discv5, keeps connections to some of them, answers the
// requests peers expect (status, ping, metadata), and receives the light client
// updates gossiped on the network.
package p2p

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"regexp"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/beacon/light/request"
	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/beacon/types"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	protoPrefix    = "/eth2/beacon_chain/req/"
	protoStatus1   = protoPrefix + "status/1/ssz_snappy"
	protoStatus2   = protoPrefix + "status/2/ssz_snappy"
	protoPing      = protoPrefix + "ping/1/ssz_snappy"
	protoMetadata2 = protoPrefix + "metadata/2/ssz_snappy"
	protoGoodbye   = protoPrefix + "goodbye/1/ssz_snappy"

	respTimeout = 10 * time.Second
)

// Config configures the light client's p2p node.
type Config struct {
	PrivateKey  *ecdsa.PrivateKey // the node's identity (discv5 and libp2p); random if nil
	Bootnodes   []*enode.Node
	ListenPort  int // TCP (libp2p) and UDP (discv5) port, and +1 UDP (libp2p over QUIC); 0: random
	TargetPeers int // peers to keep (default 10)
	// Chain is the beacon chain config (forks), Network the blob schedule: together they
	// give the fork digests over time, and the node switches digest (topics, status, ENR)
	// at each change without a restart.
	Chain   *params.ChainConfig
	Network Network
	// DigestOverride, if set, is the one digest used (for a network whose blob schedule is
	// unknown): no transitions then.
	DigestOverride [4]byte

	// OnOptimisticUpdate and OnFinalityUpdate receive the gossiped updates that pass the
	// gossip rules (Node.check), each once.
	OnOptimisticUpdate func(peer.ID, types.OptimisticUpdate)
	OnFinalityUpdate   func(peer.ID, types.FinalityUpdate)

	// VerifyHeader checks a signed header against the sync committee (blsync's committee
	// chain); it returns an error if it doesn't know the committee. With it, gossiped
	// updates that pass the checks are forwarded to other peers; without it, none are.
	VerifyHeader func(types.SignedHeader) (bool, error)
	// MinSigners is the fewest sync committee signers of a gossiped update to forward or
	// hand on, and to base the node's status on (blsync's --beacon.threshold).
	MinSigners  int
	GenesisTime uint64
}

// Node is a light client's node on the consensus layer's libp2p network.
type Node struct {
	cfg   Config
	key   *ecdsa.PrivateKey
	host  host.Host
	disc  *discover.UDPv5
	db    *enode.DB
	local *enode.LocalNode
	ps    *pubsub.PubSub
	bw    *metrics.BandwidthCounter

	sched        []forkDigest       // the network's digests over time
	forkByDigest map[[4]byte]string // for decoding by gossip topic or response context bytes

	mu       sync.Mutex
	current  forkDigest // the digest in force (status, ENR, dialing)
	accepted [][4]byte  // digests whose topics we are subscribed to (current ± transition)
	subs     map[[4]byte]*digestSubs
	status   []byte // the last status a peer returned (ours until updates are verified)
	dialing  map[peer.ID]bool
	lastDial time.Time
	backoff  map[peer.ID]time.Time // peers not to dial before a time (they turned us away)
	received atomic.Int64

	fwdMu      sync.Mutex
	optimistic gossipState // the optimistic update topic (keyed by the attested slot)
	finality   gossipState // the finality update topic (keyed by the finalized slot)
	forwarded  atomic.Int64
	served     atomic.Int64

	callbacks atomic.Pointer[callbacks] // from Config, or set by Server.Subscribe

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates the node; Start begins discovery and dialing.
func New(cfg Config) (*Node, error) {
	if cfg.TargetPeers == 0 {
		cfg.TargetPeers = 10
	}
	if cfg.Chain == nil {
		return nil, errors.New("no beacon chain config")
	}
	sched := digestSchedule(cfg.Chain, cfg.Network)
	if cfg.DigestOverride != ([4]byte{}) {
		fork := strings.ToLower(cfg.Chain.ForkAtEpoch(epochAt(cfg.Chain.GenesisTime, time.Now())).Name)
		sched = []forkDigest{{epoch: 0, digest: cfg.DigestOverride, fork: fork}}
	}
	forkByDigest := make(map[[4]byte]string)
	for _, fd := range sched {
		forkByDigest[fd.digest] = fd.fork
	}
	key := cfg.PrivateKey
	if key == nil {
		var err error
		if key, err = gcrypto.GenerateKey(); err != nil {
			return nil, err
		}
	}
	lkey, err := crypto.UnmarshalSecp256k1PrivateKey(gcrypto.FromECDSA(key))
	if err != nil {
		return nil, err
	}
	bw := metrics.NewBandwidthCounter()
	quicPort := 0
	if cfg.ListenPort != 0 {
		quicPort = cfg.ListenPort + 1
	}
	// libp2p's default limits scale with the machine (an eighth of its memory, half of the
	// file descriptor limit); in geth's process this node gets a small fixed budget, and
	// the connection manager trims connections above twice the peer target.
	limits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&limits)
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.Scale(128<<20, 512)))
	if err != nil {
		return nil, err
	}
	cm, err := connmgr.NewConnManager(cfg.TargetPeers, 2*cfg.TargetPeers, connmgr.WithGracePeriod(time.Minute))
	if err != nil {
		rm.Close()
		return nil, err
	}
	// QUIC as well as TCP: the consensus p2p spec prefers it, and it multiplexes by itself.
	// Over TCP both muxers: yamux, and mplex (outside go-libp2p proper), which some clients
	// (Lodestar) require.
	h, err := libp2p.New(
		libp2p.Identity(lkey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort), fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", quicPort)),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport), // some clients (Lodestar) only speak mplex over TCP
		libp2p.ResourceManager(rm),
		libp2p.ConnectionManager(cm),
		libp2p.DisableMetrics(), // not into the process-wide Prometheus registry
		libp2p.BandwidthReporter(bw),
		libp2p.DisableRelay(),
		libp2p.UserAgent("geth-blsync/p2p"),
	)
	if err != nil {
		cm.Close()
		rm.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, key: key, host: h, bw: bw, sched: sched, forkByDigest: forkByDigest,
		subs: make(map[[4]byte]*digestSubs), dialing: make(map[peer.ID]bool), backoff: make(map[peer.ID]time.Time),
		ctx: ctx, cancel: cancel}
	n.callbacks.Store(&callbacks{optimistic: cfg.OnOptimisticUpdate, finality: cfg.OnFinalityUpdate})
	handlers := map[string]network.StreamHandler{
		protoStatus1:      n.handleStatus,
		protoStatus2:      n.handleStatus,
		protoPing:         n.handlePing,
		protoMetadata2:    n.handleMetadata,
		protoGoodbye:      n.handleGoodbye,
		protoLCOptimistic: func(s network.Stream) { n.serveLatest(s, &n.optimistic) },
		protoLCFinality:   func(s network.Stream) { n.serveLatest(s, &n.finality) },
	}
	for p, handler := range handlers {
		h.SetStreamHandler(protocol.ID(p), func(s network.Stream) {
			defer recovered("stream handler "+p, func() { s.Reset() })
			handler(s)
		})
	}
	return n, nil
}

// recovered recovers from a panic in code that handles peers' input, logs it and calls
// fail. Neither libp2p nor gossipsub recovers panics in handlers and validators, and the
// node runs in geth's process: a bug there should cost the peer, not the process.
func recovered(what string, fail func()) {
	if r := recover(); r != nil {
		log.Error("Panic in the consensus p2p light client", "in", what, "err", r, "stack", string(debug.Stack()))
		if fail != nil {
			fail()
		}
	}
}

// callbacks are the receivers of the node's light client updates and request results.
type callbacks struct {
	optimistic func(peer.ID, types.OptimisticUpdate)
	finality   func(peer.ID, types.FinalityUpdate)
	event      func(request.Event) // request results (Server)
}

// Start begins discovery, dialing and the gossip subscriptions.
func (n *Node) Start() error {
	port, quicPort := n.cfg.ListenPort, 0
	for _, a := range n.host.Addrs() {
		if p, err := a.ValueForProtocol(ma.P_TCP); err == nil && port == 0 {
			fmt.Sscan(p, &port)
		}
		if _, err := a.ValueForProtocol(ma.P_QUIC_V1); err == nil {
			if p, err := a.ValueForProtocol(ma.P_UDP); err == nil {
				fmt.Sscan(p, &quicPort)
			}
		}
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: port})
	if err != nil {
		return err
	}
	if n.db, err = enode.OpenDB(""); err != nil {
		conn.Close()
		return err
	}
	n.local = enode.NewLocalNode(n.db, n.key)
	n.local.Set(enr.TCP(port))
	if quicPort != 0 {
		n.local.Set(enr.WithEntry("quic", uint16(quicPort)))
	}
	n.local.Set(enr.UDP(conn.LocalAddr().(*net.UDPAddr).Port))
	n.local.Set(enr.WithEntry("attnets", make([]byte, 8)))
	n.local.Set(enr.WithEntry("syncnets", make([]byte, 1)))
	if n.cfg.VerifyHeader != nil { // only a node that verifies updates keeps any to serve
		n.local.Set(enr.WithEntry(enrLightClientKey, []byte{0x01}))
	}
	// The consensus specs' gossipsub parameters. Peers are scored by the messages that
	// break the rules (invalid ones, which no honest peer sends: see check), so that a peer
	// sending them is soon ignored. Remote subscriptions are tracked for the light client
	// topics of the network's digests only.
	gsp := pubsub.DefaultGossipSubParams()
	gsp.D, gsp.Dlo, gsp.Dhi, gsp.Dlazy = 8, 6, 12, 6
	gsp.HeartbeatInterval = 700 * time.Millisecond
	gsp.FanoutTTL = time.Minute
	gsp.HistoryLength, gsp.HistoryGossip = 6, 3
	n.ps, err = pubsub.NewGossipSub(n.ctx, n.host,
		pubsub.WithGossipSubParams(gsp),
		pubsub.WithSeenMessagesTTL(2*params.EpochLength*12*time.Second),
		pubsub.WithMessageIdFn(func(m *pb.Message) string { return gossipMessageID(m.GetTopic(), m.Data) }),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(256),
		pubsub.WithValidateQueueSize(256),
		pubsub.WithPeerScore(newPeerScoreParams(), peerScoreThresholds),
		pubsub.WithSubscriptionFilter(pubsub.NewRegexpSubscriptionFilter(topicsRegexp(n.sched))),
	)
	if err != nil {
		conn.Close()
		return err
	}
	n.reconcile(epochAt(n.cfg.Chain.GenesisTime, time.Now())) // digest, ENR, topics
	n.disc, err = discover.ListenV5(conn, n.local, discover.Config{PrivateKey: n.key, Bootnodes: n.cfg.Bootnodes})
	if err != nil {
		conn.Close()
		return err
	}
	log.Info("Consensus p2p light client listening", "addr", fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", port, n.host.ID()))
	n.wg.Add(3)
	go n.dialLoop()
	go n.statsLoop()
	go n.forkLoop()
	return nil
}

// clientCounts summarizes the connected peers by client (from their identify agent version).
func (n *Node) clientCounts() string {
	counts := make(map[string]int)
	for _, id := range n.host.Network().Peers() {
		agent := "unknown"
		if v, err := n.host.Peerstore().Get(id, "AgentVersion"); err == nil {
			if a, ok := v.(string); ok && a != "" {
				agent = strings.ToLower(strings.SplitN(a, "/", 2)[0])
			}
		}
		counts[agent]++
	}
	names := make([]string, 0, len(counts))
	for a, c := range counts {
		names = append(names, fmt.Sprintf("%s:%d", a, c))
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// statsLoop logs peers, gossip and bandwidth every two minutes.
func (n *Node) statsLoop() {
	defer n.wg.Done()
	t := time.NewTicker(2 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			in, out := n.Bandwidth()
			log.Info("Consensus p2p light client", "peers", n.Peers(), "serving", n.servingPeers(), "clients", n.clientCounts(), "gossipKB", n.GossipBytes()>>10,
				"forwarded", n.Forwarded(), "served", n.served.Load(), "inKB", in>>10, "outKB", out>>10)
		}
	}
}

// Stop shuts the node down.
func (n *Node) Stop() {
	n.cancel()
	if n.disc != nil {
		n.disc.Close()
	}
	n.host.Close()
	n.wg.Wait()
	if n.db != nil {
		n.db.Close()
	}
}

// Peers returns the number of connected peers.
func (n *Node) Peers() int { return len(n.host.Network().Peers()) }

// Bandwidth returns the bytes received and sent so far.
func (n *Node) Bandwidth() (in, out int64) {
	t := n.bw.GetBandwidthTotals()
	return t.TotalIn, t.TotalOut
}

// Digest returns the fork digest in force.
func (n *Node) Digest() [4]byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.current.digest
}

// acceptsDigest reports whether a peer on digest d is useful now: the digest in force, or
// the neighbouring one within the transition window around a change.
func (n *Node) acceptsDigest(d [4]byte) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Contains(n.accepted, d)
}

// forkOf returns the fork of a digest (for decoding), or the current fork.
func (n *Node) forkOf(d [4]byte) string {
	if f, ok := n.forkByDigest[d]; ok {
		return f
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.current.fork
}

// nodeEth2 returns the `eth2` ENR entry of a node, if it has one.
func nodeEth2(nd *enode.Node) []byte {
	var eth2 []byte
	if nd.Load(enr.WithEntry("eth2", &eth2)) != nil || len(eth2) < 16 {
		return nil
	}
	return eth2
}

// dialLoop dials the bootnodes, then walks discovered nodes and keeps TargetPeers
// connections to nodes on an accepted digest.
func (n *Node) dialLoop() {
	defer n.wg.Done()
	it := n.disc.RandomNodes()
	defer it.Close()
	go func() { <-n.ctx.Done(); it.Close() }() // unblocks it.Next on Stop

	usable := func(nd *enode.Node) bool {
		eth2 := nodeEth2(nd)
		return eth2 != nil && nd.TCP() != 0 && nd.IP() != nil && n.acceptsDigest([4]byte(eth2[:4]))
	}
	// On a small network, random lookups may never return the bootnodes themselves.
	for _, nd := range n.cfg.Bootnodes {
		if usable(nd) {
			n.dial(nd)
		}
	}
	for it.Next() {
		if n.ctx.Err() != nil {
			return
		}
		nd := it.Node()
		if !usable(nd) {
			continue
		}
		for !n.canDial() {
			select {
			case <-n.ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		n.dial(nd)
	}
}

// dialInterval is the pace of dialing above half the peer target.
const dialInterval = 10 * time.Second

// canDial reports whether to dial another node. Below half the peer target, up to the
// target's number of dials run at a time; above it, one every dialInterval: most nodes
// (full ones at their peer limit) end the connection with a "too many peers" goodbye, and
// each try costs the handshakes' traffic, which then makes up most of the node's. At the
// target, a peer that serves no light client data makes room.
func (n *Node) canDial() bool {
	peers := n.Peers()
	if peers >= n.cfg.TargetPeers {
		return n.dropNonServing()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if peers >= n.cfg.TargetPeers/2 {
		return len(n.dialing) == 0 && time.Since(n.lastDial) >= dialInterval
	}
	return len(n.dialing) < n.cfg.TargetPeers
}

// servingPeers counts the connected peers that serve light client data: those whose
// identify protocol list includes updates by range, which a light client server has and
// blsync needs (nodes like this one serve only the latest updates).
func (n *Node) servingPeers() (count int) {
	for _, id := range n.host.Network().Peers() {
		if protos, err := n.host.Peerstore().GetProtocols(id); err == nil && slices.Contains(protos, protocol.ID(protoLCUpdates)) {
			count++
		}
	}
	return count
}

// dropNonServing disconnects a peer that serves no light client data (connected for a
// minute, and its identify protocol list lacks updates by range), to make room for one
// that does; it isn't dialed again for an hour. Most such peers (many nodes don't run a
// light client server) don't carry the light client topics either.
func (n *Node) dropNonServing() bool {
	for _, id := range n.host.Network().Peers() {
		conns := n.host.Network().ConnsToPeer(id)
		if len(conns) == 0 || time.Since(conns[0].Stat().Opened) < time.Minute {
			continue
		}
		protos, err := n.host.Peerstore().GetProtocols(id)
		if err != nil || len(protos) == 0 || slices.Contains(protos, protocol.ID(protoLCUpdates)) {
			continue
		}
		n.avoid(id, time.Hour)
		n.host.Network().ClosePeer(id)
		return true
	}
	return false
}

// maxBackoff bounds the peers kept from being dialed.
const maxBackoff = 10000

// avoid keeps a peer from being dialed for a while.
func (n *Node) avoid(id peer.ID, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.backoff) >= maxBackoff {
		// forget the expired ones, and while still too many, any (map order is random)
		now := time.Now()
		for id, until := range n.backoff {
			if now.After(until) || len(n.backoff) > maxBackoff*9/10 {
				delete(n.backoff, id)
			}
		}
	}
	n.backoff[id] = time.Now().Add(d)
}

// dial connects to a discovered node in the background and exchanges status.
func (n *Node) dial(nd *enode.Node) {
	pub := nd.Pubkey()
	if pub == nil {
		return
	}
	lpub, err := crypto.UnmarshalSecp256k1PublicKey(gcrypto.CompressPubkey(pub))
	if err != nil {
		return
	}
	id, err := peer.IDFromPublicKey(lpub)
	if err != nil || n.host.Network().Connectedness(id) == network.Connected {
		return
	}
	n.mu.Lock()
	if n.dialing[id] || time.Now().Before(n.backoff[id]) {
		n.mu.Unlock()
		return
	}
	n.dialing[id] = true
	n.lastDial = time.Now()
	n.mu.Unlock()
	addrs := make([]ma.Multiaddr, 0, 2)
	var quicPort uint16
	if nd.Load(enr.WithEntry("quic", &quicPort)) == nil && quicPort != 0 {
		if a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", nd.IP(), quicPort)); err == nil {
			addrs = append(addrs, a)
		}
	}
	if a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", nd.IP(), nd.TCP())); err == nil {
		addrs = append(addrs, a)
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer func() { n.mu.Lock(); delete(n.dialing, id); n.mu.Unlock() }()
		defer recovered("dial", func() { n.host.Network().ClosePeer(id) })
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		defer cancel()
		if err := n.host.Connect(ctx, peer.AddrInfo{ID: id, Addrs: addrs}); err != nil {
			log.Trace("Dial failed", "peer", id, "err", err)
			return
		}
		if err := n.exchangeStatus(id); err != nil {
			log.Debug("Status exchange failed", "peer", id, "err", err)
			n.avoid(id, 10*time.Minute)
			n.host.Network().ClosePeer(id)
			return
		}
		log.Debug("Connected to consensus peer", "peer", id, "peers", n.Peers())
	}()
}

// ourStatus is the status we send: the finalized checkpoint and the head of the last
// verified light client updates; before those, the fields of the last status a peer sent
// us, or genesis values before the first one.
func (n *Node) ourStatus(v2 bool) []byte {
	n.fwdMu.Lock()
	fin, head := n.finality.header, n.optimistic.header
	n.fwdMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()
	st := make([]byte, 84, 92)
	if fin != (types.Header{}) && head != (types.Header{}) {
		// The checkpoint's epoch is the first starting at or after the finalized block
		// (the checkpoint root is the last block at the epoch's start).
		root := fin.Hash()
		copy(st[4:36], root[:])
		binary.LittleEndian.PutUint64(st[36:44], (fin.Slot+params.EpochLength-1)/params.EpochLength)
		root = head.Hash()
		copy(st[44:76], root[:])
		binary.LittleEndian.PutUint64(st[76:84], head.Slot)
	} else if n.status != nil {
		copy(st, n.status[:84])
	}
	copy(st[0:4], n.current.digest[:])
	if v2 {
		// earliest_available_slot: we store no blocks; claim our head slot
		st = st[:92]
		copy(st[84:92], st[76:84])
	}
	return st
}

// exchangeStatus sends our status to a peer and records theirs.
func (n *Node) exchangeStatus(id peer.ID) error {
	ctx, cancel := context.WithTimeout(n.ctx, respTimeout)
	defer cancel()
	s, err := n.host.NewStream(ctx, id, protoStatus2, protoStatus1)
	if err != nil {
		return err
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	v2 := s.Protocol() == protoStatus2
	if err := writeSSZ(s, n.ourStatus(v2)); err != nil {
		return err
	}
	s.CloseWrite()
	_, resp, err := readResponse(bufio.NewReader(s), 0, maxStatusSize)
	if err != nil {
		return err
	}
	if len(resp) < 84 {
		return fmt.Errorf("short status (%d bytes)", len(resp))
	}
	if !n.acceptsDigest([4]byte(resp[:4])) {
		return fmt.Errorf("peer on fork digest %x", resp[:4])
	}
	n.mu.Lock()
	n.status = append([]byte(nil), resp[:84]...)
	n.mu.Unlock()
	return nil
}

func (n *Node) handleStatus(s network.Stream) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	if _, err := readSSZ(bufio.NewReader(s), maxStatusSize); err != nil {
		s.Reset()
		return
	}
	writeResponse(s, nil, n.ourStatus(s.Protocol() == protoStatus2))
}

func (n *Node) handlePing(s network.Stream) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	if _, err := readSSZ(bufio.NewReader(s), maxPingSize); err != nil {
		s.Reset()
		return
	}
	var seq [8]byte
	binary.LittleEndian.PutUint64(seq[:], 1)
	writeResponse(s, nil, seq[:])
}

// handleMetadata answers metadata in version 2 (seq_number, attnets, syncnets), without
// the custody group count Fulu added in version 3, and the ENR has no `cgc` either: the
// node custodies no data columns, and a count below CUSTODY_REQUIREMENT gets it rejected
// (Lighthouse bans it as faulty for 12 hours), so it makes no claim. Peers then assume
// their default: Lighthouse asks for version 3, 2 or 1; Prysm and Nimbus fall back to
// CUSTODY_REQUIREMENT.
func (n *Node) handleMetadata(s network.Stream) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	md := make([]byte, 17)
	binary.LittleEndian.PutUint64(md[0:8], 1)
	writeResponse(s, nil, md)
}

func (n *Node) handleGoodbye(s network.Stream) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	if b, err := readSSZ(bufio.NewReader(s), maxPingSize); err == nil && len(b) == 8 {
		reason := binary.LittleEndian.Uint64(b)
		log.Debug("Peer said goodbye", "peer", s.Conn().RemotePeer(), "reason", reason)
		backoff := time.Hour // irrelevant network, fault, bad score, banned...
		switch reason {
		case 1: // client shut down
			backoff = time.Minute
		case 129: // too many peers
			backoff = 10 * time.Minute
		}
		n.avoid(s.Conn().RemotePeer(), backoff)
	}
}

// digestSubs are the gossip subscriptions of one digest.
type digestSubs struct {
	topics []*pubsub.Topic
	subs   []*pubsub.Subscription
}

var lightClientTopics = []string{"light_client_optimistic_update", "light_client_finality_update"}

// topicsRegexp matches the light client topics of a network's digests.
func topicsRegexp(sched []forkDigest) *regexp.Regexp {
	digests := make([]string, len(sched))
	for i, fd := range sched {
		digests[i] = fmt.Sprintf("%x", fd.digest)
	}
	return regexp.MustCompile(fmt.Sprintf("^/eth2/(%s)/(%s)/ssz_snappy$", strings.Join(digests, "|"), strings.Join(lightClientTopics, "|")))
}

// Peer scoring counts only malformed messages (the rejected ones, see onOptimistic), which
// honest peers don't send: a peer sending about a dozen within an hour is ignored
// (graylisted) until the count decays. Gossipsub adds each
// joined topic's parameters to the node's PeerScoreParams (newPeerScoreParams).
func newPeerScoreParams() *pubsub.PeerScoreParams {
	return &pubsub.PeerScoreParams{
		SkipAtomicValidation: true,
		Topics:               make(map[string]*pubsub.TopicScoreParams),
		AppSpecificScore:     func(peer.ID) float64 { return 0 },
		DecayInterval:        12 * time.Second,
		DecayToZero:          0.01,
		RetainScore:          time.Hour,
	}
}

var (
	peerScoreThresholds = &pubsub.PeerScoreThresholds{
		SkipAtomicValidation: true,
		GossipThreshold:      -4000,
		PublishThreshold:     -8000,
		GraylistThreshold:    -16000,
	}
	topicScoreParams = &pubsub.TopicScoreParams{
		SkipAtomicValidation:           true,
		TopicWeight:                    1,
		TimeInMeshQuantum:              12 * time.Second, // unused (no weight), but gossipsub divides by it
		InvalidMessageDeliveriesWeight: -100,             // times the count squared: 13 reach the graylist
		InvalidMessageDeliveriesDecay:  0.997,            // per slot: the count halves in about 45 minutes
	}
)

// subscribeDigest subscribes to the light client update topics of a digest; messages on
// them are decoded with that digest's fork.
func (n *Node) subscribeDigest(d [4]byte) (*digestSubs, error) {
	fork := n.forkOf(d)
	ds := new(digestSubs)
	for _, name := range lightClientTopics {
		handle := n.onOptimistic
		if name == "light_client_finality_update" {
			handle = n.onFinality
		}
		topic := fmt.Sprintf("/eth2/%x/%s/ssz_snappy", d[:], name)
		// Updates that pass the gossip rules (check) are forwarded and handed to blsync,
		// which checks them again.
		if err := n.ps.RegisterTopicValidator(topic, func(_ context.Context, from peer.ID, m *pubsub.Message) (res pubsub.ValidationResult) {
			defer recovered("gossip validator", func() { res = pubsub.ValidationIgnore })
			if !decodable(fork) {
				return pubsub.ValidationIgnore // not the peer's fault
			}
			ssz, err := decodeGossip(m.Data)
			if err != nil {
				return pubsub.ValidationReject
			}
			n.received.Add(int64(len(m.Data)))
			res, err = handle(m.ReceivedFrom, fork, d, ssz)
			if err != nil {
				log.Debug("Bad light client update", "topic", topic, "peer", m.ReceivedFrom, "err", err)
			}
			if res == pubsub.ValidationAccept {
				n.forwarded.Add(1)
			}
			return res
		}); err != nil {
			n.unsubscribe(ds)
			return nil, err
		}
		t, err := n.ps.Join(topic)
		if err == nil {
			if err = t.SetScoreParams(topicScoreParams); err != nil {
				t.Close()
			}
		}
		if err != nil {
			n.ps.UnregisterTopicValidator(topic)
			n.unsubscribe(ds)
			return nil, err
		}
		sub, err := t.Subscribe()
		if err != nil {
			t.Close()
			n.ps.UnregisterTopicValidator(topic)
			n.unsubscribe(ds)
			return nil, err
		}
		ds.topics, ds.subs = append(ds.topics, t), append(ds.subs, sub)
		n.wg.Add(1)
		go func() { // drain: messages are handled in the validator
			defer n.wg.Done()
			for {
				if _, err := sub.Next(n.ctx); err != nil {
					return
				}
			}
		}()
	}
	log.Info("Subscribed to light client gossip", "digest", fmt.Sprintf("%x", d), "fork", fork)
	return ds, nil
}

// unsubscribe cancels a digest's subscriptions and leaves its topics.
func (n *Node) unsubscribe(ds *digestSubs) {
	for _, sub := range ds.subs {
		sub.Cancel()
	}
	for _, t := range ds.topics {
		n.ps.UnregisterTopicValidator(t.String())
		t.Close()
	}
}

// reconcile brings the node to the state an epoch calls for: the digest in force (status,
// ENR with the next fork) and the gossip subscriptions, including the transition window
// around a digest change.
func (n *Node) reconcile(epoch uint64) {
	current, want := topicsAt(n.sched, epoch)
	n.mu.Lock()
	switched := current.digest != n.current.digest
	prev := n.current
	n.current, n.accepted = current, want
	n.mu.Unlock()
	if switched {
		nextVersion, nextEpoch := NextFork(n.cfg.Chain, epoch)
		if n.cfg.DigestOverride != ([4]byte{}) {
			nextVersion, nextEpoch = [4]byte(n.cfg.Chain.ForkAtEpoch(epoch).Version), farFutureEpoch
		}
		eth2 := append(append([]byte{}, current.digest[:]...), nextVersion[:]...)
		n.local.Set(enr.WithEntry("eth2", binary.LittleEndian.AppendUint64(eth2, nextEpoch)))
		if prev.digest != ([4]byte{}) {
			log.Info("Switched to the next fork digest", "epoch", epoch, "digest", fmt.Sprintf("%x", current.digest), "fork", current.fork, "previous", fmt.Sprintf("%x", prev.digest))
		}
	}
	for _, d := range want {
		if n.subs[d] != nil {
			continue
		}
		ds, err := n.subscribeDigest(d)
		if err != nil {
			log.Error("Failed to subscribe to light client gossip", "digest", fmt.Sprintf("%x", d), "err", err)
			continue
		}
		n.subs[d] = ds
	}
	for d, ds := range n.subs {
		if !slices.Contains(want, d) {
			n.unsubscribe(ds)
			delete(n.subs, d)
			log.Info("Left light client gossip", "digest", fmt.Sprintf("%x", d))
		}
	}
}

// forkLoop reconciles the node with the epoch once per slot.
func (n *Node) forkLoop() {
	defer n.wg.Done()
	t := time.NewTicker(12 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case now := <-t.C:
			n.reconcile(epochAt(n.cfg.Chain.GenesisTime, now))
		}
	}
}

// epochAt is the epoch at a time, for a genesis time.
func epochAt(genesis uint64, t time.Time) uint64 {
	if t.Unix() < int64(genesis) {
		return 0
	}
	return uint64(t.Unix()-int64(genesis)) / 12 / params.EpochLength
}

// onOptimistic handles a gossiped optimistic update. Only a malformed one is rejected
// (which lowers its sender's peer score): around a fork, geth's checks of the Merkle
// proofs and the signature can fail updates that are valid (the proof of a pre-fork header
// in the new format; the signature domain, which geth takes from the attested header's
// epoch, the spec from the signature slot's), so those are ignored.
func (n *Node) onOptimistic(from peer.ID, fork string, digest [4]byte, ssz []byte) (pubsub.ValidationResult, error) {
	u, err := DecodeOptimisticUpdate(fork, ssz)
	if err == nil {
		err = optimisticSlots(&u)
	}
	if err != nil {
		return pubsub.ValidationReject, err
	}
	if err := u.Validate(); err != nil {
		return pubsub.ValidationIgnore, err
	}
	res, deliver, err := n.check(&n.optimistic, u.SignedHeader(), u.Attested.Header, false, servedUpdate{digest, ssz})
	if cb := n.callbacks.Load().optimistic; deliver && cb != nil {
		cb(from, u)
	}
	return res, err
}

// onFinality handles a gossiped finality update (rejected only if malformed, as in
// onOptimistic).
func (n *Node) onFinality(from peer.ID, fork string, digest [4]byte, ssz []byte) (pubsub.ValidationResult, error) {
	u, err := DecodeFinalityUpdate(fork, ssz)
	if err == nil {
		err = finalitySlots(&u)
	}
	if err != nil {
		return pubsub.ValidationReject, err
	}
	if err := u.Validate(); err != nil {
		return pubsub.ValidationIgnore, err
	}
	super := u.Signature.SignerCount() >= params.SyncCommitteeSupermajority
	res, deliver, err := n.check(&n.finality, u.SignedHeader(), u.Finalized.Header, super, servedUpdate{digest, ssz})
	if cb := n.callbacks.Load().finality; deliver && cb != nil {
		cb(from, u)
	}
	return res, err
}

// gossipState is what the gossip rules of a light client update topic remember.
type gossipState struct {
	forwarded uint64       // key of the last update forwarded
	header    types.Header // the header of that key (status)
	super     bool         // that update had a sync committee supermajority
	pending   uint64       // key of the last update handed on unverified
	served    servedUpdate // the last update forwarded, served on request
}

// maxClockDisparity is MAXIMUM_GOSSIP_CLOCK_DISPARITY.
const maxClockDisparity = 500 * time.Millisecond

var (
	errBadSignature     = errors.New("invalid sync committee signature")
	errNoCommitteeChain = errors.New("no committee chain")
)

// check applies the gossip rules of a light client update topic (the consensus specs'
// light client p2p interface) to an update whose slots and Merkle proofs are valid; a
// light client checks the sync committee signature where a full node compares the update
// with the one it computed:
//   - it is received no earlier than a third into its signature slot;
//   - it is newer than the last update forwarded: its key (the attested slot of an
//     optimistic update, the finalized slot of a finality update) is greater, or equal
//     with a sync committee supermajority (super) that the last one hadn't;
//   - it has the signers blsync requires (MinSigners), and the sync committee signed it.
//
// Updates that fail are ignored (see onOptimistic).
// An update that passes is forwarded, kept for serving and handed on (deliver). While the
// committee of its period isn't known (blsync still syncing its committee chain, or no
// committee chain at all), a new update is handed on once per key, unverified, and not
// forwarded: blsync keeps it until it can verify it.
func (n *Node) check(st *gossipState, head types.SignedHeader, header types.Header, super bool, u servedUpdate) (res pubsub.ValidationResult, deliver bool, err error) {
	key := header.Slot
	due := time.Unix(int64(n.cfg.GenesisTime+head.SignatureSlot*12), 0).Add(4*time.Second - maxClockDisparity)
	if time.Now().Before(due) || head.Signature.SignerCount() < n.cfg.MinSigners {
		return pubsub.ValidationIgnore, false, nil
	}
	n.fwdMu.Lock()
	defer n.fwdMu.Unlock()
	if key < st.forwarded || (key == st.forwarded && (!super || st.super)) {
		return pubsub.ValidationIgnore, false, nil
	}
	ok, err := false, errNoCommitteeChain
	if n.cfg.VerifyHeader != nil {
		ok, err = n.cfg.VerifyHeader(head)
	}
	switch {
	case err != nil:
		if key <= st.pending {
			return pubsub.ValidationIgnore, false, nil
		}
		st.pending = key
		return pubsub.ValidationIgnore, true, nil
	case !ok:
		return pubsub.ValidationIgnore, false, errBadSignature
	}
	st.forwarded, st.header, st.super, st.served = key, header, super, u
	return pubsub.ValidationAccept, true, nil
}

// ENR key advertising which light client data this node serves over req/resp: a bitfield,
// bit 0 the latest optimistic and finality updates (bit 1 updates by range, bit 2
// bootstraps: not served yet). Not a consensus-spec key: other clients ignore it.
const enrLightClientKey = "lc"

// servedUpdate is a light client update kept for serving: its SSZ and the digest of the
// topic it came on (the response's context bytes).
type servedUpdate struct {
	digest [4]byte
	ssz    []byte
}

// serveLatest answers a light client optimistic or finality update request with the last
// verified update of that kind, or "resource unavailable" (3) without one.
func (n *Node) serveLatest(s network.Stream, st *gossipState) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	n.fwdMu.Lock()
	u := st.served
	n.fwdMu.Unlock()
	if u.ssz == nil {
		s.Write([]byte{3})
		writeSSZ(s, []byte("no update yet"))
		return
	}
	n.served.Add(1)
	writeResponse(s, u.digest[:], u.ssz)
}

// Forwarded returns the number of light client updates forwarded to other peers.
func (n *Node) Forwarded() int64 { return n.forwarded.Load() }

// GossipBytes returns the compressed bytes of light client gossip received.
func (n *Node) GossipBytes() int64 { return n.received.Load() }
