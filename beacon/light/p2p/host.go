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
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/beacon/light/request"
	"github.com/ethereum/go-ethereum/beacon/types"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	protoPrefix    = "/eth2/beacon_chain/req/"
	protoStatus1   = protoPrefix + "status/1/ssz_snappy"
	protoStatus2   = protoPrefix + "status/2/ssz_snappy"
	protoPing      = protoPrefix + "ping/1/ssz_snappy"
	protoMetadata2 = protoPrefix + "metadata/2/ssz_snappy"
	protoMetadata3 = protoPrefix + "metadata/3/ssz_snappy"
	protoGoodbye   = protoPrefix + "goodbye/1/ssz_snappy"

	respTimeout = 10 * time.Second
)

// Config configures the light client's p2p node.
type Config struct {
	Bootnodes   []*enode.Node
	ListenPort  int // TCP (libp2p) and UDP (discv5) port; 0: random
	TargetPeers int
	// ForkName names the current fork, for decoding the updates ("fulu", ...).
	ForkName string
	// ForkDigest is the current fork digest (see ForkDigest), ForkVersion the current fork
	// version. Zero digest: learned from the ENRs of discovered nodes (the most common one;
	// unreliable, since many nodes' ENRs are stale).
	ForkDigest  [4]byte
	ForkVersion [4]byte

	OnOptimisticUpdate func(peer.ID, types.OptimisticUpdate)
	OnFinalityUpdate   func(peer.ID, types.FinalityUpdate)

	// VerifyHeader checks a signed header against the sync committee (blsync's committee
	// chain). With it, gossiped updates that pass the checks are forwarded to other peers;
	// without it, none are.
	VerifyHeader func(types.SignedHeader) (bool, error)
	GenesisTime  uint64
}

// Node is a light client's node on the consensus layer's libp2p network.
type Node struct {
	cfg   Config
	key   *ecdsa.PrivateKey
	host  host.Host
	disc  *discover.UDPv5
	local *enode.LocalNode
	ps    *pubsub.PubSub
	bw    *metrics.BandwidthCounter

	mu       sync.Mutex
	eth2     []byte // our ENR `eth2` entry (fork digest, next fork version, next fork epoch)
	digest   [4]byte
	status   []byte // status we send: the last one a peer returned, with our digest
	dialing  map[peer.ID]bool
	received atomic.Int64

	fwdMu                 sync.Mutex
	fwdOptimistic, fwdFin uint64 // attested / finalized slot of the last update forwarded
	lastOptimistic        []byte // SSZ of the last optimistic update forwarded, served on request
	lastFinality          []byte // same for the finality update
	forwarded             atomic.Int64
	served                atomic.Int64

	eventCallback func(request.Event) // set by Server.Subscribe

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates the node; Start begins discovery and dialing.
func New(cfg Config) (*Node, error) {
	if cfg.TargetPeers == 0 {
		cfg.TargetPeers = 20
	}
	key, err := gcrypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	lkey, err := crypto.UnmarshalSecp256k1PrivateKey(gcrypto.FromECDSA(key))
	if err != nil {
		return nil, err
	}
	bw := metrics.NewBandwidthCounter()
	h, err := libp2p.New(
		libp2p.Identity(lkey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort)),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.BandwidthReporter(bw),
		libp2p.DisableRelay(),
		libp2p.UserAgent("geth-blsync/p2p"),
	)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, key: key, host: h, bw: bw, dialing: make(map[peer.ID]bool), ctx: ctx, cancel: cancel}
	for _, p := range []string{protoStatus1, protoStatus2} {
		h.SetStreamHandler(protocol.ID(p), n.handleStatus)
	}
	h.SetStreamHandler(protoPing, n.handlePing)
	h.SetStreamHandler(protoMetadata2, n.handleMetadata)
	h.SetStreamHandler(protoMetadata3, n.handleMetadata)
	h.SetStreamHandler(protoGoodbye, n.handleGoodbye)
	h.SetStreamHandler(protoLCOptimistic, func(s network.Stream) { n.serveLatest(s, &n.lastOptimistic) })
	h.SetStreamHandler(protoLCFinality, func(s network.Stream) { n.serveLatest(s, &n.lastFinality) })
	return n, nil
}

// Start begins discovery, dialing and the gossip subscriptions.
func (n *Node) Start() error {
	port := n.cfg.ListenPort
	if port == 0 {
		if p, err := n.host.Addrs()[0].ValueForProtocol(ma.P_TCP); err == nil {
			fmt.Sscan(p, &port)
		}
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: port})
	if err != nil {
		return err
	}
	db, err := enode.OpenDB("")
	if err != nil {
		return err
	}
	n.local = enode.NewLocalNode(db, n.key)
	n.local.Set(enr.TCP(port))
	n.local.Set(enr.UDP(conn.LocalAddr().(*net.UDPAddr).Port))
	n.local.Set(enr.WithEntry("attnets", make([]byte, 8)))
	n.local.Set(enr.WithEntry("syncnets", make([]byte, 1)))
	if n.cfg.VerifyHeader != nil { // only a node that verifies updates keeps any to serve
		n.local.Set(enr.WithEntry(enrLightClientKey, []byte{0x01}))
	}
	if n.cfg.ForkDigest != ([4]byte{}) {
		// ENRForkID: fork digest, next fork version, next fork epoch (none known here)
		eth2 := append(append(n.cfg.ForkDigest[:0:0], n.cfg.ForkDigest[:]...), n.cfg.ForkVersion[:]...)
		n.setEth2(append(eth2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))
	}
	n.disc, err = discover.ListenV5(conn, n.local, discover.Config{PrivateKey: n.key, Bootnodes: n.cfg.Bootnodes})
	if err != nil {
		return err
	}
	log.Info("Consensus p2p light client listening", "addr", fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", port, n.host.ID()))
	n.wg.Add(2)
	go n.dialLoop()
	go n.statsLoop()
	return nil
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
			log.Info("Consensus p2p light client", "peers", n.Peers(), "gossipKB", n.GossipBytes()>>10,
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
}

// Peers returns the number of connected peers.
func (n *Node) Peers() int { return len(n.host.Network().Peers()) }

// Bandwidth returns the bytes received and sent so far.
func (n *Node) Bandwidth() (in, out int64) {
	t := n.bw.GetBandwidthTotals()
	return t.TotalIn, t.TotalOut
}

// Digest returns the fork digest in use (zero until learned).
func (n *Node) Digest() [4]byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.digest
}

func (n *Node) setEth2(eth2 []byte) {
	n.mu.Lock()
	n.eth2 = eth2
	copy(n.digest[:], eth2[:4])
	n.mu.Unlock()
	n.local.Set(enr.WithEntry("eth2", eth2))
}

// nodeEth2 returns the `eth2` ENR entry of a node, if it has one.
func nodeEth2(nd *enode.Node) []byte {
	var eth2 []byte
	if nd.Load(enr.WithEntry("eth2", &eth2)) != nil || len(eth2) < 16 {
		return nil
	}
	return eth2
}

// dialLoop walks discovered nodes, learns the fork digest if needed, and keeps
// TargetPeers connections to nodes on it.
func (n *Node) dialLoop() {
	defer n.wg.Done()
	it := n.disc.RandomNodes()
	defer it.Close()
	go func() { <-n.ctx.Done(); it.Close() }() // unblocks it.Next on Stop
	var (
		seen   = make(map[string]int)    // eth2 entry → count (while learning)
		sample = make(map[string][]byte) // eth2 entry
		learnt = n.Digest() != [4]byte{}
		joined bool
	)
	for it.Next() {
		if n.ctx.Err() != nil {
			return
		}
		nd := it.Node()
		eth2 := nodeEth2(nd)
		if eth2 == nil || nd.TCP() == 0 || nd.IP() == nil {
			continue
		}
		if !learnt {
			k := hex.EncodeToString(eth2[:16])
			seen[k]++
			sample[k] = eth2[:16]
			if len(seen) > 0 && sum(seen) >= 20 {
				best := ""
				for k, c := range seen {
					if best == "" || c > seen[best] {
						best = k
					}
				}
				n.setEth2(sample[best])
				log.Info("Learned the fork digest from discovered nodes", "digest", hex.EncodeToString(sample[best][:4]), "votes", seen[best], "of", sum(seen))
				learnt = true
			}
			continue
		}
		if !joined {
			if err := n.joinGossip(); err != nil {
				log.Error("Failed to join gossip", "err", err)
				return
			}
			joined = true
		}
		d := n.Digest()
		if [4]byte(eth2[:4]) != d {
			continue
		}
		for n.Peers() >= n.cfg.TargetPeers {
			select {
			case <-n.ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
		n.dial(nd)
	}
}

func sum(m map[string]int) (s int) {
	for _, c := range m {
		s += c
	}
	return
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
	if n.dialing[id] {
		n.mu.Unlock()
		return
	}
	n.dialing[id] = true
	n.mu.Unlock()
	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", nd.IP(), nd.TCP()))
	if err != nil {
		return
	}
	go func() {
		defer func() { n.mu.Lock(); delete(n.dialing, id); n.mu.Unlock() }()
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		defer cancel()
		if err := n.host.Connect(ctx, peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{addr}}); err != nil {
			log.Trace("Dial failed", "peer", id, "err", err)
			return
		}
		if err := n.exchangeStatus(id); err != nil {
			log.Debug("Status exchange failed", "peer", id, "err", err)
			n.host.Network().ClosePeer(id)
			return
		}
		log.Debug("Connected to consensus peer", "peer", id, "peers", n.Peers())
	}()
}

// ourStatus is the status we send: the fields of the last status a peer sent us (so
// finality and head match the network), or genesis values before the first one.
func (n *Node) ourStatus(v2 bool) []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := make([]byte, 84, 92)
	if n.status != nil {
		copy(st, n.status[:84])
	}
	copy(st[0:4], n.digest[:])
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
	_, resp, err := readResponse(bufio.NewReader(s), 0)
	if err != nil {
		return err
	}
	if len(resp) < 84 {
		return fmt.Errorf("short status (%d bytes)", len(resp))
	}
	d := n.Digest()
	if [4]byte(resp[:4]) != d {
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
	if _, err := readSSZ(bufio.NewReader(s)); err != nil {
		s.Reset()
		return
	}
	writeResponse(s, nil, n.ourStatus(s.Protocol() == protoStatus2))
}

func (n *Node) handlePing(s network.Stream) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	if _, err := readSSZ(bufio.NewReader(s)); err != nil {
		s.Reset()
		return
	}
	var seq [8]byte
	binary.LittleEndian.PutUint64(seq[:], 1)
	writeResponse(s, nil, seq[:])
}

func (n *Node) handleMetadata(s network.Stream) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	// seq_number, attnets (8 bytes), syncnets (1), and from v3 custody_group_count (0)
	md := make([]byte, 17, 25)
	binary.LittleEndian.PutUint64(md[0:8], 1)
	if s.Protocol() == protoMetadata3 {
		md = md[:25]
	}
	writeResponse(s, nil, md)
}

func (n *Node) handleGoodbye(s network.Stream) {
	defer s.Close()
	if b, err := readSSZ(bufio.NewReader(s)); err == nil && len(b) == 8 {
		log.Debug("Peer said goodbye", "peer", s.Conn().RemotePeer(), "reason", binary.LittleEndian.Uint64(b))
	}
}

// joinGossip subscribes to the light client update topics of the current fork digest.
func (n *Node) joinGossip() error {
	ps, err := pubsub.NewGossipSub(n.ctx, n.host,
		pubsub.WithMessageIdFn(func(m *pb.Message) string { return gossipMessageID(m.GetTopic(), m.Data) }),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(256),
		pubsub.WithValidateQueueSize(256),
	)
	if err != nil {
		return err
	}
	n.ps = ps
	d := n.Digest()
	for name, handle := range map[string]func(peer.ID, []byte) (forward func() bool, err error){
		"light_client_optimistic_update": n.onOptimistic,
		"light_client_finality_update":   n.onFinality,
	} {
		topic := fmt.Sprintf("/eth2/%x/%s/ssz_snappy", d[:], name)
		// Updates are handed to blsync (which verifies them itself) and forwarded only if
		// they pass the gossip checks here; anything else is ignored, not rejected, so a
		// lagging committee chain of ours never penalizes honest peers.
		if err := ps.RegisterTopicValidator(topic, func(_ context.Context, from peer.ID, m *pubsub.Message) pubsub.ValidationResult {
			ssz, err := decodeGossip(m.Data)
			if err != nil {
				return pubsub.ValidationReject
			}
			n.received.Add(int64(len(m.Data)))
			forward, err := handle(m.ReceivedFrom, ssz)
			if err != nil {
				log.Debug("Bad light client update", "topic", name, "peer", m.ReceivedFrom, "err", err)
				return pubsub.ValidationReject
			}
			if forward() {
				n.forwarded.Add(1)
				return pubsub.ValidationAccept
			}
			return pubsub.ValidationIgnore
		}); err != nil {
			return err
		}
		t, err := ps.Join(topic)
		if err != nil {
			return err
		}
		sub, err := t.Subscribe()
		if err != nil {
			return err
		}
		n.wg.Add(1)
		go func() { // drain: messages are handled in the validator
			defer n.wg.Done()
			for {
				if _, err := sub.Next(n.ctx); err != nil {
					return
				}
			}
		}()
		log.Info("Subscribed to light client gossip", "topic", topic)
	}
	return nil
}

func (n *Node) onOptimistic(from peer.ID, ssz []byte) (func() bool, error) {
	u, err := DecodeOptimisticUpdate(n.cfg.ForkName, ssz)
	if err != nil {
		return nil, err
	}
	if cb := n.cfg.OnOptimisticUpdate; cb != nil {
		cb(from, u)
	}
	return func() bool {
		return n.forwardable(u.SignedHeader(), u.Attested.Slot, &n.fwdOptimistic, &n.lastOptimistic, ssz)
	}, nil
}

func (n *Node) onFinality(from peer.ID, ssz []byte) (func() bool, error) {
	u, err := DecodeFinalityUpdate(n.cfg.ForkName, ssz)
	if err != nil {
		return nil, err
	}
	if cb := n.cfg.OnFinalityUpdate; cb != nil {
		cb(from, u)
	}
	return func() bool {
		return n.forwardable(u.SignedHeader(), u.Finalized.Slot, &n.fwdFin, &n.lastFinality, ssz)
	}, nil
}

// forwardable applies the gossip rules for light client updates: received no earlier than
// a third of the signature slot (allowing for clock disparity), newer than the last one
// forwarded on the topic (key: attested slot, or finalized slot), and signed by the sync
// committee. It records the update as forwarded if so.
func (n *Node) forwardable(head types.SignedHeader, key uint64, last *uint64, keep *[]byte, ssz []byte) bool {
	if n.cfg.VerifyHeader == nil {
		return false
	}
	due := time.Unix(int64(n.cfg.GenesisTime+head.SignatureSlot*12), 0).Add(4*time.Second - 500*time.Millisecond)
	if time.Now().Before(due) {
		return false
	}
	n.fwdMu.Lock()
	defer n.fwdMu.Unlock()
	if key <= *last {
		return false
	}
	if ok, err := n.cfg.VerifyHeader(head); err != nil || !ok {
		return false
	}
	*last = key
	*keep = ssz
	return true
}

// ENR key advertising which light client data this node serves over req/resp: a bitfield,
// bit 0 the latest optimistic and finality updates (bit 1 updates by range, bit 2
// bootstraps: not served yet). Not a consensus-spec key: other clients ignore it.
const enrLightClientKey = "lc"

// serveLatest answers a light client optimistic or finality update request with the last
// verified update of that kind, or "resource unavailable" (3) without one.
func (n *Node) serveLatest(s network.Stream, keep *[]byte) {
	defer s.Close()
	s.SetDeadline(time.Now().Add(respTimeout))
	n.fwdMu.Lock()
	ssz := *keep
	n.fwdMu.Unlock()
	d := n.Digest()
	if ssz == nil {
		s.Write([]byte{3})
		writeSSZ(s, []byte("no update yet"))
		return
	}
	n.served.Add(1)
	writeResponse(s, d[:], ssz)
}

// Forwarded returns the number of light client updates forwarded to other peers.
func (n *Node) Forwarded() int64 { return n.forwarded.Load() }

// GossipBytes returns the compressed bytes of light client gossip received.
func (n *Node) GossipBytes() int64 { return n.received.Load() }
