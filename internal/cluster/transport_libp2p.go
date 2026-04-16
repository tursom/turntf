package cluster

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	routingdiscovery "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

const (
	libP2PClusterStreamProtocol protocol.ID = "/turntf/cluster/stream/1.0.0"
	libP2PMaxFrameSize                      = websocketReadLimit
	libP2PDiscoveryInterval                 = 30 * time.Second
)

type libP2PIdentityConn interface {
	RemotePeerID() string
}

type libP2PTransport struct {
	cfg           LibP2PConfig
	clusterSecret string
	manager       *Manager
	inboundAccept func(TransportConn)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu              sync.Mutex
	host            host.Host
	dht             *dht.IpfsDHT
	dhtDiscovery    *routingdiscovery.RoutingDiscovery
	pubsub          *pubsub.PubSub
	subscription    *pubsub.Subscription
	mdns            mdns.Service
	topic           string
	dhtBootstrapped bool
}

type libP2PTransportConn struct {
	stream     network.Stream
	direction  string
	localAddr  string
	remoteAddr string
	remotePeer string

	readMu  sync.Mutex
	writeMu sync.Mutex
}

type libP2PMDNSNotifee struct {
	transport *libP2PTransport
}

func newLibP2PTransport(cfg LibP2PConfig, clusterSecret string, manager *Manager) *libP2PTransport {
	return &libP2PTransport{
		cfg:           cfg,
		clusterSecret: clusterSecret,
		manager:       manager,
	}
}

func (t *libP2PTransport) SetAccept(accept func(TransportConn)) {
	if t == nil {
		return
	}
	t.inboundAccept = accept
}

func (t *libP2PTransport) Start(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.ctx, t.cancel = context.WithCancel(ctx)

	key, err := loadOrCreateLibP2PPrivateKey(t.cfg.PrivateKeyPath)
	if err != nil {
		return err
	}
	listenAddrs, err := parseMultiaddrs(t.cfg.ListenAddrs)
	if err != nil {
		return err
	}

	opts := []libp2p.Option{
		libp2p.Identity(key),
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.UserAgent("turntf/libp2p"),
	}
	if len(t.cfg.RelayPeers) == 0 {
		opts = append(opts, libp2p.DisableRelay())
	} else {
		opts = append(opts, libp2p.EnableRelay())
		if t.cfg.EnableHolePunching {
			opts = append(opts, libp2p.EnableHolePunching())
		}
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return fmt.Errorf("create libp2p host: %w", err)
	}
	t.host = h
	h.SetStreamHandler(libP2PClusterStreamProtocol, t.handleStream)

	if err := t.connectConfiguredPeers(t.cfg.RelayPeers); err != nil {
		_ = h.Close()
		return err
	}
	if err := t.connectConfiguredPeers(t.cfg.BootstrapPeers); err != nil {
		_ = h.Close()
		return err
	}
	if t.cfg.EnableDHT {
		bootstrapInfos, err := libP2PAddrInfos(t.cfg.BootstrapPeers)
		if err != nil {
			_ = h.Close()
			return err
		}
		kad, err := dht.New(
			t.ctx,
			h,
			dht.ProtocolPrefix(protocol.ID(t.privateProtocolPrefix())),
			dht.Mode(dht.ModeServer),
			dht.QueryFilter(dht.PrivateQueryFilter),
			dht.RoutingTableFilter(dht.PrivateRoutingTableFilter),
			dht.BootstrapPeers(bootstrapInfos...),
		)
		if err != nil {
			_ = h.Close()
			return fmt.Errorf("create libp2p dht: %w", err)
		}
		t.dht = kad
		if err := kad.Bootstrap(t.ctx); err != nil {
			_ = h.Close()
			return fmt.Errorf("bootstrap libp2p dht: %w", err)
		}
		t.mu.Lock()
		t.dhtBootstrapped = true
		t.mu.Unlock()
		t.dhtDiscovery = routingdiscovery.NewRoutingDiscovery(kad)
		t.wg.Add(1)
		go t.dhtDiscoveryLoop()
	}
	if t.cfg.EnableMDNS {
		service := mdns.NewMdnsService(h, t.privateMDNSServiceName(), &libP2PMDNSNotifee{transport: t})
		if err := service.Start(); err != nil {
			_ = h.Close()
			return fmt.Errorf("start libp2p mdns: %w", err)
		}
		t.mdns = service
	}
	if t.cfg.GossipSubEnabled {
		ps, err := pubsub.NewGossipSub(t.ctx, h)
		if err != nil {
			_ = h.Close()
			return fmt.Errorf("create libp2p gossipsub: %w", err)
		}
		topic := t.gossipTopic()
		sub, err := ps.Subscribe(topic)
		if err != nil {
			_ = h.Close()
			return fmt.Errorf("subscribe libp2p gossipsub topic: %w", err)
		}
		t.pubsub = ps
		t.subscription = sub
		t.topic = topic
		t.wg.Add(1)
		go t.readPubSubLoop()
	}

	return nil
}

func (t *libP2PTransport) Dial(ctx context.Context, peerURL string) (TransportConn, error) {
	if t == nil || t.host == nil {
		return nil, fmt.Errorf("libp2p host is not started")
	}
	info, addr, err := libP2PAddrInfo(peerURL)
	if err != nil {
		return nil, err
	}
	if err := t.host.Connect(ctx, *info); err != nil {
		return nil, fmt.Errorf("connect libp2p peer %s: %w", info.ID, err)
	}
	stream, err := t.host.NewStream(ctx, info.ID, libP2PClusterStreamProtocol)
	if err != nil {
		return nil, fmt.Errorf("open libp2p stream %s: %w", info.ID, err)
	}
	conn := newLibP2PTransportConn(stream, true)
	if conn.remotePeer != info.ID.String() {
		_ = stream.Reset()
		return nil, fmt.Errorf("libp2p peer mismatch: expected %s got %s", info.ID, conn.remotePeer)
	}
	if addr != "" {
		conn.remoteAddr = addr
	}
	return conn, nil
}

func (t *libP2PTransport) Close() error {
	if t == nil {
		return nil
	}
	if t.cancel != nil {
		t.cancel()
	}
	if t.subscription != nil {
		t.subscription.Cancel()
	}
	if t.mdns != nil {
		_ = t.mdns.Close()
	}
	if t.dht != nil {
		_ = t.dht.Close()
	}
	if t.host != nil {
		_ = t.host.Close()
	}
	t.wg.Wait()
	return nil
}

func (t *libP2PTransport) Publish(ctx context.Context, payload []byte) error {
	if t == nil || t.pubsub == nil || strings.TrimSpace(t.topic) == "" {
		return nil
	}
	return t.pubsub.Publish(t.topic, payload)
}

func (t *libP2PTransport) PeerID() string {
	if t == nil || t.host == nil {
		return ""
	}
	return t.host.ID().String()
}

func (t *libP2PTransport) ListenAddrs() []string {
	if t == nil || t.host == nil {
		return nil
	}
	p2pPart, err := ma.NewComponent("p2p", t.host.ID().String())
	if err != nil {
		return nil
	}
	addrs := make([]string, 0, len(t.host.Addrs()))
	for _, addr := range t.host.Addrs() {
		addrs = append(addrs, addr.Encapsulate(p2pPart.Multiaddr()).String())
	}
	return addrs
}

func (t *libP2PTransport) DHTBootstrapped() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dhtBootstrapped
}

func (t *libP2PTransport) Topic() string {
	if t == nil {
		return ""
	}
	return t.topic
}

func (t *libP2PTransport) TopicPeers() int {
	if t == nil || t.pubsub == nil || t.topic == "" {
		return 0
	}
	return len(t.pubsub.ListPeers(t.topic))
}

func (t *libP2PTransport) handleStream(stream network.Stream) {
	if t == nil {
		_ = stream.Reset()
		return
	}
	conn := newLibP2PTransportConn(stream, false)
	if t.inboundAccept != nil {
		t.inboundAccept(conn)
		return
	}
	if t.manager == nil {
		_ = conn.Close()
		return
	}
	t.manager.AcceptLibP2PConn(conn)
}

func (t *libP2PTransport) connectConfiguredPeers(values []string) error {
	for _, raw := range values {
		info, _, err := libP2PAddrInfo(raw)
		if err != nil {
			return err
		}
		t.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
		connectCtx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
		err = t.host.Connect(connectCtx, *info)
		cancel()
		if err != nil {
			return fmt.Errorf("connect libp2p configured peer %s: %w", info.ID, err)
		}
	}
	return nil
}

func (t *libP2PTransport) readPubSubLoop() {
	defer t.wg.Done()
	for {
		msg, err := t.subscription.Next(t.ctx)
		if err != nil {
			return
		}
		if msg == nil || msg.Local || msg.ReceivedFrom == t.host.ID() {
			continue
		}
		if t.manager != nil {
			t.manager.handleLibP2PPubSubMessage(msg.Data, msg.ReceivedFrom.String())
		}
	}
}

func (t *libP2PTransport) dhtDiscoveryLoop() {
	defer t.wg.Done()

	t.runDHTDiscoveryRound()
	ticker := time.NewTicker(libP2PDiscoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.runDHTDiscoveryRound()
		}
	}
}

func (t *libP2PTransport) runDHTDiscoveryRound() {
	if t == nil || t.dhtDiscovery == nil || t.host == nil || t.manager == nil {
		return
	}
	namespace := t.dhtDiscoveryNamespace()

	advertiseCtx, advertiseCancel := context.WithTimeout(t.ctx, 20*time.Second)
	_, _ = t.dhtDiscovery.Advertise(advertiseCtx, namespace)
	advertiseCancel()

	findCtx, findCancel := context.WithTimeout(t.ctx, 20*time.Second)
	peers, err := t.dhtDiscovery.FindPeers(findCtx, namespace)
	if err != nil {
		findCancel()
		return
	}
	for info := range peers {
		if info.ID == "" || info.ID == t.host.ID() || len(info.Addrs) == 0 {
			continue
		}
		addrs, err := peer.AddrInfoToP2pAddrs(&info)
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			t.manager.startLibP2PCandidate(addr.String())
		}
	}
	findCancel()
}

func (t *libP2PTransport) privateProtocolPrefix() string {
	return "/turntf/" + t.clusterHash()
}

func (t *libP2PTransport) dhtDiscoveryNamespace() string {
	return "turntf/" + t.clusterHash() + "/peers/v1"
}

func (t *libP2PTransport) privateMDNSServiceName() string {
	return "_turntf-" + t.clusterHash() + "._udp"
}

func (t *libP2PTransport) gossipTopic() string {
	return "/turntf/" + t.clusterHash() + "/events/v1"
}

func (t *libP2PTransport) clusterHash() string {
	sum := sha256.Sum256([]byte("turntf/libp2p/" + t.clusterSecret))
	return hex.EncodeToString(sum[:])[:16]
}

func (n *libP2PMDNSNotifee) HandlePeerFound(info peer.AddrInfo) {
	if n == nil || n.transport == nil || n.transport.manager == nil || info.ID == "" {
		return
	}
	addrs, err := peer.AddrInfoToP2pAddrs(&info)
	if err != nil || len(addrs) == 0 {
		return
	}
	n.transport.manager.startLibP2PCandidate(addrs[0].String())
}

func newLibP2PTransportConn(stream network.Stream, outbound bool) *libP2PTransportConn {
	direction := "inbound"
	if outbound {
		direction = "outbound"
	}
	conn := stream.Conn()
	return &libP2PTransportConn{
		stream:     stream,
		direction:  direction,
		localAddr:  libP2PConnAddr(conn.LocalMultiaddr(), conn.LocalPeer()),
		remoteAddr: libP2PConnAddr(conn.RemoteMultiaddr(), conn.RemotePeer()),
		remotePeer: conn.RemotePeer().String(),
	}
}

func (c *libP2PTransportConn) Send(ctx context.Context, payload []byte) error {
	if len(payload) > libP2PMaxFrameSize {
		return fmt.Errorf("libp2p payload exceeds %d bytes", libP2PMaxFrameSize)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	go func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		if _, err := c.stream.Write(header[:]); err != nil {
			done <- err
			return
		}
		_, err := c.stream.Write(payload)
		done <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (c *libP2PTransportConn) Receive(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		c.readMu.Lock()
		defer c.readMu.Unlock()
		var header [4]byte
		if _, err := io.ReadFull(c.stream, header[:]); err != nil {
			done <- result{err: err}
			return
		}
		size := binary.BigEndian.Uint32(header[:])
		if size > libP2PMaxFrameSize {
			done <- result{err: fmt.Errorf("libp2p frame exceeds %d bytes", libP2PMaxFrameSize)}
			return
		}
		payload := make([]byte, size)
		_, err := io.ReadFull(c.stream, payload)
		done <- result{data: payload, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-done:
		return result.data, result.err
	}
}

func (c *libP2PTransportConn) Close() error {
	return c.stream.Close()
}

func (c *libP2PTransportConn) LocalAddr() string {
	return c.localAddr
}

func (c *libP2PTransportConn) RemoteAddr() string {
	return c.remoteAddr
}

func (c *libP2PTransportConn) Direction() string {
	return c.direction
}

func (c *libP2PTransportConn) Transport() string {
	return transportLibP2P
}

func (c *libP2PTransportConn) RemotePeerID() string {
	return c.remotePeer
}

func loadOrCreateLibP2PPrivateKey(path string) (crypto.PrivKey, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, errors.New("libp2p private key path cannot be empty")
	}
	data, err := os.ReadFile(trimmed)
	if err == nil {
		key, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal libp2p private key %s: %w", trimmed, err)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read libp2p private key %s: %w", trimmed, err)
	}
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate libp2p private key: %w", err)
	}
	data, err = crypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal libp2p private key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(trimmed), 0o700); err != nil {
		return nil, fmt.Errorf("create libp2p private key directory: %w", err)
	}
	if err := os.WriteFile(trimmed, data, 0o600); err != nil {
		return nil, fmt.Errorf("write libp2p private key %s: %w", trimmed, err)
	}
	return key, nil
}

func parseMultiaddrs(values []string) ([]ma.Multiaddr, error) {
	addrs := make([]ma.Multiaddr, 0, len(values))
	for _, raw := range values {
		addr, err := ma.NewMultiaddr(strings.TrimSpace(raw))
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

func libP2PAddrInfo(raw string) (*peer.AddrInfo, string, error) {
	addr, err := ma.NewMultiaddr(strings.TrimSpace(raw))
	if err != nil {
		return nil, "", fmt.Errorf("parse libp2p peer addr: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return nil, "", errors.New("libp2p peer addr must include /p2p peer id")
	}
	return info, addr.String(), nil
}

func libP2PAddrInfos(values []string) ([]peer.AddrInfo, error) {
	infos := make([]peer.AddrInfo, 0, len(values))
	for _, raw := range values {
		info, _, err := libP2PAddrInfo(raw)
		if err != nil {
			return nil, err
		}
		infos = append(infos, *info)
	}
	return infos, nil
}

func libP2PConnAddr(addr ma.Multiaddr, id peer.ID) string {
	if addr == nil {
		if id == "" {
			return ""
		}
		return "/p2p/" + id.String()
	}
	if id == "" {
		return addr.String()
	}
	p2pPart, err := ma.NewComponent("p2p", id.String())
	if err != nil {
		return addr.String()
	}
	return addr.Encapsulate(p2pPart.Multiaddr()).String()
}

func (m *Manager) publishLibP2PEvent(envelope *internalproto.Envelope) {
	if m == nil || m.libp2p == nil || envelope == nil {
		return
	}
	data, err := m.marshalSignedEnvelope(envelope)
	if err != nil {
		m.logWarn("libp2p_gossipsub_publish_failed", err).
			Msg("failed to sign libp2p gossipsub event")
		return
	}
	if err := m.libp2p.Publish(m.ctx, data); err != nil {
		m.logWarn("libp2p_gossipsub_publish_failed", err).
			Msg("failed to publish libp2p gossipsub event")
	}
}

func (m *Manager) handleLibP2PPubSubMessage(data []byte, remotePeerID string) {
	if m == nil || len(data) == 0 || strings.TrimSpace(remotePeerID) == "" {
		return
	}
	var envelope internalproto.Envelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		m.logWarn("libp2p_gossipsub_message_ignored", err).
			Str("libp2p_peer_id", remotePeerID).
			Msg("ignored invalid libp2p gossipsub protobuf")
		return
	}
	if envelope.GetEventBatch() == nil {
		return
	}
	if err := m.verifyEnvelope(&envelope); err != nil {
		m.logWarn("libp2p_gossipsub_message_ignored", err).
			Str("libp2p_peer_id", remotePeerID).
			Msg("ignored unauthenticated libp2p gossipsub event")
		return
	}

	m.mu.Lock()
	peer := m.peers[envelope.NodeId]
	var sess *session
	if peer != nil && peer.libP2PPeerID == remotePeerID {
		sess = peer.trustedSession
	}
	m.mu.Unlock()
	if sess == nil {
		m.logWarn("libp2p_gossipsub_message_ignored", nil).
			Str("libp2p_peer_id", remotePeerID).
			Int64("envelope_node_id", envelope.NodeId).
			Msg("ignored libp2p gossipsub event without trusted stream")
		return
	}
	if err := m.handleEventBatch(sess, &envelope); err != nil && !errors.Is(err, errClockProtectionRejected) {
		m.logSessionWarn("libp2p_gossipsub_event_failed", sess, err).
			Msg("failed to apply libp2p gossipsub event")
	}
}

func (m *Manager) startLibP2PCandidate(rawURL string) {
	if m == nil || m.cfg.DiscoveryDisabled || !m.canDialPeerURL(rawURL) {
		return
	}
	normalized, err := normalizePeerURL(rawURL)
	if err != nil {
		return
	}
	m.mu.Lock()
	if _, ok := m.dynamicPeers[normalized]; ok {
		m.mu.Unlock()
		return
	}
	peer := &configuredPeer{
		URL:          normalized,
		libP2PPeerID: libP2PPeerIDFromAddr(normalized),
		dynamic:      true,
		source:       peerSourceDiscovered,
	}
	m.dynamicPeers[normalized] = peer
	m.mu.Unlock()

	m.wg.Add(1)
	go m.dialLoop(peer)
}
