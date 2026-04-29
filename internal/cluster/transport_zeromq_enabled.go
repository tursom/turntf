//go:build zeromq

package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pebbe/zmq4"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

var errZeroMQNotBuilt error
var zeroMQContextConfigOnce sync.Once
var zeroMQContextConfigErr error

const (
	zeroMQSocketBufferBytes = 1 << 20
	zeroMQSocketBacklog     = 1024
)

var zeroMQCurveAuthState struct {
	sync.Mutex
	refs int
	next uint64
}

type zeroMQDialer struct {
	cfg              ZeroMQConfig
	serverKeyForPeer func(string) string
}

type zeroMQClusterListener struct {
	mux *ZeroMQMuxListener
}

// zeroMQWakePair turns Go-side queue activity into a pollable inproc signal
// so ROUTER/DEALER loops can block indefinitely without reintroducing 100ms polling.
type zeroMQWakePair struct {
	recv *zmq4.Socket
	send *zmq4.Socket

	mu      sync.Mutex
	pending bool
	closed  bool
}

type ZeroMQMuxListener struct {
	bindURL string
	cfg     ZeroMQConfig

	curveAuthDomain  string
	curveAuthStarted bool

	clusterAccept func(TransportConn)
	clientAccept  func(TransportConn)

	sendCh       chan zeroMQRouterOutbound
	connClosedCh chan string
	done         chan struct{}
	wake         *zeroMQWakePair

	closeOnce sync.Once
	waitGroup sync.WaitGroup
}

type zeroMQRouterOutbound struct {
	identityKey string
	identity    []byte
	payload     []byte
}

type zeroMQRouterPeer struct {
	identity []byte
	conn     *zeroMQTransportConn
	role     internalproto.ZeroMQMuxHello_Role
}

type zeroMQTransportConn struct {
	direction  string
	localAddr  string
	remoteAddr string

	sendCh chan []byte
	recvCh chan []byte
	sendFn func(ctx context.Context, payload []byte) error

	done    chan struct{}
	onClose func()
	wake    *zeroMQWakePair

	closeOnce sync.Once
	errMu     sync.Mutex
	closeErr  error
}

func zeroMQEnabled() bool {
	return true
}

func newZeroMQDialer() Dialer {
	return newZeroMQDialerWithConfig(ZeroMQConfig{}, nil)
}

func newZeroMQDialerWithConfig(cfg ZeroMQConfig, serverKeyForPeer func(string) string) Dialer {
	return &zeroMQDialer{cfg: cfg, serverKeyForPeer: serverKeyForPeer}
}

func newZeroMQListener(bindURL string) Listener {
	return &zeroMQClusterListener{mux: NewZeroMQMuxListener(bindURL)}
}

func NewZeroMQMuxListener(bindURL string) *ZeroMQMuxListener {
	return NewZeroMQMuxListenerWithConfig(bindURL, ZeroMQConfig{})
}

func NewZeroMQMuxListenerWithConfig(bindURL string, cfg ZeroMQConfig) *ZeroMQMuxListener {
	return &ZeroMQMuxListener{
		bindURL:      bindURL,
		cfg:          cfg,
		sendCh:       make(chan zeroMQRouterOutbound, outboundQueueSize),
		connClosedCh: make(chan string, outboundQueueSize),
		done:         make(chan struct{}),
	}
}

func (l *ZeroMQMuxListener) SetClusterAccept(accept func(TransportConn)) {
	if l == nil {
		return
	}
	l.clusterAccept = accept
}

func (l *ZeroMQMuxListener) SetClientAccept(accept func(TransportConn)) {
	if l == nil {
		return
	}
	l.clientAccept = accept
}

func (d *zeroMQDialer) Dial(ctx context.Context, peerURL string) (TransportConn, error) {
	if err := ensureZeroMQRuntimeConfigured(); err != nil {
		return nil, err
	}
	socket, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		return nil, fmt.Errorf("create zeromq dealer socket: %w", err)
	}
	if err := configureZeroMQSocket(socket); err != nil {
		_ = socket.Close()
		return nil, err
	}
	identity, err := newZeroMQIdentity()
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	if err := socket.SetIdentity(identity); err != nil {
		_ = socket.Close()
		return nil, fmt.Errorf("set zeromq dealer identity: %w", err)
	}
	if err := d.configureClientSecurity(socket, peerURL); err != nil {
		_ = socket.Close()
		return nil, err
	}
	address, err := zeroMQDialAddress(peerURL)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	if err := socket.Connect(address); err != nil {
		_ = socket.Close()
		return nil, fmt.Errorf("connect zeromq dealer %s: %w", peerURL, err)
	}
	wake, err := newZeroMQWakePair()
	if err != nil {
		_ = socket.Close()
		return nil, err
	}

	conn := &zeroMQTransportConn{
		direction:  "outbound",
		localAddr:  "",
		remoteAddr: peerURL,
		sendCh:     make(chan []byte, outboundQueueSize),
		recvCh:     make(chan []byte, outboundQueueSize),
		done:       make(chan struct{}),
		wake:       wake,
	}
	go runZeroMQDealer(socket, wake, conn)
	if err := writeZeroMQMuxHello(ctx, conn, internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLUSTER); err != nil {
		conn.finish(err)
		return nil, err
	}
	return conn, nil
}

func (l *zeroMQClusterListener) Start(ctx context.Context, accept func(TransportConn)) error {
	l.mux.SetClusterAccept(accept)
	return l.mux.Start(ctx)
}

func (l *zeroMQClusterListener) Close() error {
	return l.mux.Close()
}

func (l *ZeroMQMuxListener) Start(ctx context.Context) error {
	if err := ensureZeroMQRuntimeConfigured(); err != nil {
		return err
	}
	socket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		return fmt.Errorf("create zeromq router socket: %w", err)
	}
	if err := configureZeroMQSocket(socket); err != nil {
		_ = socket.Close()
		return err
	}
	if err := l.configureServerSecurity(socket); err != nil {
		_ = socket.Close()
		return err
	}
	if err := socket.Bind(l.bindURL); err != nil {
		_ = socket.Close()
		l.releaseServerSecurity()
		return fmt.Errorf("bind zeromq router %s: %w", l.bindURL, err)
	}
	wake, err := newZeroMQWakePair()
	if err != nil {
		_ = socket.Close()
		l.releaseServerSecurity()
		return err
	}
	l.wake = wake

	l.waitGroup.Add(1)
	go func() {
		defer l.waitGroup.Done()
		defer func() {
			_ = socket.Close()
			wake.Close()
			l.releaseServerSecurity()
		}()
		runZeroMQRouter(ctx, socket, wake, l)
	}()
	return nil
}

func (l *ZeroMQMuxListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
		l.signalWake()
	})
	l.waitGroup.Wait()
	return nil
}

func (d *zeroMQDialer) configureClientSecurity(socket *zmq4.Socket, peerURL string) error {
	if zeroMQConfigSecurity(d.cfg) != ZeroMQSecurityCurve {
		return nil
	}
	if !zmq4.HasCurve() {
		return fmt.Errorf("zeromq curve security is not available in the linked libzmq")
	}
	serverKey := ""
	if d.serverKeyForPeer != nil {
		serverKey = strings.TrimSpace(d.serverKeyForPeer(peerURL))
	}
	if serverKey == "" {
		return fmt.Errorf("zeromq curve server public key is required for %s", peerURL)
	}
	if err := socket.SetCurveServerkey(serverKey); err != nil {
		return fmt.Errorf("set zeromq curve server public key: %w", err)
	}
	if err := socket.SetCurvePublickey(strings.TrimSpace(d.cfg.Curve.ClientPublicKey)); err != nil {
		return fmt.Errorf("set zeromq curve client public key: %w", err)
	}
	if err := socket.SetCurveSecretkey(strings.TrimSpace(d.cfg.Curve.ClientSecretKey)); err != nil {
		return fmt.Errorf("set zeromq curve client secret key: %w", err)
	}
	return nil
}

func (l *ZeroMQMuxListener) configureServerSecurity(socket *zmq4.Socket) error {
	if zeroMQConfigSecurity(l.cfg) != ZeroMQSecurityCurve {
		return nil
	}
	if !zmq4.HasCurve() {
		return fmt.Errorf("zeromq curve security is not available in the linked libzmq")
	}
	domain, err := acquireZeroMQCurveAuth(l.cfg.Curve.AllowedClientPublicKeys)
	if err != nil {
		return err
	}
	l.curveAuthDomain = domain
	l.curveAuthStarted = true
	if err := socket.SetCurveServer(1); err != nil {
		l.releaseServerSecurity()
		return fmt.Errorf("set zeromq curve server mode: %w", err)
	}
	if err := socket.SetCurveSecretkey(strings.TrimSpace(l.cfg.Curve.ServerSecretKey)); err != nil {
		l.releaseServerSecurity()
		return fmt.Errorf("set zeromq curve server secret key: %w", err)
	}
	if err := socket.SetZapDomain(domain); err != nil {
		l.releaseServerSecurity()
		return fmt.Errorf("set zeromq zap domain: %w", err)
	}
	return nil
}

func (l *ZeroMQMuxListener) releaseServerSecurity() {
	if !l.curveAuthStarted {
		return
	}
	releaseZeroMQCurveAuth(l.curveAuthDomain)
	l.curveAuthStarted = false
	l.curveAuthDomain = ""
}

func acquireZeroMQCurveAuth(allowedClientPublicKeys []string) (string, error) {
	keys := make([]string, 0, len(allowedClientPublicKeys))
	for _, raw := range allowedClientPublicKeys {
		key := strings.TrimSpace(raw)
		if key != "" {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return "", fmt.Errorf("zeromq curve allowed client public keys cannot be empty")
	}

	zeroMQCurveAuthState.Lock()
	defer zeroMQCurveAuthState.Unlock()
	if zeroMQCurveAuthState.refs == 0 {
		if err := zmq4.AuthStart(); err != nil {
			return "", fmt.Errorf("start zeromq curve authenticator: %w", err)
		}
	}
	zeroMQCurveAuthState.next++
	domain := fmt.Sprintf("turntf-zeromq-curve-%d", zeroMQCurveAuthState.next)
	zmq4.AuthCurveAdd(domain, keys...)
	zeroMQCurveAuthState.refs++
	return domain, nil
}

func releaseZeroMQCurveAuth(domain string) {
	zeroMQCurveAuthState.Lock()
	defer zeroMQCurveAuthState.Unlock()
	if strings.TrimSpace(domain) != "" {
		zmq4.AuthCurveRemoveAll(domain)
	}
	if zeroMQCurveAuthState.refs > 0 {
		zeroMQCurveAuthState.refs--
	}
	if zeroMQCurveAuthState.refs == 0 {
		zmq4.AuthStop()
		// pebbe/zmq4 may release the inproc ZAP endpoint just after AuthStop returns.
		time.Sleep(100 * time.Millisecond)
	}
}

func zeroMQConfigSecurity(cfg ZeroMQConfig) string {
	security := strings.ToLower(strings.TrimSpace(cfg.Security))
	if security == "" {
		return ZeroMQSecurityNone
	}
	return security
}

func configureZeroMQSocket(socket *zmq4.Socket) error {
	if err := socket.SetLinger(0); err != nil {
		return fmt.Errorf("set zeromq linger: %w", err)
	}
	if err := socket.SetImmediate(true); err != nil {
		return fmt.Errorf("set zeromq immediate: %w", err)
	}
	if err := socket.SetBacklog(zeroMQSocketBacklog); err != nil {
		return fmt.Errorf("set zeromq backlog: %w", err)
	}
	if err := socket.SetSndbuf(zeroMQSocketBufferBytes); err != nil {
		return fmt.Errorf("set zeromq sndbuf: %w", err)
	}
	if err := socket.SetRcvbuf(zeroMQSocketBufferBytes); err != nil {
		return fmt.Errorf("set zeromq rcvbuf: %w", err)
	}
	if err := socket.SetSndhwm(outboundQueueSize); err != nil {
		return fmt.Errorf("set zeromq sndhwm: %w", err)
	}
	if err := socket.SetRcvhwm(outboundQueueSize); err != nil {
		return fmt.Errorf("set zeromq rcvhwm: %w", err)
	}
	if err := socket.SetTcpKeepalive(1); err != nil {
		return fmt.Errorf("set zeromq tcp keepalive: %w", err)
	}
	if err := socket.SetMaxmsgsize(websocketReadLimit); err != nil {
		return fmt.Errorf("set zeromq max message size: %w", err)
	}
	return nil
}

func runZeroMQDealer(socket *zmq4.Socket, wake *zeroMQWakePair, conn *zeroMQTransportConn) {
	defer conn.finish(errSessionClosed)
	if wake != nil {
		defer wake.Close()
	}
	poller := zmq4.NewPoller()
	socketPollID := poller.Add(socket, zmq4.POLLIN)
	if wake != nil && wake.recv != nil {
		poller.Add(wake.recv, zmq4.POLLIN)
	}

	pending := make([][]byte, 0, outboundQueueSize)
	pollingWritable := false
	for {
		zeroMQDrainBytesQueue(conn.sendCh, &pending)
		if len(pending) > 0 {
			if !zeroMQFlushDealerMessages(socket, conn, &pending) {
				return
			}
		}
		if len(pending) == 0 && wake != nil {
			wake.Reset()
			zeroMQDrainBytesQueue(conn.sendCh, &pending)
			if len(pending) > 0 {
				if !zeroMQFlushDealerMessages(socket, conn, &pending) {
					return
				}
			}
		}
		wantWritable := len(pending) > 0
		if wantWritable != pollingWritable {
			events := zmq4.POLLIN
			if wantWritable {
				events |= zmq4.POLLOUT
			}
			if _, err := poller.Update(socketPollID, events); err != nil {
				conn.finish(fmt.Errorf("update zeromq dealer poll events: %w", err))
				return
			}
			pollingWritable = wantWritable
		}

		select {
		case <-conn.done:
			return
		default:
		}

		polled, err := poller.Poll(-1)
		if err != nil {
			conn.finish(fmt.Errorf("poll zeromq dealer: %w", err))
			return
		}
		for _, item := range polled {
			if wake != nil && item.Socket == wake.recv {
				wake.Drain()
				zeroMQDrainBytesQueue(conn.sendCh, &pending)
				if len(pending) > 0 {
					if !zeroMQFlushDealerMessages(socket, conn, &pending) {
						return
					}
				}
				continue
			}
			if item.Events&zmq4.POLLIN != 0 {
				if !zeroMQReceiveDealerMessages(socket, conn) {
					return
				}
			}
			if item.Events&zmq4.POLLOUT != 0 && len(pending) > 0 {
				if !zeroMQFlushDealerMessages(socket, conn, &pending) {
					return
				}
			}
		}
	}
}

func runZeroMQRouter(ctx context.Context, socket *zmq4.Socket, wake *zeroMQWakePair, listener *ZeroMQMuxListener) {
	peers := make(map[string]*zeroMQRouterPeer)
	pending := make([]zeroMQRouterOutbound, 0, outboundQueueSize)
	poller := zmq4.NewPoller()
	socketPollID := poller.Add(socket, zmq4.POLLIN)
	if wake != nil && wake.recv != nil {
		poller.Add(wake.recv, zmq4.POLLIN)
	}
	pollingWritable := false
	closePeer := func(identityKey string) {
		peer, ok := peers[identityKey]
		if !ok {
			return
		}
		delete(peers, identityKey)
		peer.conn.finish(errSessionClosed)
	}
	defer func() {
		for identityKey := range peers {
			closePeer(identityKey)
		}
	}()

	for {
		for {
			select {
			case <-ctx.Done():
				return
			case <-listener.done:
				return
			case identityKey := <-listener.connClosedCh:
				closePeer(identityKey)
			case outbound := <-listener.sendCh:
				pending = append(pending, outbound)
			default:
				goto POLL
			}
		}

	POLL:
		if len(pending) > 0 {
			if !zeroMQFlushRouterMessages(socket, peers, closePeer, &pending) {
				return
			}
		}
		if len(pending) == 0 && wake != nil {
			wake.Reset()
			zeroMQDrainRouterControlQueues(listener, &pending, closePeer)
			if len(pending) > 0 {
				if !zeroMQFlushRouterMessages(socket, peers, closePeer, &pending) {
					return
				}
			}
		}
		wantWritable := len(pending) > 0
		if wantWritable != pollingWritable {
			events := zmq4.POLLIN
			if wantWritable {
				events |= zmq4.POLLOUT
			}
			if _, err := poller.Update(socketPollID, events); err != nil {
				return
			}
			pollingWritable = wantWritable
		}
		polled, err := poller.Poll(-1)
		if err != nil {
			return
		}

		for _, item := range polled {
			if wake != nil && item.Socket == wake.recv {
				wake.Drain()
				zeroMQDrainRouterControlQueues(listener, &pending, closePeer)
				if len(pending) > 0 {
					if !zeroMQFlushRouterMessages(socket, peers, closePeer, &pending) {
						return
					}
				}
				continue
			}
			if item.Events&zmq4.POLLIN != 0 {
				if !zeroMQReceiveRouterMessages(socket, listener, peers, closePeer) {
					return
				}
			}
			if item.Events&zmq4.POLLOUT != 0 && len(pending) > 0 {
				if !zeroMQFlushRouterMessages(socket, peers, closePeer, &pending) {
					return
				}
			}
		}
	}
}

func (l *ZeroMQMuxListener) acceptHandler(role internalproto.ZeroMQMuxHello_Role) func(TransportConn) {
	switch role {
	case internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLUSTER:
		return l.clusterAccept
	case internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLIENT:
		return l.clientAccept
	default:
		return nil
	}
}

func (l *ZeroMQMuxListener) newInboundConn(identityKey string, identity []byte, accept func(TransportConn)) *zeroMQTransportConn {
	conn := &zeroMQTransportConn{
		direction:  "inbound",
		localAddr:  l.bindURL,
		remoteAddr: "identity:" + shortZeroMQIdentity(identity),
		recvCh:     make(chan []byte, outboundQueueSize),
		done:       make(chan struct{}),
	}
	conn.sendFn = func(ctx context.Context, payload []byte) error {
		outbound := zeroMQRouterOutbound{
			identityKey: identityKey,
			identity:    identity,
			payload:     payload,
		}
		select {
		case <-ctxDone(ctx):
			return ctx.Err()
		case <-conn.done:
			return conn.closedErr()
		case <-l.done:
			return conn.closedErr()
		case l.sendCh <- outbound:
			if len(l.sendCh) == 1 {
				l.signalWake()
			}
			return nil
		}
	}
	conn.onClose = func() {
		select {
		case l.connClosedCh <- identityKey:
			if len(l.connClosedCh) == 1 {
				l.signalWake()
			}
		case <-l.done:
		default:
		}
	}
	accept(conn)
	return conn
}

func (c *zeroMQTransportConn) Send(ctx context.Context, payload []byte) error {
	return c.sendQueuedPayload(ctx, cloneBytes(payload))
}

func (c *zeroMQTransportConn) SendOwned(ctx context.Context, payload []byte) error {
	return c.sendQueuedPayload(ctx, payload)
}

func (c *zeroMQTransportConn) sendQueuedPayload(ctx context.Context, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-c.done:
		return c.closedErr()
	default:
	}
	if c.sendFn != nil {
		return c.sendFn(ctx, payload)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.closedErr()
	case c.sendCh <- payload:
		if len(c.sendCh) == 1 {
			c.signalWake()
		}
		return nil
	}
}

func (c *zeroMQTransportConn) Receive(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.closedErr()
	case payload, ok := <-c.recvCh:
		if !ok {
			return nil, c.closedErr()
		}
		return payload, nil
	}
}

func (c *zeroMQTransportConn) Close() error {
	c.finish(errSessionClosed)
	return nil
}

func (c *zeroMQTransportConn) LocalAddr() string {
	return c.localAddr
}

func (c *zeroMQTransportConn) RemoteAddr() string {
	return c.remoteAddr
}

func (c *zeroMQTransportConn) Direction() string {
	return c.direction
}

func (c *zeroMQTransportConn) Transport() string {
	return transportZeroMQ
}

func (c *zeroMQTransportConn) deliver(payload []byte) bool {
	select {
	case <-c.done:
		return false
	case c.recvCh <- payload:
		return true
	}
}

func (c *zeroMQTransportConn) finish(err error) {
	c.closeOnce.Do(func() {
		c.errMu.Lock()
		if err != nil {
			c.closeErr = err
		} else {
			c.closeErr = errSessionClosed
		}
		c.errMu.Unlock()
		close(c.done)
		c.signalWake()
		if c.onClose != nil {
			c.onClose()
		}
	})
}

func (c *zeroMQTransportConn) closedErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return errSessionClosed
}

func zeroMQDialAddress(peerURL string) (string, error) {
	parsed, err := url.Parse(peerURL)
	if err != nil {
		return "", fmt.Errorf("parse zeromq peer url: %w", err)
	}
	parsed.Scheme = zeroMQBindSchemeTCP
	return parsed.String(), nil
}

func writeZeroMQMuxHello(ctx context.Context, conn TransportConn, role internalproto.ZeroMQMuxHello_Role) error {
	data, err := gproto.Marshal(&internalproto.ZeroMQMuxHello{
		Role:            role,
		ProtocolVersion: internalproto.ZeroMQMuxProtocolVersion,
	})
	if err != nil {
		return fmt.Errorf("marshal zeromq mux hello: %w", err)
	}
	if err := conn.Send(ctx, data); err != nil {
		return fmt.Errorf("send zeromq mux hello: %w", err)
	}
	return nil
}

func parseZeroMQMuxHello(payload []byte) (internalproto.ZeroMQMuxHello_Role, error) {
	var hello internalproto.ZeroMQMuxHello
	if err := gproto.Unmarshal(payload, &hello); err != nil {
		return internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_UNSPECIFIED, err
	}
	if hello.ProtocolVersion != internalproto.ZeroMQMuxProtocolVersion {
		return internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_UNSPECIFIED, fmt.Errorf("unsupported zeromq mux protocol version %q", hello.ProtocolVersion)
	}
	switch hello.Role {
	case internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLUSTER, internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLIENT:
		return hello.Role, nil
	default:
		return internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_UNSPECIFIED, fmt.Errorf("unsupported zeromq mux role %q", hello.Role.String())
	}
}

func newZeroMQIdentity() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate zeromq identity: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func shortZeroMQIdentity(identity []byte) string {
	encoded := hex.EncodeToString(identity)
	if len(encoded) <= 16 {
		return encoded
	}
	return encoded[:16]
}

func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}

func newZeroMQWakePair() (*zeroMQWakePair, error) {
	if err := ensureZeroMQRuntimeConfigured(); err != nil {
		return nil, err
	}
	endpointID, err := newZeroMQIdentity()
	if err != nil {
		return nil, err
	}
	recv, err := zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		return nil, fmt.Errorf("create zeromq wake receiver socket: %w", err)
	}
	if err := configureZeroMQWakeSocket(recv); err != nil {
		_ = recv.Close()
		return nil, err
	}
	send, err := zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		_ = recv.Close()
		return nil, fmt.Errorf("create zeromq wake sender socket: %w", err)
	}
	if err := configureZeroMQWakeSocket(send); err != nil {
		_ = send.Close()
		_ = recv.Close()
		return nil, err
	}
	endpoint := "inproc://turntf-zeromq-wake-" + endpointID
	if err := recv.Bind(endpoint); err != nil {
		_ = send.Close()
		_ = recv.Close()
		return nil, fmt.Errorf("bind zeromq wake receiver socket: %w", err)
	}
	if err := send.Connect(endpoint); err != nil {
		_ = send.Close()
		_ = recv.Close()
		return nil, fmt.Errorf("connect zeromq wake sender socket: %w", err)
	}
	wake := &zeroMQWakePair{
		recv: recv,
		send: send,
	}
	return wake, nil
}

func configureZeroMQWakeSocket(socket *zmq4.Socket) error {
	if err := socket.SetLinger(0); err != nil {
		return fmt.Errorf("set zeromq wake linger: %w", err)
	}
	if err := socket.SetSndhwm(outboundQueueSize); err != nil {
		return fmt.Errorf("set zeromq wake sndhwm: %w", err)
	}
	if err := socket.SetRcvhwm(outboundQueueSize); err != nil {
		return fmt.Errorf("set zeromq wake rcvhwm: %w", err)
	}
	return nil
}

func (w *zeroMQWakePair) Signal() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.pending || w.send == nil {
		return
	}
	if _, err := w.send.SendBytes([]byte{1}, zmq4.DONTWAIT); err == nil || zeroMQWouldBlock(err) {
		w.pending = true
	}
}

func (w *zeroMQWakePair) Drain() {
	if w == nil || w.recv == nil {
		return
	}
	for {
		if _, err := w.recv.RecvBytes(zmq4.DONTWAIT); err != nil {
			if zeroMQWouldBlock(err) {
				return
			}
			return
		}
	}
}

func (w *zeroMQWakePair) Reset() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.pending = false
	w.mu.Unlock()
}

func (w *zeroMQWakePair) Close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	if w.send != nil {
		_ = w.send.Close()
	}
	if w.recv != nil {
		_ = w.recv.Close()
	}
}

func (l *ZeroMQMuxListener) signalWake() {
	if l == nil || l.wake == nil {
		return
	}
	l.wake.Signal()
}

func (c *zeroMQTransportConn) signalWake() {
	if c == nil || c.wake == nil {
		return
	}
	c.wake.Signal()
}

func zeroMQDrainBytesQueue(ch <-chan []byte, pending *[][]byte) {
	for {
		select {
		case payload := <-ch:
			*pending = append(*pending, payload)
		default:
			return
		}
	}
}

func zeroMQDrainRouterControlQueues(listener *ZeroMQMuxListener, pending *[]zeroMQRouterOutbound, closePeer func(string)) {
	if listener == nil {
		return
	}
	for {
		select {
		case identityKey := <-listener.connClosedCh:
			closePeer(identityKey)
		case outbound := <-listener.sendCh:
			*pending = append(*pending, outbound)
		default:
			return
		}
	}
}

func zeroMQFlushDealerMessages(socket *zmq4.Socket, conn *zeroMQTransportConn, pending *[][]byte) bool {
	for len(*pending) > 0 {
		payload := (*pending)[0]
		if _, err := socket.SendBytes(payload, zmq4.DONTWAIT); err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			conn.finish(fmt.Errorf("send zeromq dealer payload: %w", err))
			return false
		}
		*pending = (*pending)[1:]
		zeroMQDrainBytesQueue(conn.sendCh, pending)
	}
	return true
}

func zeroMQReceiveDealerMessages(socket *zmq4.Socket, conn *zeroMQTransportConn) bool {
	for {
		payload, err := zeroMQReceiveLastFrame(socket)
		if err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			conn.finish(fmt.Errorf("receive zeromq dealer payload: %w", err))
			return false
		}
		if len(payload) == 0 {
			continue
		}
		if !conn.deliver(payload) {
			return false
		}
	}
}

func zeroMQReceiveRouterMessages(socket *zmq4.Socket, listener *ZeroMQMuxListener, peers map[string]*zeroMQRouterPeer, closePeer func(string)) bool {
	for {
		identity, payload, err := zeroMQReceiveRouterMessage(socket)
		if err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			return false
		}
		if len(identity) == 0 || len(payload) == 0 {
			continue
		}
		identityKey := string(identity)
		peer, ok := peers[identityKey]
		if !ok {
			role, err := parseZeroMQMuxHello(payload)
			if err != nil {
				continue
			}
			accept := listener.acceptHandler(role)
			if accept == nil {
				continue
			}
			peer = &zeroMQRouterPeer{
				identity: identity,
				role:     role,
				conn: listener.newInboundConn(identityKey, identity, func(c TransportConn) {
					go accept(c)
				}),
			}
			peers[identityKey] = peer
			continue
		}
		if !peer.conn.deliver(payload) {
			closePeer(identityKey)
		}
	}
}

func zeroMQFlushRouterMessages(socket *zmq4.Socket, peers map[string]*zeroMQRouterPeer, closePeer func(string), pending *[]zeroMQRouterOutbound) bool {
	for len(*pending) > 0 {
		outbound := (*pending)[0]
		if _, ok := peers[outbound.identityKey]; !ok {
			*pending = (*pending)[1:]
			continue
		}
		if _, err := socket.SendBytes(outbound.identity, zmq4.SNDMORE|zmq4.DONTWAIT); err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			closePeer(outbound.identityKey)
			*pending = (*pending)[1:]
			continue
		}
		if _, err := socket.SendBytes(outbound.payload, zmq4.DONTWAIT); err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			closePeer(outbound.identityKey)
			*pending = (*pending)[1:]
			continue
		}
		*pending = (*pending)[1:]
	}
	return true
}

func zeroMQReceiveLastFrame(socket *zmq4.Socket) ([]byte, error) {
	payload, err := socket.RecvBytes(zmq4.DONTWAIT)
	if err != nil {
		return nil, err
	}
	more, err := socket.GetRcvmore()
	if err != nil {
		return nil, err
	}
	for more {
		payload, err = socket.RecvBytes(zmq4.DONTWAIT)
		if err != nil {
			return nil, err
		}
		more, err = socket.GetRcvmore()
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func zeroMQReceiveRouterMessage(socket *zmq4.Socket) ([]byte, []byte, error) {
	identity, err := socket.RecvBytes(zmq4.DONTWAIT)
	if err != nil {
		return nil, nil, err
	}
	more, err := socket.GetRcvmore()
	if err != nil {
		return nil, nil, err
	}
	if !more {
		return nil, nil, nil
	}
	payload, err := socket.RecvBytes(zmq4.DONTWAIT)
	if err != nil {
		return nil, nil, err
	}
	more, err = socket.GetRcvmore()
	if err != nil {
		return nil, nil, err
	}
	for more {
		payload, err = socket.RecvBytes(zmq4.DONTWAIT)
		if err != nil {
			return nil, nil, err
		}
		more, err = socket.GetRcvmore()
		if err != nil {
			return nil, nil, err
		}
	}
	return identity, payload, nil
}

func zeroMQWouldBlock(err error) bool {
	return zmq4.AsErrno(err) == zmq4.Errno(syscall.EAGAIN)
}

func ensureZeroMQRuntimeConfigured() error {
	zeroMQContextConfigOnce.Do(func() {
		threads := runtime.GOMAXPROCS(0) / 2
		if threads < 1 {
			threads = 1
		}
		if threads > 4 {
			threads = 4
		}
		zeroMQContextConfigErr = zmq4.SetIoThreads(threads)
	})
	return zeroMQContextConfigErr
}
