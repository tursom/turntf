//go:build zeromq

package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

const zeroMQPollInterval = 100 * time.Millisecond

var errZeroMQNotBuilt error

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

	conn := &zeroMQTransportConn{
		direction:  "outbound",
		localAddr:  "",
		remoteAddr: peerURL,
		sendCh:     make(chan []byte, outboundQueueSize),
		recvCh:     make(chan []byte, outboundQueueSize),
		done:       make(chan struct{}),
	}
	go runZeroMQDealer(socket, conn)
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

	l.waitGroup.Add(1)
	go func() {
		defer l.waitGroup.Done()
		defer func() {
			_ = socket.Close()
			l.releaseServerSecurity()
		}()
		runZeroMQRouter(ctx, socket, l)
	}()
	return nil
}

func (l *ZeroMQMuxListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
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

func runZeroMQDealer(socket *zmq4.Socket, conn *zeroMQTransportConn) {
	defer conn.finish(errSessionClosed)

	var pending []byte
	for {
		if pending == nil {
			select {
			case <-conn.done:
				return
			case payload := <-conn.sendCh:
				pending = payload
			default:
			}
		}

		events := zmq4.POLLIN
		if pending != nil {
			events |= zmq4.POLLOUT
		}
		poller := zmq4.NewPoller()
		poller.Add(socket, events)
		polled, err := poller.Poll(zeroMQPollInterval)
		if err != nil {
			conn.finish(fmt.Errorf("poll zeromq dealer: %w", err))
			return
		}

		select {
		case <-conn.done:
			return
		default:
		}

		if len(polled) == 0 {
			continue
		}
		for _, item := range polled {
			if item.Events&zmq4.POLLIN != 0 {
				frames, err := socket.RecvMessageBytes(0)
				if err != nil {
					conn.finish(fmt.Errorf("receive zeromq dealer payload: %w", err))
					return
				}
				if len(frames) == 0 {
					continue
				}
				if !conn.deliver(frames[len(frames)-1]) {
					return
				}
			}
			if item.Events&zmq4.POLLOUT != 0 && pending != nil {
				if _, err := socket.SendBytes(pending, zmq4.DONTWAIT); err != nil {
					conn.finish(fmt.Errorf("send zeromq dealer payload: %w", err))
					return
				}
				pending = nil
			}
		}
	}
}

func runZeroMQRouter(ctx context.Context, socket *zmq4.Socket, listener *ZeroMQMuxListener) {
	peers := make(map[string]*zeroMQRouterPeer)
	pending := make([]zeroMQRouterOutbound, 0, outboundQueueSize)
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
		events := zmq4.POLLIN
		if len(pending) > 0 {
			events |= zmq4.POLLOUT
		}
		poller := zmq4.NewPoller()
		poller.Add(socket, events)
		polled, err := poller.Poll(zeroMQPollInterval)
		if err != nil {
			return
		}
		if len(polled) == 0 {
			continue
		}

		for _, item := range polled {
			if item.Events&zmq4.POLLIN != 0 {
				frames, err := socket.RecvMessageBytes(0)
				if err != nil {
					return
				}
				if len(frames) < 2 {
					continue
				}
				identity := cloneBytes(frames[0])
				payload := frames[len(frames)-1]
				identityKey := hex.EncodeToString(identity)
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
			if item.Events&zmq4.POLLOUT != 0 && len(pending) > 0 {
				outbound := pending[0]
				pending = pending[1:]
				if _, ok := peers[outbound.identityKey]; !ok {
					continue
				}
				if _, err := socket.SendMessageDontwait(outbound.identity, outbound.payload); err != nil {
					closePeer(outbound.identityKey)
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
			identity:    cloneBytes(identity),
			payload:     cloneBytes(payload),
		}
		select {
		case <-ctxDone(ctx):
			return ctx.Err()
		case <-conn.done:
			return conn.closedErr()
		case <-l.done:
			return conn.closedErr()
		case l.sendCh <- outbound:
			return nil
		}
	}
	conn.onClose = func() {
		select {
		case l.connClosedCh <- identityKey:
		case <-l.done:
		default:
		}
	}
	accept(conn)
	return conn
}

func (c *zeroMQTransportConn) Send(ctx context.Context, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-c.done:
		return c.closedErr()
	default:
	}
	if c.sendFn != nil {
		return c.sendFn(ctx, cloneBytes(payload))
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.closedErr()
	case c.sendCh <- cloneBytes(payload):
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
		return cloneBytes(payload), nil
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
	case c.recvCh <- cloneBytes(payload):
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
