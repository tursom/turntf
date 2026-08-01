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

	zmq4 "github.com/pebbe/zmq4/draft"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

var errZeroMQNotBuilt error
var zeroMQContextConfigOnce sync.Once
var zeroMQContextConfigErr error

const (
	// ZeroMQ套接字缓冲区大小（1MB）和连接积压。
	zeroMQSocketBufferBytes = 1 << 20
	zeroMQSocketBacklog     = 1024
	// ZMTP心跳用于检测没有正常TCP关闭握手的半开连接。
	zeroMQHeartbeatInterval = 15 * time.Second
	zeroMQHeartbeatTimeout  = 45 * time.Second
)

// zeroMQCurveAuthState 管理全局ZeroMQ Curve认证器的引用计数。
var zeroMQCurveAuthState struct {
	sync.Mutex
	refs int
	next uint64
}

// zeroMQDialer 是使用DEALER套接字的ZeroMQ出站拨号器。
type zeroMQDialer struct {
	cfg              ZeroMQConfig
	serverKeyForPeer func(string) string
}

// zeroMQClusterListener 包装ZeroMQMuxListener以适配Listener接口。
type zeroMQClusterListener struct {
	mux *ZeroMQMuxListener
}

// zeroMQWakePair 使用inproc PAIR套接字实现可轮询的唤醒信号。
// 当Go端队列有活动时，通过此机制通知C端的轮询循环，
// 避免引入100ms的轮询延迟。
type zeroMQWakePair struct {
	recv *zmq4.Socket
	send *zmq4.Socket

	mu      sync.Mutex
	pending bool
	closed  bool
}

// zeroMQSocketMonitor 将单个DEALER socket的断线事件接入其事件循环。
type zeroMQSocketMonitor struct {
	socket *zmq4.Socket
}

// ZeroMQMuxListener 是一个多路复用ZeroMQ ROUTER套接字，
// 可在同一端口上接受集群连接和客户端连接。
// 通过hello消息中的角色标识区分连接类型。
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

// zeroMQRouterOutbound 是ROUTER套接字的一条待发送消息。
type zeroMQRouterOutbound struct {
	identityKey string
	identity    []byte
	payload     []byte
	result      chan error
}

// zeroMQRouterPeer 表示ROUTER套接字上的一个已连接对等节点。
type zeroMQRouterPeer struct {
	identity []byte
	conn     *zeroMQTransportConn
	role     internalproto.ZeroMQMuxHello_Role
}

// zeroMQTransportConn 通过内部通道实现TransportConn接口。
// DEALER连接通过sendCh/recvCh进行通信；
// ROUTER连接使用sendFn回调和recvCh。
type zeroMQTransportConn struct {
	direction  string
	localAddr  string
	remoteAddr string

	sendCh        chan []byte
	recvCh        chan []byte
	sendFn        func(ctx context.Context, payload []byte) error
	sendAndWaitFn func(ctx context.Context, payload []byte) error

	done    chan struct{}
	onClose func()
	wake    *zeroMQWakePair

	closeOnce sync.Once
	errMu     sync.Mutex
	closeErr  error
}

// zeroMQEnabled 当zeromq编译标签启用时返回true。
func zeroMQEnabled() bool {
	return true
}

// newZeroMQDialer 创建一个ZeroMQ拨号器（默认配置）。
func newZeroMQDialer() Dialer {
	return newZeroMQDialerWithConfig(ZeroMQConfig{}, nil)
}

// newZeroMQDialerWithConfig 使用给定配置创建ZeroMQ拨号器。
func newZeroMQDialerWithConfig(cfg ZeroMQConfig, serverKeyForPeer func(string) string) Dialer {
	return &zeroMQDialer{cfg: cfg, serverKeyForPeer: serverKeyForPeer}
}

// newZeroMQListener 创建一个ZeroMQ集群监听器。
func newZeroMQListener(bindURL string) Listener {
	return &zeroMQClusterListener{mux: NewZeroMQMuxListener(bindURL)}
}

// NewZeroMQMuxListener 创建一个ZeroMQ多路复用监听器。
func NewZeroMQMuxListener(bindURL string) *ZeroMQMuxListener {
	return NewZeroMQMuxListenerWithConfig(bindURL, ZeroMQConfig{})
}

// NewZeroMQMuxListenerWithConfig 使用给定配置创建ZeroMQ多路复用监听器。
func NewZeroMQMuxListenerWithConfig(bindURL string, cfg ZeroMQConfig) *ZeroMQMuxListener {
	return &ZeroMQMuxListener{
		bindURL:      bindURL,
		cfg:          cfg,
		sendCh:       make(chan zeroMQRouterOutbound, outboundQueueSize),
		connClosedCh: make(chan string, outboundQueueSize),
		done:         make(chan struct{}),
	}
}

// SetClusterAccept 设置集群连接的回调函数。
func (l *ZeroMQMuxListener) SetClusterAccept(accept func(TransportConn)) {
	if l == nil {
		return
	}
	l.clusterAccept = accept
}

// SetClientAccept 设置客户端连接的回调函数。
func (l *ZeroMQMuxListener) SetClientAccept(accept func(TransportConn)) {
	if l == nil {
		return
	}
	l.clientAccept = accept
}

// Dial 使用DEALER套接字向对等URL发起ZeroMQ连接。
// 流程：配置运行时 → 创建DEALER → 配置身份和安全 → 连接 → 发送hello消息。
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
	monitor, err := newZeroMQDisconnectMonitor(socket)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	address, err := zeroMQDialAddress(peerURL)
	if err != nil {
		monitor.Close()
		_ = socket.Close()
		return nil, err
	}
	if err := socket.Connect(address); err != nil {
		monitor.Close()
		_ = socket.Close()
		return nil, fmt.Errorf("connect zeromq dealer %s: %w", peerURL, err)
	}
	wake, err := newZeroMQWakePair()
	if err != nil {
		monitor.Close()
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
	go runZeroMQDealer(socket, wake, monitor, conn)
	if err := writeZeroMQMuxHello(ctx, conn, internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLUSTER); err != nil {
		conn.finish(err)
		return nil, err
	}
	return conn, nil
}

// Start 启动ZeroMQ监听器。
func (l *zeroMQClusterListener) Start(ctx context.Context, accept func(TransportConn)) error {
	l.mux.SetClusterAccept(accept)
	return l.mux.Start(ctx)
}

// Close 关闭ZeroMQ监听器。
func (l *zeroMQClusterListener) Close() error {
	return l.mux.Close()
}

// Start 启动ZeroMQ多路复用监听器。
// 创建ROUTER套接字，配置安全，绑定地址，启动事件循环。
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
	if err := socket.SetRouterMandatory(1); err != nil {
		_ = socket.Close()
		return fmt.Errorf("set zeromq router mandatory: %w", err)
	}
	if err := socket.SetRouterNotify(zmq4.NotifyDisconnect); err != nil {
		_ = socket.Close()
		return fmt.Errorf("set zeromq router disconnect notifications: %w", err)
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

// Close 关闭多路复用监听器。
func (l *ZeroMQMuxListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
		l.signalWake()
	})
	l.waitGroup.Wait()
	return nil
}

// configureClientSecurity 配置DEALER套接字的CurveZMQ客户端安全。
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

// configureServerSecurity 配置ROUTER套接字的CurveZMQ服务器安全。
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

// releaseServerSecurity 释放CurveZMQ认证器资源。
func (l *ZeroMQMuxListener) releaseServerSecurity() {
	if !l.curveAuthStarted {
		return
	}
	releaseZeroMQCurveAuth(l.curveAuthDomain)
	l.curveAuthStarted = false
	l.curveAuthDomain = ""
}

// acquireZeroMQCurveAuth 全局引用计数管理CurveZMQ认证器。
// 为每个监听器分配唯一的ZAP域。
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

// releaseZeroMQCurveAuth 释放一个CurveZMQ认证域。
// 当引用计数归零时停止认证器。
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
		time.Sleep(100 * time.Millisecond)
	}
}

// zeroMQConfigSecurity 返回规范化的ZeroMQ安全模式。
func zeroMQConfigSecurity(cfg ZeroMQConfig) string {
	security := strings.ToLower(strings.TrimSpace(cfg.Security))
	if security == "" {
		return ZeroMQSecurityNone
	}
	return security
}

// configureZeroMQSocket 应用通用的ZeroMQ套接字配置。
// 包括：linger=0、immediate=true、backlog、缓冲区大小、高水位线、TCP keepalive、ZMTP心跳、最大消息大小。
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
	if err := socket.SetHeartbeatIvl(zeroMQHeartbeatInterval); err != nil {
		return fmt.Errorf("set zeromq heartbeat interval: %w", err)
	}
	if err := socket.SetHeartbeatTimeout(zeroMQHeartbeatTimeout); err != nil {
		return fmt.Errorf("set zeromq heartbeat timeout: %w", err)
	}
	if err := socket.SetHeartbeatTtl(zeroMQHeartbeatTimeout); err != nil {
		return fmt.Errorf("set zeromq heartbeat ttl: %w", err)
	}
	if err := socket.SetMaxmsgsize(websocketReadLimit); err != nil {
		return fmt.Errorf("set zeromq max message size: %w", err)
	}
	return nil
}

// runZeroMQDealer 运行DEALER套接字的事件循环。
// 使用零拷贝的zmq4.Poller，通过唤醒对避免忙轮询。
func runZeroMQDealer(socket *zmq4.Socket, wake *zeroMQWakePair, monitor *zeroMQSocketMonitor, conn *zeroMQTransportConn) {
	defer conn.finish(errSessionClosed)
	defer func() {
		_ = socket.Monitor("", 0)
		if monitor != nil {
			monitor.Close()
		}
		_ = socket.Close()
	}()
	if wake != nil {
		defer wake.Close()
	}
	poller := zmq4.NewPoller()
	socketPollID := poller.Add(socket, zmq4.POLLIN)
	if wake != nil && wake.recv != nil {
		poller.Add(wake.recv, zmq4.POLLIN)
	}
	if monitor != nil && monitor.socket != nil {
		poller.Add(monitor.socket, zmq4.POLLIN)
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
			if monitor != nil && item.Socket == monitor.socket {
				disconnected, err := zeroMQReceiveDisconnectEvents(monitor.socket)
				if err != nil {
					conn.finish(fmt.Errorf("receive zeromq dealer monitor event: %w", err))
					return
				}
				if disconnected {
					conn.finish(fmt.Errorf("zeromq dealer disconnected"))
					return
				}
				continue
			}
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

// runZeroMQRouter 运行ROUTER套接字的事件循环。
// 管理对等节点映射，分发入站数据到对应连接，发送出站数据。
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

// acceptHandler 根据hello消息中的角色返回对应的接受回调。
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

// newInboundConn 创建入站ZeroMQ连接的TransportConn。
// 使用sendFn回调通过ROUTER套接字发送数据。
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
	conn.sendAndWaitFn = func(ctx context.Context, payload []byte) error {
		result := make(chan error, 1)
		outbound := zeroMQRouterOutbound{
			identityKey: identityKey,
			identity:    identity,
			payload:     payload,
			result:      result,
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
		}
		select {
		case <-ctxDone(ctx):
			return ctx.Err()
		case <-conn.done:
			return conn.closedErr()
		case <-l.done:
			return conn.closedErr()
		case err := <-result:
			return err
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

// Send 发送消息（复制负载数据）。
func (c *zeroMQTransportConn) Send(ctx context.Context, payload []byte) error {
	return c.sendQueuedPayload(ctx, cloneBytes(payload))
}

// SendOwned 发送消息（获取负载数据的所有权，避免复制）。
func (c *zeroMQTransportConn) SendOwned(ctx context.Context, payload []byte) error {
	return c.sendQueuedPayload(ctx, payload)
}

// SendAndWait 发送消息，并在底层 socket 接受该消息后返回。
// 普通 Send 保持异步排队语义；该方法用于发送完终止响应后必须立即关闭连接的路径。
func (c *zeroMQTransportConn) SendAndWait(ctx context.Context, payload []byte) error {
	if c.sendAndWaitFn != nil {
		return c.sendAndWaitFn(ctx, cloneBytes(payload))
	}
	return c.Send(ctx, payload)
}

// sendQueuedPayload 将负载排入发送队列并信号通知唤醒。
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

// Receive 从接收通道读取下一条消息。
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

// Close 关闭连接。
func (c *zeroMQTransportConn) Close() error {
	c.finish(errSessionClosed)
	return nil
}

// LocalAddr 返回本地地址。
func (c *zeroMQTransportConn) LocalAddr() string {
	return c.localAddr
}

// RemoteAddr 返回远程地址。
func (c *zeroMQTransportConn) RemoteAddr() string {
	return c.remoteAddr
}

// Direction 返回连接方向：inbound或outbound。
func (c *zeroMQTransportConn) Direction() string {
	return c.direction
}

// Transport 返回传输类型字符串zeromq。
func (c *zeroMQTransportConn) Transport() string {
	return transportZeroMQ
}

// deliver 将负载投递到连接的接收通道。
func (c *zeroMQTransportConn) deliver(payload []byte) bool {
	select {
	case <-c.done:
		return false
	case c.recvCh <- payload:
		return true
	}
}

// finish 安全关闭连接，通知所有等待的goroutine。
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

// closedErr 返回连接的关闭错误。
func (c *zeroMQTransportConn) closedErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return errSessionClosed
}

// zeroMQDialAddress 将zmq+tcp:// URL转换为ZeroMQ拨号所需的tcp:// URL。
func zeroMQDialAddress(peerURL string) (string, error) {
	parsed, err := url.Parse(peerURL)
	if err != nil {
		return "", fmt.Errorf("parse zeromq peer url: %w", err)
	}
	parsed.Scheme = zeroMQBindSchemeTCP
	return parsed.String(), nil
}

// writeZeroMQMuxHello 发送ZeroMQ多路复用的hello消息，声明连接角色。
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

// parseZeroMQMuxHello 解析hello消息，提取连接的角色。
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

// newZeroMQIdentity 生成16字节的随机十六进制ZeroMQ身份标识。
func newZeroMQIdentity() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate zeromq identity: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// shortZeroMQIdentity 返回身份标识的简短版本（用于日志记录）。
func shortZeroMQIdentity(identity []byte) string {
	encoded := hex.EncodeToString(identity)
	if len(encoded) <= 16 {
		return encoded
	}
	return encoded[:16]
}

// cloneBytes 创建字节切片的副本。
func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}

// newZeroMQWakePair 创建一个PAIR套接字对用于Go端到C端的事件通知。
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

// configureZeroMQWakeSocket 配置PAIR唤醒套接字。
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

// Signal 通过PAIR套接字发送唤醒信号。
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

// Drain 排空PAIR套接字中的所有待处理唤醒信号。
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

// Reset 重置待处理标记。
func (w *zeroMQWakePair) Reset() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.pending = false
	w.mu.Unlock()
}

// Close 关闭PAIR套接字对。
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

// signalWake 发送ROUTER监听器的唤醒信号。
func (l *ZeroMQMuxListener) signalWake() {
	if l == nil || l.wake == nil {
		return
	}
	l.wake.Signal()
}

// signalWake 发送连接的唤醒信号。
func (c *zeroMQTransportConn) signalWake() {
	if c == nil || c.wake == nil {
		return
	}
	c.wake.Signal()
}

// zeroMQDrainBytesQueue 非阻塞地将通道中的所有待发送字节排入pending切片。
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

// zeroMQDrainRouterControlQueues 非阻塞地排空ROUTER套接字的控制队列。
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

// zeroMQFlushDealerMessages 非阻塞地将待发送消息写入DEALER套接字。
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

// zeroMQReceiveDealerMessages 从DEALER套接字接收所有可用消息。
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

// zeroMQReceiveRouterMessages 从ROUTER套接字接收消息。
// 对于未知身份的消息，解析hello并创建新对等节点；
// 对于已知身份的消息，投递到对应的连接。
func zeroMQReceiveRouterMessages(socket *zmq4.Socket, listener *ZeroMQMuxListener, peers map[string]*zeroMQRouterPeer, closePeer func(string)) bool {
	for {
		identity, payload, err := zeroMQReceiveRouterMessage(socket)
		if err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			return false
		}
		if len(identity) == 0 {
			continue
		}
		identityKey := string(identity)
		peer, ok := peers[identityKey]
		if len(payload) == 0 {
			if ok {
				closePeer(identityKey)
			}
			continue
		}
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

// newZeroMQDisconnectMonitor 为一个DEALER socket创建只监听断线事件的PAIR socket。
func newZeroMQDisconnectMonitor(socket *zmq4.Socket) (*zeroMQSocketMonitor, error) {
	if socket == nil {
		return nil, fmt.Errorf("zeromq monitor socket is nil")
	}
	endpointID, err := newZeroMQIdentity()
	if err != nil {
		return nil, err
	}
	endpoint := "inproc://turntf-zeromq-monitor-" + endpointID
	if err := socket.Monitor(endpoint, zmq4.EVENT_DISCONNECTED); err != nil {
		return nil, fmt.Errorf("enable zeromq dealer disconnect monitor: %w", err)
	}
	monitorSocket, err := zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		_ = socket.Monitor("", 0)
		return nil, fmt.Errorf("create zeromq dealer monitor socket: %w", err)
	}
	if err := monitorSocket.SetLinger(0); err != nil {
		_ = monitorSocket.Close()
		_ = socket.Monitor("", 0)
		return nil, fmt.Errorf("set zeromq dealer monitor linger: %w", err)
	}
	if err := monitorSocket.Connect(endpoint); err != nil {
		_ = monitorSocket.Close()
		_ = socket.Monitor("", 0)
		return nil, fmt.Errorf("connect zeromq dealer monitor: %w", err)
	}
	return &zeroMQSocketMonitor{socket: monitorSocket}, nil
}

// Close 关闭DEALER monitor接收socket。
func (m *zeroMQSocketMonitor) Close() {
	if m == nil || m.socket == nil {
		return
	}
	_ = m.socket.Close()
}

// zeroMQReceiveDisconnectEvents 排空monitor事件并报告是否观察到物理断线。
func zeroMQReceiveDisconnectEvents(socket *zmq4.Socket) (bool, error) {
	for {
		event, _, _, err := socket.RecvEvent(zmq4.DONTWAIT)
		if err != nil {
			if zeroMQWouldBlock(err) {
				return false, nil
			}
			return false, err
		}
		if event&zmq4.EVENT_DISCONNECTED != 0 {
			return true, nil
		}
	}
}

// zeroMQFlushRouterMessages 将待发送消息刷新到ROUTER套接字。
func zeroMQFlushRouterMessages(socket *zmq4.Socket, peers map[string]*zeroMQRouterPeer, closePeer func(string), pending *[]zeroMQRouterOutbound) bool {
	for len(*pending) > 0 {
		outbound := (*pending)[0]
		if _, ok := peers[outbound.identityKey]; !ok {
			outbound.finish(errSessionClosed)
			*pending = (*pending)[1:]
			continue
		}
		if _, err := socket.SendBytes(outbound.identity, zmq4.SNDMORE|zmq4.DONTWAIT); err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			outbound.finish(err)
			closePeer(outbound.identityKey)
			*pending = (*pending)[1:]
			continue
		}
		if _, err := socket.SendBytes(outbound.payload, zmq4.DONTWAIT); err != nil {
			if zeroMQWouldBlock(err) {
				return true
			}
			outbound.finish(err)
			closePeer(outbound.identityKey)
			*pending = (*pending)[1:]
			continue
		}
		outbound.finish(nil)
		*pending = (*pending)[1:]
	}
	return true
}

func (o zeroMQRouterOutbound) finish(err error) {
	if o.result == nil {
		return
	}
	o.result <- err
}

// zeroMQReceiveLastFrame 接收多部分消息的最后一帧。
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

// zeroMQReceiveRouterMessage 接收ROUTER多部分消息（身份 + 负载）。
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

// zeroMQWouldBlock 检查ZeroMQ错误是否为EAGAIN（操作会阻塞）。
func zeroMQWouldBlock(err error) bool {
	return zmq4.AsErrno(err) == zmq4.Errno(syscall.EAGAIN)
}

// ensureZeroMQRuntimeConfigured 初始化全局ZeroMQ上下文。
// I/O线程数设置为GOMAXPROCS/2，最少1个最多4个。
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
