package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tursom/turntf/internal/mesh"
)

const transportTCPMTLS = "tcp_mtls"
const tcpMTLSScheme = "tcp+tls"

// TCPMTLSConfig 仅用于集群；禁用时不监听、不加载证书、不允许发现拨号。
// 证书 URI SAN 必须且只能包含一个 urn:turntf:node:<正整数> 身份。
type TCPMTLSConfig struct {
	Enabled             bool     `toml:"enabled"`
	ListenAddr          string   `toml:"listen_addr"`
	AdvertisedEndpoints []string `toml:"advertised_endpoints"`
	CAFile              string   `toml:"ca_file"`
	CertFile            string   `toml:"cert_file"`
	KeyFile             string   `toml:"key_file"`
	AllowedNodeIDs      []int64  `toml:"allowed_node_ids"`
	HandshakeTimeoutMs  int64    `toml:"handshake_timeout_ms"`
	MaxFrameBytes       int      `toml:"max_frame_bytes"`
}

func (c TCPMTLSConfig) withDefaults() TCPMTLSConfig {
	if c.HandshakeTimeoutMs == 0 {
		c.HandshakeTimeoutMs = 5000
	}
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = 8 << 20
	}
	return c
}

func parseTCPMTLSEndpoint(raw string) (*url.URL, int64, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, 0, err
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(u.Path, "/"), 10, 64)
	port, portErr := strconv.Atoi(u.Port())
	if err != nil || id <= 0 || u.Path != "/"+strconv.FormatInt(id, 10) || u.Scheme != tcpMTLSScheme || u.Hostname() == "" || u.Hostname() == "0.0.0.0" || u.Hostname() == "::" || portErr != nil || port < 1 || port > 65535 || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" {
		return nil, 0, fmt.Errorf("tcp mTLS endpoint must be tcp+tls://host:port/nodeID")
	}
	u.Host = net.JoinHostPort(strings.ToLower(u.Hostname()), strconv.Itoa(port))
	return u, id, nil
}

func (c TCPMTLSConfig) validate() error {
	if !c.Enabled {
		return nil
	}
	if c.CAFile == "" || c.CertFile == "" || c.KeyFile == "" {
		return fmt.Errorf("tcp mTLS requires ca_file, cert_file and key_file")
	}
	if c.HandshakeTimeoutMs < 1 || c.HandshakeTimeoutMs > 60000 || c.MaxFrameBytes < 1 || c.MaxFrameBytes > 64<<20 {
		return fmt.Errorf("tcp mTLS requires timeout 1..60000ms and frame limit 1..67108864 bytes")
	}
	if len(c.AllowedNodeIDs) == 0 {
		return fmt.Errorf("tcp mTLS requires allowed_node_ids")
	}
	for _, id := range c.AllowedNodeIDs {
		if id <= 0 {
			return fmt.Errorf("tcp mTLS allowed node IDs must be positive")
		}
	}
	if c.ListenAddr != "" {
		if _, _, err := net.SplitHostPort(c.ListenAddr); err != nil {
			return fmt.Errorf("tcp mTLS listen_addr: %w", err)
		}
	}
	if c.ListenAddr == "" && len(c.AdvertisedEndpoints) > 0 {
		return fmt.Errorf("tcp mTLS advertised endpoints require a listener")
	}
	for _, endpoint := range c.AdvertisedEndpoints {
		if _, _, err := parseTCPMTLSEndpoint(endpoint); err != nil {
			return err
		}
	}
	return nil
}

func certificateNodeID(cert *x509.Certificate) (int64, error) {
	var id int64
	for _, uri := range cert.URIs {
		if !strings.HasPrefix(uri.String(), "urn:turntf:node:") {
			continue
		}
		raw := strings.TrimPrefix(uri.String(), "urn:turntf:node:")
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n <= 0 || strconv.FormatInt(n, 10) != raw || id != 0 {
			return 0, fmt.Errorf("invalid or ambiguous certificate node identity")
		}
		id = n
	}
	if id == 0 {
		return 0, fmt.Errorf("certificate missing turntf node URI SAN")
	}
	return id, nil
}

// TCPMTLSMeshTransportAdapter 拥有监听器、握手和连接生命周期。计数器不记录证书或私钥。
type TCPMTLSMeshTransportAdapter struct {
	cfg               TCPMTLSConfig
	nodeID            int64
	acceptCh          chan mesh.TransportConn
	mu                sync.Mutex
	ctx               context.Context
	cancel            context.CancelFunc
	listener          net.Listener
	tlsConfig         *tls.Config
	closed            bool
	conns             map[net.Conn]struct{}
	wg                sync.WaitGroup
	dialAttempts      atomic.Uint64
	handshakeRejected atomic.Uint64
	established       atomic.Uint64
}

type TCPMTLSStats struct {
	DialAttempts      uint64 `json:"dial_attempts"`
	HandshakeRejected uint64 `json:"handshake_rejected"`
	Established       uint64 `json:"established_total"`
}

func (a *TCPMTLSMeshTransportAdapter) Stats() TCPMTLSStats {
	return TCPMTLSStats{a.dialAttempts.Load(), a.handshakeRejected.Load(), a.established.Load()}
}
func NewTCPMTLSMeshTransportAdapter(cfg Config) *TCPMTLSMeshTransportAdapter {
	if !cfg.TCPMTLS.Enabled {
		return nil
	}
	return &TCPMTLSMeshTransportAdapter{cfg: cfg.TCPMTLS.withDefaults(), nodeID: cfg.NodeID, acceptCh: make(chan mesh.TransportConn, meshTransportAcceptQueue), conns: make(map[net.Conn]struct{})}
}
func (a *TCPMTLSMeshTransportAdapter) Kind() mesh.TransportKind          { return mesh.TransportTCPMTLS }
func (a *TCPMTLSMeshTransportAdapter) Accept() <-chan mesh.TransportConn { return a.acceptCh }
func (a *TCPMTLSMeshTransportAdapter) LocalCapabilities() *mesh.TransportCapability {
	return &mesh.TransportCapability{Transport: a.Kind(), InboundEnabled: a.cfg.ListenAddr != "", OutboundEnabled: true, AdvertisedEndpoints: append([]string(nil), a.cfg.AdvertisedEndpoints...)}
}
func (a *TCPMTLSMeshTransportAdapter) allowed(id int64) bool {
	if id == a.nodeID {
		return false
	}
	for _, allowed := range a.cfg.AllowedNodeIDs {
		if id == allowed {
			return true
		}
	}
	return false
}
func (a *TCPMTLSMeshTransportAdapter) loadTLS() (*tls.Config, error) {
	pair, err := tls.LoadX509KeyPair(a.cfg.CertFile, a.cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, err
	}
	id, err := certificateNodeID(leaf)
	if err != nil {
		return nil, err
	}
	if id != a.nodeID {
		return nil, fmt.Errorf("tcp mTLS local certificate node ID mismatch")
	}
	pem, err := os.ReadFile(a.cfg.CAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tcp mTLS CA file contains no certificates")
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return nil, fmt.Errorf("tcp mTLS local certificate: %w", err)
		}
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, RootCAs: roots, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, VerifyConnection: func(s tls.ConnectionState) error {
		if len(s.VerifiedChains) == 0 || len(s.PeerCertificates) == 0 {
			return fmt.Errorf("tcp mTLS requires verified certificate chain")
		}
		id, err := certificateNodeID(s.PeerCertificates[0])
		if err != nil {
			return err
		}
		if !a.allowed(id) {
			return fmt.Errorf("tcp mTLS peer node ID is not allowed")
		}
		return nil
	}}, nil
}
func (a *TCPMTLSMeshTransportAdapter) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.ctx != nil {
		return fmt.Errorf("tcp mTLS adapter already started or closed")
	}
	if err := a.cfg.validate(); err != nil {
		return err
	}
	for _, endpoint := range a.cfg.AdvertisedEndpoints {
		_, id, _ := parseTCPMTLSEndpoint(endpoint)
		if id != a.nodeID {
			return fmt.Errorf("tcp mTLS advertised node ID mismatch")
		}
	}
	cfg, err := a.loadTLS()
	if err != nil {
		return err
	}
	var listener net.Listener
	if a.cfg.ListenAddr != "" {
		listener, err = net.Listen("tcp", a.cfg.ListenAddr)
		if err != nil {
			return err
		}
	}
	a.tlsConfig = cfg
	a.ctx, a.cancel = context.WithCancel(ctx)
	a.listener = listener
	if listener != nil {
		a.wg.Add(1)
		go a.acceptLoop()
	}
	go func() { <-a.ctx.Done(); _ = a.Close() }()
	return nil
}
func (a *TCPMTLSMeshTransportAdapter) track(c net.Conn) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.ctx.Err() != nil {
		_ = c.Close()
		return false
	}
	a.conns[c] = struct{}{}
	return true
}
func (a *TCPMTLSMeshTransportAdapter) untrack(c net.Conn) {
	a.mu.Lock()
	delete(a.conns, c)
	a.mu.Unlock()
}
func (a *TCPMTLSMeshTransportAdapter) acceptLoop() {
	defer a.wg.Done()
	// 限制并发 TLS 握手，避免未认证连接耗尽 goroutine。
	slots := make(chan struct{}, 64)
	for {
		raw, err := a.listener.Accept()
		if err != nil {
			return
		}
		select {
		case slots <- struct{}{}:
		default:
			_ = raw.Close()
			continue
		}
		if !a.track(raw) {
			<-slots
			return
		}
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			defer func() { <-slots }()
			conn := tls.Server(raw, a.tlsConfig)
			ctx, cancel := context.WithTimeout(a.ctx, time.Duration(a.cfg.HandshakeTimeoutMs)*time.Millisecond)
			defer cancel()
			if err := conn.HandshakeContext(ctx); err != nil {
				a.handshakeRejected.Add(1)
				_ = raw.Close()
				a.untrack(raw)
				return
			}
			id, _ := certificateNodeID(conn.ConnectionState().PeerCertificates[0])
			framed := a.wrap(conn, raw, id, "")
			select {
			case a.acceptCh <- framed:
			case <-a.ctx.Done():
				_ = framed.Close()
			default:
				_ = framed.Close()
			}
		}()
	}
}
func (a *TCPMTLSMeshTransportAdapter) Dial(ctx context.Context, endpoint string) (mesh.TransportConn, error) {
	u, id, err := parseTCPMTLSEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if !a.allowed(id) {
		return nil, fmt.Errorf("tcp mTLS target node ID is not allowed")
	}
	a.mu.Lock()
	if a.closed || a.ctx == nil || a.ctx.Err() != nil {
		a.mu.Unlock()
		return nil, fmt.Errorf("tcp mTLS adapter not running")
	}
	parent := a.ctx
	cfg := a.tlsConfig.Clone()
	a.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(a.cfg.HandshakeTimeoutMs)*time.Millisecond)
	defer cancel()
	stop := context.AfterFunc(parent, cancel)
	defer stop()
	cfg.ServerName = u.Hostname()
	verify := cfg.VerifyConnection
	cfg.VerifyConnection = func(s tls.ConnectionState) error {
		if err := verify(s); err != nil {
			return err
		}
		actual, err := certificateNodeID(s.PeerCertificates[0])
		if err != nil || actual != id {
			return fmt.Errorf("tcp mTLS endpoint certificate identity mismatch")
		}
		return nil
	}
	a.dialAttempts.Add(1)
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}
	if !a.track(raw) {
		return nil, context.Canceled
	}
	conn := tls.Client(raw, cfg)
	if err := conn.HandshakeContext(ctx); err != nil {
		a.handshakeRejected.Add(1)
		_ = raw.Close()
		a.untrack(raw)
		return nil, err
	}
	return a.wrap(conn, raw, id, u.String()), nil
}
func (a *TCPMTLSMeshTransportAdapter) wrap(conn net.Conn, raw net.Conn, id int64, hint string) *tcpMTLSConn {
	a.established.Add(1)
	if hint == "" {
		hint = raw.RemoteAddr().String()
	}
	return &tcpMTLSConn{Conn: conn, nodeID: id, hint: hint, maxFrame: a.cfg.MaxFrameBytes, onClose: func() { a.untrack(raw) }}
}
func (a *TCPMTLSMeshTransportAdapter) Close() error {
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		if a.cancel != nil {
			a.cancel()
		}
		if a.listener != nil {
			_ = a.listener.Close()
		}
		for c := range a.conns {
			_ = c.Close()
		}
	}
	a.mu.Unlock()
	a.wg.Wait()
	for {
		select {
		case c := <-a.acceptCh:
			_ = c.Close()
		default:
			return nil
		}
	}
}

// 每个方向独立串行化；取消或部分帧失败必须关闭整个流，禁止继续解析错位数据。
type tcpMTLSConn struct {
	net.Conn
	nodeID         int64
	hint           string
	maxFrame       int
	sendMu, recvMu sync.Mutex
	closeOnce      sync.Once
	onClose        func()
}

func (c *tcpMTLSConn) AuthenticatedNodeID() int64    { return c.nodeID }
func (c *tcpMTLSConn) RemoteNodeHint() string        { return c.hint }
func (c *tcpMTLSConn) Transport() mesh.TransportKind { return mesh.TransportTCPMTLS }
func (c *tcpMTLSConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.Conn.Close()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return err
}
func (c *tcpMTLSConn) Send(ctx context.Context, p []byte) error {
	if len(p) == 0 || len(p) > c.maxFrame {
		return fmt.Errorf("tcp mTLS frame length out of bounds")
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(p)))
	for _, part := range [][]byte{header[:], p} {
		for len(part) > 0 {
			n, err := c.Conn.Write(part)
			if err != nil {
				_ = c.Close()
				return err
			}
			if n == 0 {
				_ = c.Close()
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}
func (c *tcpMTLSConn) Receive(ctx context.Context) ([]byte, error) {
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var header [4]byte
	if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
		_ = c.Close()
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || uint64(n) > uint64(c.maxFrame) {
		_ = c.Close()
		return nil, fmt.Errorf("tcp mTLS frame length out of bounds: %d", n)
	}
	p := make([]byte, int(n))
	if _, err := io.ReadFull(c.Conn, p); err != nil {
		_ = c.Close()
		return nil, err
	}
	return p, nil
}
