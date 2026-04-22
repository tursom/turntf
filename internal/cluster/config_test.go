package cluster

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestNormalizeConfiguredPeerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr string
	}{
		{
			name: "ws",
			raw:  "WS://Example.COM/internal/cluster/ws",
			want: "ws://example.com/internal/cluster/ws",
		},
		{
			name: "wss",
			raw:  "WSS://Example.COM/internal/cluster/ws",
			want: "wss://example.com/internal/cluster/ws",
		},
		{
			name: "zmq tcp",
			raw:  "zmq+tcp://Example.COM:9091",
			want: "zmq+tcp://example.com:9091",
		},
		{
			name:    "zmq wildcard host",
			raw:     "zmq+tcp://0.0.0.0:9091",
			wantErr: "peer url host cannot be a wildcard address",
		},
		{
			name:    "zmq path",
			raw:     "zmq+tcp://127.0.0.1:9091/path",
			wantErr: "peer url path is not allowed",
		},
		{
			name:    "invalid scheme",
			raw:     "http://127.0.0.1:9091",
			wantErr: "peer url scheme must be ws, wss, or zmq+tcp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeConfiguredPeerURL(tt.raw)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize configured peer url: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected normalized peer url: got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestNormalizeZeroMQBindURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr string
	}{
		{
			name: "ipv4 wildcard",
			raw:  "tcp://0.0.0.0:9090",
			want: "tcp://0.0.0.0:9090",
		},
		{
			name: "ipv6 wildcard",
			raw:  "tcp://[::]:9090",
			want: "tcp://[::]:9090",
		},
		{
			name:    "invalid scheme",
			raw:     "ws://127.0.0.1:9090",
			wantErr: "zeromq bind url scheme must be tcp",
		},
		{
			name:    "missing port",
			raw:     "tcp://127.0.0.1",
			wantErr: "zeromq bind url port cannot be empty",
		},
		{
			name:    "path not allowed",
			raw:     "tcp://127.0.0.1:9090/path",
			wantErr: "zeromq bind url path is not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeZeroMQBindURL(tt.raw)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize zeromq bind url: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected normalized bind url: got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestNormalizeLibP2PAddrs(t *testing.T) {
	t.Parallel()

	peerAddr := testLibP2PPeerAddr(t, "/ip4/127.0.0.1/tcp/4001")
	normalizedPeer, err := normalizeConfiguredPeerURL(" " + peerAddr + " ")
	if err != nil {
		t.Fatalf("normalize libp2p peer addr: %v", err)
	}
	if normalizedPeer != peerAddr {
		t.Fatalf("unexpected normalized libp2p peer addr: got=%q want=%q", normalizedPeer, peerAddr)
	}

	if _, err := normalizeConfiguredPeerURL("/ip4/127.0.0.1/tcp/4001"); err == nil || err.Error() != "libp2p peer addr must include /p2p peer id" {
		t.Fatalf("unexpected peer addr error: %v", err)
	}
	if _, err := normalizeLibP2PListenAddr(peerAddr); err == nil || err.Error() != "libp2p listen addr must not include /p2p" {
		t.Fatalf("unexpected listen addr error: %v", err)
	}
	listenAddr, err := normalizeLibP2PListenAddr("/ip4/0.0.0.0/tcp/4001")
	if err != nil {
		t.Fatalf("normalize libp2p listen addr: %v", err)
	}
	if listenAddr != "/ip4/0.0.0.0/tcp/4001" {
		t.Fatalf("unexpected listen addr: %q", listenAddr)
	}
}

func TestConfigValidateNormalizesPeersAndZeroMQ(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     "/internal/cluster/ws",
		ClusterSecret:     "secret",
		MessageWindowSize: 128,
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
			BindURL: "tcp://[::]:9090",
		},
		Peers: []Peer{
			{URL: "WS://Example.COM/internal/cluster/ws"},
			{URL: "zmq+tcp://Example.COM:9091"},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if cfg.ZeroMQ.BindURL != "tcp://[::]:9090" {
		t.Fatalf("unexpected normalized zeromq bind url: %q", cfg.ZeroMQ.BindURL)
	}
	if cfg.Peers[0].URL != "ws://example.com/internal/cluster/ws" {
		t.Fatalf("unexpected normalized websocket peer: %q", cfg.Peers[0].URL)
	}
	if cfg.Peers[1].URL != "zmq+tcp://example.com:9091" {
		t.Fatalf("unexpected normalized zeromq peer: %q", cfg.Peers[1].URL)
	}
}

func TestConfigValidateRejectsInvalidZeroMQCombinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "duplicate normalized peers",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				Peers: []Peer{
					{URL: "WS://Example.COM/internal/cluster/ws"},
					{URL: "ws://example.com/internal/cluster/ws"},
				},
			},
			wantErr: `duplicate peer url "ws://example.com/internal/cluster/ws"`,
		},
		{
			name: "invalid zeromq peer path",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				Peers: []Peer{
					{URL: "zmq+tcp://127.0.0.1:9091/path"},
				},
			},
			wantErr: "peer url path is not allowed",
		},
		{
			name: "zeromq peer requires zeromq to be enabled",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				Peers: []Peer{
					{URL: "zmq+tcp://127.0.0.1:9091"},
				},
			},
			wantErr: `zeromq peer url "zmq+tcp://127.0.0.1:9091" requires services.zeromq.enabled`,
		},
		{
			name: "invalid zeromq security",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				ZeroMQ: ZeroMQConfig{
					Enabled:  true,
					Security: "tls",
				},
			},
			wantErr: "zeromq security must be none or curve",
		},
		{
			name: "curve peer key only valid for zeromq peer",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				ZeroMQ: ZeroMQConfig{
					Enabled: true,
				},
				Peers: []Peer{
					{
						URL:                        "ws://127.0.0.1:9081/internal/cluster/ws",
						ZeroMQCurveServerPublicKey: testZeroMQCurveKey("P"),
					},
				},
			},
			wantErr: "zeromq curve server public key requires a zmq+tcp peer url",
		},
		{
			name: "curve static peer requires server public key",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				ZeroMQ: ZeroMQConfig{
					Enabled:  true,
					Security: ZeroMQSecurityCurve,
					Curve: ZeroMQCurveConfig{
						ServerPublicKey: testZeroMQCurveKey("S"),
						ServerSecretKey: testZeroMQCurveKey("s"),
						ClientPublicKey: testZeroMQCurveKey("C"),
						ClientSecretKey: testZeroMQCurveKey("c"),
					},
				},
				Peers: []Peer{
					{URL: "zmq+tcp://127.0.0.1:9091"},
				},
			},
			wantErr: "zeromq peer curve server public key cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err == nil || err.Error() != tt.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestConfigValidateNormalizesZeroMQCurveConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		ZeroMQ: ZeroMQConfig{
			Enabled:  true,
			BindURL:  "tcp://127.0.0.1:9090",
			Security: " CURVE ",
			Curve: ZeroMQCurveConfig{
				ServerPublicKey: testZeroMQCurveKey("S"),
				ServerSecretKey: testZeroMQCurveKey("s"),
				ClientPublicKey: testZeroMQCurveKey("C"),
				ClientSecretKey: testZeroMQCurveKey("c"),
				AllowedClientPublicKeys: []string{
					" " + testZeroMQCurveKey("A") + " ",
					testZeroMQCurveKey("A"),
				},
			},
		},
		Peers: []Peer{
			{
				URL:                        "zmq+tcp://Example.COM:9091",
				ZeroMQCurveServerPublicKey: " " + testZeroMQCurveKey("P") + " ",
			},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if cfg.ZeroMQ.Security != ZeroMQSecurityCurve {
		t.Fatalf("unexpected zeromq security: %q", cfg.ZeroMQ.Security)
	}
	if len(cfg.ZeroMQ.Curve.AllowedClientPublicKeys) != 1 || cfg.ZeroMQ.Curve.AllowedClientPublicKeys[0] != testZeroMQCurveKey("A") {
		t.Fatalf("unexpected allowed keys: %+v", cfg.ZeroMQ.Curve.AllowedClientPublicKeys)
	}
	if cfg.Peers[0].URL != "zmq+tcp://example.com:9091" || cfg.Peers[0].ZeroMQCurveServerPublicKey != testZeroMQCurveKey("P") {
		t.Fatalf("unexpected normalized peer: %+v", cfg.Peers[0])
	}
}

func TestConfigValidateLibP2P(t *testing.T) {
	t.Parallel()

	peerAddr := testLibP2PPeerAddr(t, "/ip4/127.0.0.1/tcp/4001")
	cfg := Config{
		NodeID:        testNodeID(1),
		AdvertisePath: "/internal/cluster/ws",
		ClusterSecret: "secret",
		LibP2P: LibP2PConfig{
			Enabled:        true,
			PrivateKeyPath: " ./data/node-a-libp2p.key ",
			ListenAddrs:    []string{" /ip4/0.0.0.0/tcp/4001 "},
			BootstrapPeers: []string{peerAddr},
		},
		Peers: []Peer{{URL: peerAddr}},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if cfg.LibP2P.PrivateKeyPath != "./data/node-a-libp2p.key" {
		t.Fatalf("unexpected private key path: %q", cfg.LibP2P.PrivateKeyPath)
	}
	if len(cfg.LibP2P.ListenAddrs) != 1 || cfg.LibP2P.ListenAddrs[0] != "/ip4/0.0.0.0/tcp/4001" {
		t.Fatalf("unexpected listen addrs: %+v", cfg.LibP2P.ListenAddrs)
	}
	if len(cfg.LibP2P.BootstrapPeers) != 1 || cfg.LibP2P.BootstrapPeers[0] != peerAddr {
		t.Fatalf("unexpected bootstrap peers: %+v", cfg.LibP2P.BootstrapPeers)
	}
	if len(cfg.Peers) != 1 || cfg.Peers[0].URL != peerAddr {
		t.Fatalf("unexpected peer addr: %+v", cfg.Peers)
	}
}

func TestConfigValidateLibP2PDefaultsAndErrors(t *testing.T) {
	t.Parallel()

	peerAddr := testLibP2PPeerAddr(t, "/ip4/127.0.0.1/tcp/4001")
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
		check   func(*testing.T, Config)
	}{
		{
			name: "enabled defaults",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				LibP2P:        LibP2PConfig{Enabled: true},
			}.WithDefaults(),
			check: func(t *testing.T, cfg Config) {
				t.Helper()
				if cfg.LibP2P.PrivateKeyPath != DefaultLibP2PPrivateKeyPath {
					t.Fatalf("unexpected private key path: %q", cfg.LibP2P.PrivateKeyPath)
				}
				if !cfg.LibP2P.EnableDHT || !cfg.LibP2P.EnableHolePunching || !cfg.LibP2P.GossipSubEnabled {
					t.Fatalf("unexpected libp2p defaults: %+v", cfg.LibP2P)
				}
				if len(cfg.LibP2P.ListenAddrs) != 1 || cfg.LibP2P.ListenAddrs[0] != "/ip4/0.0.0.0/tcp/0" {
					t.Fatalf("unexpected default listen addrs: %+v", cfg.LibP2P.ListenAddrs)
				}
			},
		},
		{
			name: "peer requires enabled",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				Peers:         []Peer{{URL: peerAddr}},
			},
			wantErr: `libp2p peer url "` + peerAddr + `" requires services.libp2p.enabled`,
		},
		{
			name: "static peer missing p2p",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				LibP2P:        LibP2PConfig{Enabled: true, PrivateKeyPath: DefaultLibP2PPrivateKeyPath},
				Peers:         []Peer{{URL: "/ip4/127.0.0.1/tcp/4001"}},
			},
			wantErr: "libp2p peer addr must include /p2p peer id",
		},
		{
			name: "listen addr includes p2p",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				LibP2P: LibP2PConfig{
					Enabled:        true,
					PrivateKeyPath: DefaultLibP2PPrivateKeyPath,
					ListenAddrs:    []string{peerAddr},
				},
			},
			wantErr: "libp2p listen addr must not include /p2p",
		},
		{
			name: "disabled service may be preconfigured",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				LibP2P: LibP2PConfig{
					PrivateKeyPath: DefaultLibP2PPrivateKeyPath,
					ListenAddrs:    []string{"/ip4/0.0.0.0/tcp/4001"},
					BootstrapPeers: []string{peerAddr},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate config: %v", err)
			}
			if tt.check != nil {
				tt.check(t, tt.cfg)
			}
		})
	}
}

func testZeroMQCurveKey(prefix string) string {
	return prefix + strings.Repeat("1", 40-len(prefix))
}

func testLibP2PPeerAddr(t *testing.T, transportAddr string) string {
	t.Helper()
	_, pub, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate libp2p key: %v", err)
	}
	id, err := libp2ppeer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("derive libp2p peer id: %v", err)
	}
	return transportAddr + "/p2p/" + id.String()
}

type recordingDialer struct {
	urls chan string
}

func (d *recordingDialer) Dial(ctx context.Context, peerURL string) (TransportConn, error) {
	select {
	case d.urls <- peerURL:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestManagerStartDoesNotUseLegacyDialerForWebSocketStaticPeers(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: 128,
		Peers: []Peer{
			{URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	dialer := &recordingDialer{urls: make(chan string, 4)}
	mgr.dialers[transportWebSocket] = dialer
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer mgr.Close()

	select {
	case rawURL := <-dialer.urls:
		t.Fatalf("legacy websocket dialer should stay unused, got %q", rawURL)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestManagerStartFailsWhenZeroMQSupportIsNotBuilt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "enabled listener",
			cfg: Config{
				NodeID:            testNodeID(1),
				AdvertisePath:     websocketPath,
				ClusterSecret:     "secret",
				MessageWindowSize: 128,
				ZeroMQ: ZeroMQConfig{
					Enabled: true,
					BindURL: "tcp://127.0.0.1:9090",
				},
			},
		},
		{
			name: "outbound only zeromq requires zeromq build",
			cfg: Config{
				NodeID:            testNodeID(1),
				AdvertisePath:     websocketPath,
				ClusterSecret:     "secret",
				MessageWindowSize: 128,
				ZeroMQ: ZeroMQConfig{
					Enabled: true,
				},
				Peers: []Peer{
					{URL: "zmq+tcp://127.0.0.1:9091"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := NewManager(tt.cfg, nil)
			if err != nil {
				t.Fatalf("new manager: %v", err)
			}
			defer mgr.Close()

			if err := mgr.Start(context.Background()); !errors.Is(err, errZeroMQNotBuilt) {
				t.Fatalf("expected errZeroMQNotBuilt, got %v", err)
			}
		})
	}
}

func TestConfigValidateAllowsOutboundOnlyZeroMQ(t *testing.T) {
	t.Parallel()

	cfg := Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     "/internal/cluster/ws",
		ClusterSecret:     "secret",
		MessageWindowSize: 128,
		ZeroMQ: ZeroMQConfig{
			Enabled: true,
		},
		Peers: []Peer{
			{URL: "zmq+tcp://Example.COM:9091"},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if cfg.ZeroMQ.BindURL != "" {
		t.Fatalf("expected empty zeromq bind url for outbound-only mode, got %q", cfg.ZeroMQ.BindURL)
	}
	if cfg.zeroMQMode() != "outbound_only" {
		t.Fatalf("unexpected zeromq mode: %q", cfg.zeroMQMode())
	}
	if cfg.Peers[0].URL != "zmq+tcp://example.com:9091" {
		t.Fatalf("unexpected normalized zeromq peer: %q", cfg.Peers[0].URL)
	}
}
