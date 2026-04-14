package cluster

import (
	"context"
	"testing"
	"time"
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
			name: "enabled without bind url",
			cfg: Config{
				NodeID:        testNodeID(1),
				AdvertisePath: "/internal/cluster/ws",
				ClusterSecret: "secret",
				ZeroMQ: ZeroMQConfig{
					Enabled: true,
				},
			},
			wantErr: "zeromq bind url cannot be empty",
		},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err == nil || err.Error() != tt.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
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

func TestManagerStartSkipsZeroMQStaticPeers(t *testing.T) {
	t.Parallel()

	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: 128,
		Peers: []Peer{
			{URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
			{URL: "zmq+tcp://127.0.0.1:9091"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	dialer := &recordingDialer{urls: make(chan string, 4)}
	mgr.dialer = dialer
	mgr.Start(context.Background())
	defer mgr.Close()

	select {
	case rawURL := <-dialer.urls:
		if rawURL != "ws://127.0.0.1:9081/internal/cluster/ws" {
			t.Fatalf("unexpected dial url: %q", rawURL)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected websocket static peer to start dialing")
	}

	select {
	case rawURL := <-dialer.urls:
		t.Fatalf("expected zeromq peer to be skipped, got dial for %q", rawURL)
	case <-time.After(200 * time.Millisecond):
	}
}
