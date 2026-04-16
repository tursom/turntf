package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/store"
)

const defaultConfigPath = "./config.toml"
const defaultSQLitePath = "./data/turntf.db"
const defaultPebblePath = "./data/turntf.pebble"

type serveConfig struct {
	Services servicesConfig    `toml:"services"`
	Store    storeConfig       `toml:"store"`
	Auth     authConfig        `toml:"auth"`
	Logging  loggingConfig     `toml:"logging"`
	Cluster  clusterFileConfig `toml:"cluster"`
}

type servicesConfig struct {
	HTTP   httpServiceConfig `toml:"http"`
	ZeroMQ zeroMQFileConfig  `toml:"zeromq"`
	LibP2P libP2PFileConfig  `toml:"libp2p"`
}

type httpServiceConfig struct {
	ListenAddr string `toml:"listen_addr"`
}

type storeConfig struct {
	MessageWindowSize int               `toml:"message_window_size"`
	Engine            string            `toml:"engine"`
	SQLite            sqliteStoreConfig `toml:"sqlite"`
	Pebble            pebbleStoreConfig `toml:"pebble"`
}

type sqliteStoreConfig struct {
	DBPath string `toml:"db_path"`
}

type pebbleStoreConfig struct {
	Path string `toml:"path"`
}

type authConfig struct {
	TokenSecret     string               `toml:"token_secret"`
	TokenTTLMinutes int                  `toml:"token_ttl_minutes"`
	BootstrapAdmin  bootstrapAdminConfig `toml:"bootstrap_admin"`
}

type bootstrapAdminConfig struct {
	Username     string `toml:"username"`
	PasswordHash string `toml:"password_hash"`
}

type loggingConfig struct {
	Level    string `toml:"level"`
	FilePath string `toml:"file_path"`
}

type clusterFileConfig struct {
	Secret     string                      `toml:"secret"`
	Forwarding clusterForwardingFileConfig `toml:"forwarding"`
	Clock      clockFileConfig             `toml:"clock"`
	Peers      []peerFileConfig            `toml:"peers"`
}

type clusterForwardingFileConfig struct {
	Enabled       *bool                              `toml:"enabled"`
	BridgeEnabled *bool                              `toml:"bridge_enabled"`
	NodeFeeWeight *int64                             `toml:"node_fee_weight"`
	Traffic       clusterForwardingTrafficFileConfig `toml:"traffic"`
}

type clusterForwardingTrafficFileConfig struct {
	ControlCritical      string `toml:"control_critical"`
	ControlQuery         string `toml:"control_query"`
	TransientInteractive string `toml:"transient_interactive"`
	ReplicationStream    string `toml:"replication_stream"`
	SnapshotBulk         string `toml:"snapshot_bulk"`
}

type clockFileConfig struct {
	MaxSkewMs                  *int64 `toml:"max_skew_ms"`
	SyncTimeoutMs              *int64 `toml:"sync_timeout_ms"`
	CredibleRttMs              *int64 `toml:"credible_rtt_ms"`
	TrustedFreshMs             *int64 `toml:"trusted_fresh_ms"`
	ObserveGraceMs             *int64 `toml:"observe_grace_ms"`
	WriteGateGraceMs           *int64 `toml:"write_gate_grace_ms"`
	RejectAfterFailures        *int   `toml:"reject_after_failures"`
	RejectAfterSkewSamples     *int   `toml:"reject_after_skew_samples"`
	RecoverAfterHealthySamples *int   `toml:"recover_after_healthy_samples"`
}

type peerFileConfig struct {
	URL    string               `toml:"url"`
	ZeroMQ peerZeroMQFileConfig `toml:"zeromq"`
}

type peerZeroMQFileConfig struct {
	CurveServerPublicKey string `toml:"curve_server_public_key"`
}

type zeroMQFileConfig struct {
	Enabled           bool                  `toml:"enabled"`
	BindURL           string                `toml:"bind_url"`
	Security          string                `toml:"security"`
	ForwardingEnabled *bool                 `toml:"forwarding_enabled"`
	Curve             zeroMQCurveFileConfig `toml:"curve"`
}

type zeroMQCurveFileConfig struct {
	ServerPublicKey         string   `toml:"server_public_key"`
	ServerSecretKey         string   `toml:"server_secret_key"`
	ClientPublicKey         string   `toml:"client_public_key"`
	ClientSecretKey         string   `toml:"client_secret_key"`
	AllowedClientPublicKeys []string `toml:"allowed_client_public_keys"`
}

type libP2PFileConfig struct {
	Enabled                   bool     `toml:"enabled"`
	PrivateKeyPath            string   `toml:"private_key_path"`
	ListenAddrs               []string `toml:"listen_addrs"`
	BootstrapPeers            []string `toml:"bootstrap_peers"`
	EnableDHT                 *bool    `toml:"enable_dht"`
	EnableMDNS                bool     `toml:"enable_mdns"`
	RelayPeers                []string `toml:"relay_peers"`
	EnableHolePunching        *bool    `toml:"enable_hole_punching"`
	GossipSubEnabled          *bool    `toml:"gossipsub_enabled"`
	NativeRelayClientEnabled  bool     `toml:"native_relay_client_enabled"`
	NativeRelayServiceEnabled bool     `toml:"native_relay_service_enabled"`
}

type runtimeServeConfig struct {
	ConfigPath   string
	Services     runtimeServicesConfig
	SQLitePath   string
	PebblePath   string
	StoreOptions store.Options
	Auth         runtimeAuthConfig
	Logging      runtimeLoggingConfig
	Cluster      cluster.Config
}

type runtimeServicesConfig struct {
	HTTP   runtimeHTTPServiceConfig
	ZeroMQ cluster.ZeroMQConfig
	LibP2P cluster.LibP2PConfig
}

type runtimeHTTPServiceConfig struct {
	ListenAddr string
}

type runtimeAuthConfig struct {
	TokenSecret     string
	TokenTTLMinutes int
	BootstrapAdmin  store.BootstrapAdminConfig
}

func loadServeRuntimeConfig(path string) (runtimeServeConfig, error) {
	configPath := resolveConfigPath(path)

	var cfg serveConfig
	meta, err := toml.DecodeFile(configPath, &cfg)
	if err != nil {
		return runtimeServeConfig{}, fmt.Errorf("read config %s: %w", configPath, err)
	}

	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		fields := make([]string, 0, len(undecoded))
		for _, item := range undecoded {
			fields = append(fields, item.String())
		}
		return runtimeServeConfig{}, fmt.Errorf("read config %s: unknown fields %s", configPath, strings.Join(fields, ", "))
	}

	return cfg.runtimeConfig(configPath)
}

func resolveConfigPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return defaultConfigPath
	}
	return trimmed
}

func trimStringSlice(values []string) []string {
	trimmed := make([]string, 0, len(values))
	for _, value := range values {
		item := strings.TrimSpace(value)
		if item != "" {
			trimmed = append(trimmed, item)
		}
	}
	return trimmed
}

func (c serveConfig) runtimeConfig(configPath string) (runtimeServeConfig, error) {
	httpListenAddr := strings.TrimSpace(c.Services.HTTP.ListenAddr)
	if httpListenAddr == "" {
		return runtimeServeConfig{}, fmt.Errorf("services.http.listen_addr cannot be empty")
	}
	if c.Store.MessageWindowSize < 0 {
		return runtimeServeConfig{}, fmt.Errorf("store.message_window_size must be positive")
	}
	engine := strings.ToLower(strings.TrimSpace(c.Store.Engine))
	if engine == "" {
		engine = store.EngineSQLite
	}
	if engine != store.EngineSQLite && engine != store.EnginePebble {
		return runtimeServeConfig{}, fmt.Errorf("store.engine must be sqlite or pebble")
	}
	if strings.TrimSpace(c.Auth.TokenSecret) == "" {
		return runtimeServeConfig{}, fmt.Errorf("auth.token_secret cannot be empty")
	}
	if strings.TrimSpace(c.Auth.BootstrapAdmin.Username) == "" {
		return runtimeServeConfig{}, fmt.Errorf("auth.bootstrap_admin.username cannot be empty")
	}
	if strings.TrimSpace(c.Auth.BootstrapAdmin.PasswordHash) == "" {
		return runtimeServeConfig{}, fmt.Errorf("auth.bootstrap_admin.password_hash cannot be empty")
	}
	if c.Auth.TokenTTLMinutes < 0 {
		return runtimeServeConfig{}, fmt.Errorf("auth.token_ttl_minutes must be non-negative")
	}
	loggingCfg, err := c.Logging.runtimeConfig()
	if err != nil {
		return runtimeServeConfig{}, err
	}

	messageWindowSize := c.Store.MessageWindowSize
	if messageWindowSize == 0 {
		messageWindowSize = store.DefaultMessageWindowSize
	}
	sqlitePath := strings.TrimSpace(c.Store.SQLite.DBPath)
	if sqlitePath == "" {
		sqlitePath = defaultSQLitePath
	}
	pebblePath := strings.TrimSpace(c.Store.Pebble.Path)
	if pebblePath == "" {
		pebblePath = defaultPebblePath
	}
	tokenTTLMinutes := c.Auth.TokenTTLMinutes
	if tokenTTLMinutes == 0 {
		tokenTTLMinutes = 1440
	}
	forwardingCfg, err := c.Cluster.Forwarding.runtimeConfig()
	if err != nil {
		return runtimeServeConfig{}, err
	}

	peers := make([]cluster.Peer, 0, len(c.Cluster.Peers))
	for _, peer := range c.Cluster.Peers {
		peers = append(peers, cluster.Peer{
			URL:                        strings.TrimSpace(peer.URL),
			ZeroMQCurveServerPublicKey: strings.TrimSpace(peer.ZeroMQ.CurveServerPublicKey),
		})
	}

	maxClockSkewMs := cluster.DefaultMaxClockSkewMs
	if c.Cluster.Clock.MaxSkewMs != nil {
		maxClockSkewMs = *c.Cluster.Clock.MaxSkewMs
	}
	clockSyncTimeoutMs := cluster.DefaultClockSyncTimeoutMs
	if c.Cluster.Clock.SyncTimeoutMs != nil {
		clockSyncTimeoutMs = *c.Cluster.Clock.SyncTimeoutMs
	}
	clockCredibleRttMs := cluster.DefaultClockCredibleRTTMs
	if c.Cluster.Clock.CredibleRttMs != nil {
		clockCredibleRttMs = *c.Cluster.Clock.CredibleRttMs
	}
	clockTrustedFreshMs := cluster.DefaultClockTrustedFreshMs
	if c.Cluster.Clock.TrustedFreshMs != nil {
		clockTrustedFreshMs = *c.Cluster.Clock.TrustedFreshMs
	}
	clockObserveGraceMs := cluster.DefaultClockObserveGraceMs
	if c.Cluster.Clock.ObserveGraceMs != nil {
		clockObserveGraceMs = *c.Cluster.Clock.ObserveGraceMs
	}
	clockWriteGateGraceMs := cluster.DefaultClockWriteGateGraceMs
	if c.Cluster.Clock.WriteGateGraceMs != nil {
		clockWriteGateGraceMs = *c.Cluster.Clock.WriteGateGraceMs
	}
	clockRejectAfterFailures := cluster.DefaultClockRejectAfterFailures
	if c.Cluster.Clock.RejectAfterFailures != nil {
		clockRejectAfterFailures = *c.Cluster.Clock.RejectAfterFailures
	}
	clockRejectAfterSkewSamples := cluster.DefaultClockRejectAfterSkewSamples
	if c.Cluster.Clock.RejectAfterSkewSamples != nil {
		clockRejectAfterSkewSamples = *c.Cluster.Clock.RejectAfterSkewSamples
	}
	clockRecoverAfterHealthySamples := cluster.DefaultClockRecoverAfterHealthySamples
	if c.Cluster.Clock.RecoverAfterHealthySamples != nil {
		clockRecoverAfterHealthySamples = *c.Cluster.Clock.RecoverAfterHealthySamples
	}
	zeroMQCfg := cluster.ZeroMQConfig{
		Enabled:           c.Services.ZeroMQ.Enabled,
		BindURL:           strings.TrimSpace(c.Services.ZeroMQ.BindURL),
		Security:          strings.TrimSpace(c.Services.ZeroMQ.Security),
		ForwardingEnabled: c.Services.ZeroMQ.ForwardingEnabled,
		Curve: cluster.ZeroMQCurveConfig{
			ServerPublicKey:         strings.TrimSpace(c.Services.ZeroMQ.Curve.ServerPublicKey),
			ServerSecretKey:         strings.TrimSpace(c.Services.ZeroMQ.Curve.ServerSecretKey),
			ClientPublicKey:         strings.TrimSpace(c.Services.ZeroMQ.Curve.ClientPublicKey),
			ClientSecretKey:         strings.TrimSpace(c.Services.ZeroMQ.Curve.ClientSecretKey),
			AllowedClientPublicKeys: trimStringSlice(c.Services.ZeroMQ.Curve.AllowedClientPublicKeys),
		},
	}
	libP2PCfg := c.Services.LibP2P.runtimeConfig()
	clusterCfg := cluster.Config{
		AdvertisePath:                   cluster.WebSocketPath,
		ClusterSecret:                   strings.TrimSpace(c.Cluster.Secret),
		Forwarding:                      forwardingCfg,
		ZeroMQ:                          zeroMQCfg,
		LibP2P:                          libP2PCfg,
		Peers:                           peers,
		MessageWindowSize:               messageWindowSize,
		MaxClockSkewMs:                  maxClockSkewMs,
		ClockSyncTimeoutMs:              clockSyncTimeoutMs,
		ClockCredibleRttMs:              clockCredibleRttMs,
		ClockTrustedFreshMs:             clockTrustedFreshMs,
		ClockObserveGraceMs:             clockObserveGraceMs,
		ClockWriteGateGraceMs:           clockWriteGateGraceMs,
		ClockRejectAfterFailures:        clockRejectAfterFailures,
		ClockRejectAfterSkewSamples:     clockRejectAfterSkewSamples,
		ClockRecoverAfterHealthySamples: clockRecoverAfterHealthySamples,
	}
	clusterCfg = clusterCfg.WithDefaults()
	if err := validateClusterFileConfig(&clusterCfg); err != nil {
		return runtimeServeConfig{}, fmt.Errorf("invalid cluster config: %w", err)
	}
	if clusterCfg.Enabled() && strings.TrimSpace(c.Auth.TokenSecret) == clusterCfg.ClusterSecret {
		return runtimeServeConfig{}, fmt.Errorf("auth.token_secret must differ from cluster.secret")
	}

	return runtimeServeConfig{
		ConfigPath: configPath,
		Services: runtimeServicesConfig{
			HTTP: runtimeHTTPServiceConfig{
				ListenAddr: httpListenAddr,
			},
			ZeroMQ: clusterCfg.ZeroMQ,
			LibP2P: clusterCfg.LibP2P,
		},
		SQLitePath: filepath.Clean(sqlitePath),
		PebblePath: filepath.Clean(pebblePath),
		StoreOptions: store.Options{
			Engine:            engine,
			PebblePath:        filepath.Clean(pebblePath),
			MessageWindowSize: messageWindowSize,
		},
		Auth: runtimeAuthConfig{
			TokenSecret:     strings.TrimSpace(c.Auth.TokenSecret),
			TokenTTLMinutes: tokenTTLMinutes,
			BootstrapAdmin: store.BootstrapAdminConfig{
				Username:     strings.TrimSpace(c.Auth.BootstrapAdmin.Username),
				PasswordHash: strings.TrimSpace(c.Auth.BootstrapAdmin.PasswordHash),
			},
		},
		Logging: loggingCfg,
		Cluster: clusterCfg,
	}, nil
}

func (c libP2PFileConfig) runtimeConfig() cluster.LibP2PConfig {
	enableDHT := true
	if c.EnableDHT != nil {
		enableDHT = *c.EnableDHT
	}
	enableHolePunching := true
	if c.EnableHolePunching != nil {
		enableHolePunching = *c.EnableHolePunching
	}
	gossipSubEnabled := true
	if c.GossipSubEnabled != nil {
		gossipSubEnabled = *c.GossipSubEnabled
	}
	return cluster.LibP2PConfig{
		Enabled:                   c.Enabled,
		PrivateKeyPath:            strings.TrimSpace(c.PrivateKeyPath),
		ListenAddrs:               trimStringSlice(c.ListenAddrs),
		BootstrapPeers:            trimStringSlice(c.BootstrapPeers),
		EnableDHT:                 enableDHT,
		EnableMDNS:                c.EnableMDNS,
		RelayPeers:                trimStringSlice(c.RelayPeers),
		EnableHolePunching:        enableHolePunching,
		GossipSubEnabled:          gossipSubEnabled,
		NativeRelayClientEnabled:  c.NativeRelayClientEnabled,
		NativeRelayServiceEnabled: c.NativeRelayServiceEnabled,
	}
}

func (c clusterForwardingFileConfig) runtimeConfig() (cluster.ForwardingConfig, error) {
	traffic, err := c.Traffic.runtimeConfig()
	if err != nil {
		return cluster.ForwardingConfig{}, err
	}
	nodeFeeWeight := int64(0)
	if c.NodeFeeWeight != nil {
		nodeFeeWeight = *c.NodeFeeWeight
	}
	return cluster.ForwardingConfig{
		Enabled:       c.Enabled,
		BridgeEnabled: c.BridgeEnabled,
		NodeFeeWeight: nodeFeeWeight,
		Traffic:       traffic,
	}, nil
}

func (c clusterForwardingTrafficFileConfig) runtimeConfig() (cluster.ForwardingTrafficConfig, error) {
	controlCritical, err := cluster.ParseForwardingDisposition(c.ControlCritical)
	if err != nil {
		return cluster.ForwardingTrafficConfig{}, fmt.Errorf("cluster.forwarding.traffic.control_critical: %w", err)
	}
	controlQuery, err := cluster.ParseForwardingDisposition(c.ControlQuery)
	if err != nil {
		return cluster.ForwardingTrafficConfig{}, fmt.Errorf("cluster.forwarding.traffic.control_query: %w", err)
	}
	transientInteractive, err := cluster.ParseForwardingDisposition(c.TransientInteractive)
	if err != nil {
		return cluster.ForwardingTrafficConfig{}, fmt.Errorf("cluster.forwarding.traffic.transient_interactive: %w", err)
	}
	replicationStream, err := cluster.ParseForwardingDisposition(c.ReplicationStream)
	if err != nil {
		return cluster.ForwardingTrafficConfig{}, fmt.Errorf("cluster.forwarding.traffic.replication_stream: %w", err)
	}
	snapshotBulk, err := cluster.ParseForwardingDisposition(c.SnapshotBulk)
	if err != nil {
		return cluster.ForwardingTrafficConfig{}, fmt.Errorf("cluster.forwarding.traffic.snapshot_bulk: %w", err)
	}
	return cluster.ForwardingTrafficConfig{
		ControlCritical:      controlCritical,
		ControlQuery:         controlQuery,
		TransientInteractive: transientInteractive,
		ReplicationStream:    replicationStream,
		SnapshotBulk:         snapshotBulk,
	}, nil
}

func validateClusterFileConfig(c *cluster.Config) error {
	if c == nil {
		return fmt.Errorf("cluster config cannot be nil")
	}
	validating := *c
	if validating.NodeID == 0 {
		validating.NodeID = 1
	}
	if err := validating.Validate(); err != nil {
		return err
	}
	*c = validating
	return nil
}
