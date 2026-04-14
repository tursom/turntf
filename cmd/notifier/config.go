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
	API     apiConfig         `toml:"api"`
	Store   storeConfig       `toml:"store"`
	Auth    authConfig        `toml:"auth"`
	Logging loggingConfig     `toml:"logging"`
	Cluster clusterFileConfig `toml:"cluster"`
}

type apiConfig struct {
	ListenAddr string `toml:"listen_addr"`
}

type storeConfig struct {
	DBPath            string            `toml:"db_path"`
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
	AdvertisePath                   string           `toml:"advertise_path"`
	Secret                          string           `toml:"secret"`
	MaxClockSkewMs                  *int64           `toml:"max_clock_skew_ms"`
	ClockSyncTimeoutMs              *int64           `toml:"clock_sync_timeout_ms"`
	ClockCredibleRttMs              *int64           `toml:"clock_credible_rtt_ms"`
	ClockTrustedFreshMs             *int64           `toml:"clock_trusted_fresh_ms"`
	ClockObserveGraceMs             *int64           `toml:"clock_observe_grace_ms"`
	ClockWriteGateGraceMs           *int64           `toml:"clock_write_gate_grace_ms"`
	ClockRejectAfterFailures        *int             `toml:"clock_reject_after_failures"`
	ClockRejectAfterSkewSamples     *int             `toml:"clock_reject_after_skew_samples"`
	ClockRecoverAfterHealthySamples *int             `toml:"clock_recover_after_healthy_samples"`
	Peers                           []peerFileConfig `toml:"peers"`
}

type peerFileConfig struct {
	URL string `toml:"url"`
}

type runtimeServeConfig struct {
	ConfigPath   string
	APIAddr      string
	SQLitePath   string
	PebblePath   string
	StoreOptions store.Options
	Auth         runtimeAuthConfig
	Logging      runtimeLoggingConfig
	Cluster      cluster.Config
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

func (c serveConfig) runtimeConfig(configPath string) (runtimeServeConfig, error) {
	if strings.TrimSpace(c.API.ListenAddr) == "" {
		return runtimeServeConfig{}, fmt.Errorf("api.listen_addr cannot be empty")
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
		sqlitePath = strings.TrimSpace(c.Store.DBPath)
	}
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

	peers := make([]cluster.Peer, 0, len(c.Cluster.Peers))
	for _, peer := range c.Cluster.Peers {
		peers = append(peers, cluster.Peer{
			URL: strings.TrimSpace(peer.URL),
		})
	}

	maxClockSkewMs := cluster.DefaultMaxClockSkewMs
	if c.Cluster.MaxClockSkewMs != nil {
		maxClockSkewMs = *c.Cluster.MaxClockSkewMs
	}
	clockSyncTimeoutMs := cluster.DefaultClockSyncTimeoutMs
	if c.Cluster.ClockSyncTimeoutMs != nil {
		clockSyncTimeoutMs = *c.Cluster.ClockSyncTimeoutMs
	}
	clockCredibleRttMs := cluster.DefaultClockCredibleRTTMs
	if c.Cluster.ClockCredibleRttMs != nil {
		clockCredibleRttMs = *c.Cluster.ClockCredibleRttMs
	}
	clockTrustedFreshMs := cluster.DefaultClockTrustedFreshMs
	if c.Cluster.ClockTrustedFreshMs != nil {
		clockTrustedFreshMs = *c.Cluster.ClockTrustedFreshMs
	}
	clockObserveGraceMs := cluster.DefaultClockObserveGraceMs
	if c.Cluster.ClockObserveGraceMs != nil {
		clockObserveGraceMs = *c.Cluster.ClockObserveGraceMs
	}
	clockWriteGateGraceMs := cluster.DefaultClockWriteGateGraceMs
	if c.Cluster.ClockWriteGateGraceMs != nil {
		clockWriteGateGraceMs = *c.Cluster.ClockWriteGateGraceMs
	}
	clockRejectAfterFailures := cluster.DefaultClockRejectAfterFailures
	if c.Cluster.ClockRejectAfterFailures != nil {
		clockRejectAfterFailures = *c.Cluster.ClockRejectAfterFailures
	}
	clockRejectAfterSkewSamples := cluster.DefaultClockRejectAfterSkewSamples
	if c.Cluster.ClockRejectAfterSkewSamples != nil {
		clockRejectAfterSkewSamples = *c.Cluster.ClockRejectAfterSkewSamples
	}
	clockRecoverAfterHealthySamples := cluster.DefaultClockRecoverAfterHealthySamples
	if c.Cluster.ClockRecoverAfterHealthySamples != nil {
		clockRecoverAfterHealthySamples = *c.Cluster.ClockRecoverAfterHealthySamples
	}
	clusterCfg := cluster.Config{
		AdvertisePath:                   strings.TrimSpace(c.Cluster.AdvertisePath),
		ClusterSecret:                   strings.TrimSpace(c.Cluster.Secret),
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
	if err := validateClusterFileConfig(clusterCfg); err != nil {
		return runtimeServeConfig{}, fmt.Errorf("invalid cluster config: %w", err)
	}
	if clusterCfg.Enabled() && strings.TrimSpace(c.Auth.TokenSecret) == clusterCfg.ClusterSecret {
		return runtimeServeConfig{}, fmt.Errorf("auth.token_secret must differ from cluster.secret")
	}

	return runtimeServeConfig{
		ConfigPath: configPath,
		APIAddr:    strings.TrimSpace(c.API.ListenAddr),
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

func validateClusterFileConfig(c cluster.Config) error {
	if c.MaxClockSkewMs < 0 {
		return fmt.Errorf("cluster max clock skew must be non-negative")
	}
	if c.ClockSyncTimeoutMs < 0 {
		return fmt.Errorf("cluster clock sync timeout must be non-negative")
	}
	if c.ClockCredibleRttMs < 0 {
		return fmt.Errorf("cluster clock credible rtt must be non-negative")
	}
	if c.ClockTrustedFreshMs < 0 {
		return fmt.Errorf("cluster clock trusted fresh window must be non-negative")
	}
	if c.ClockObserveGraceMs < 0 {
		return fmt.Errorf("cluster clock observe grace must be non-negative")
	}
	if c.ClockWriteGateGraceMs < 0 {
		return fmt.Errorf("cluster clock write gate grace must be non-negative")
	}
	if c.ClockRejectAfterFailures < 0 {
		return fmt.Errorf("cluster clock reject-after-failures must be non-negative")
	}
	if c.ClockRejectAfterSkewSamples < 0 {
		return fmt.Errorf("cluster clock reject-after-skew-samples must be non-negative")
	}
	if c.ClockRecoverAfterHealthySamples < 0 {
		return fmt.Errorf("cluster clock recover-after-healthy-samples must be non-negative")
	}
	if c.Enabled() {
		if strings.TrimSpace(c.ClusterSecret) == "" {
			return fmt.Errorf("cluster secret cannot be empty")
		}
		if strings.TrimSpace(c.AdvertisePath) == "" {
			return fmt.Errorf("cluster advertise path cannot be empty when cluster mode is enabled")
		}
		if !strings.HasPrefix(strings.TrimSpace(c.AdvertisePath), "/") {
			return fmt.Errorf("cluster advertise path must start with /")
		}
	}

	seenPeers := make(map[string]struct{}, len(c.Peers))
	for _, peer := range c.Peers {
		url := strings.TrimSpace(peer.URL)
		if url == "" {
			return fmt.Errorf("peer url cannot be empty")
		}
		if _, ok := seenPeers[url]; ok {
			return fmt.Errorf("duplicate peer url %q", url)
		}
		seenPeers[url] = struct{}{}
	}
	return nil
}
