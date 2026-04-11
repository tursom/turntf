package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	"notifier/internal/clock"
	"notifier/internal/cluster"
	"notifier/internal/store"
)

const defaultConfigPath = "./config.toml"

type serveConfig struct {
	Node    nodeConfig        `toml:"node"`
	API     apiConfig         `toml:"api"`
	Store   storeConfig       `toml:"store"`
	Auth    authConfig        `toml:"auth"`
	Cluster clusterFileConfig `toml:"cluster"`
}

type nodeConfig struct {
	ID   string `toml:"id"`
	Slot int    `toml:"slot"`
}

type apiConfig struct {
	ListenAddr string `toml:"listen_addr"`
}

type storeConfig struct {
	DBPath            string `toml:"db_path"`
	MessageWindowSize int    `toml:"message_window_size"`
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

type clusterFileConfig struct {
	AdvertisePath  string           `toml:"advertise_path"`
	Secret         string           `toml:"secret"`
	MaxClockSkewMs *int64           `toml:"max_clock_skew_ms"`
	Peers          []peerFileConfig `toml:"peers"`
}

type peerFileConfig struct {
	NodeID string `toml:"node_id"`
	URL    string `toml:"url"`
}

type runtimeServeConfig struct {
	ConfigPath   string
	APIAddr      string
	DBPath       string
	StoreOptions store.Options
	Auth         runtimeAuthConfig
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
	if strings.TrimSpace(c.Node.ID) == "" {
		return runtimeServeConfig{}, fmt.Errorf("node.id cannot be empty")
	}
	if c.Node.Slot <= 0 {
		return runtimeServeConfig{}, fmt.Errorf("node.slot must be positive")
	}
	if c.Node.Slot > clock.MaxNodeID {
		return runtimeServeConfig{}, fmt.Errorf("node.slot %d exceeds max %d", c.Node.Slot, clock.MaxNodeID)
	}
	if strings.TrimSpace(c.API.ListenAddr) == "" {
		return runtimeServeConfig{}, fmt.Errorf("api.listen_addr cannot be empty")
	}
	if strings.TrimSpace(c.Store.DBPath) == "" {
		return runtimeServeConfig{}, fmt.Errorf("store.db_path cannot be empty")
	}
	if c.Store.MessageWindowSize < 0 {
		return runtimeServeConfig{}, fmt.Errorf("store.message_window_size must be positive")
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

	messageWindowSize := c.Store.MessageWindowSize
	if messageWindowSize == 0 {
		messageWindowSize = store.DefaultMessageWindowSize
	}
	tokenTTLMinutes := c.Auth.TokenTTLMinutes
	if tokenTTLMinutes == 0 {
		tokenTTLMinutes = 1440
	}

	peers := make([]cluster.Peer, 0, len(c.Cluster.Peers))
	for _, peer := range c.Cluster.Peers {
		peers = append(peers, cluster.Peer{
			NodeID: strings.TrimSpace(peer.NodeID),
			URL:    strings.TrimSpace(peer.URL),
		})
	}

	slot := uint16(c.Node.Slot)
	maxClockSkewMs := cluster.DefaultMaxClockSkewMs
	if c.Cluster.MaxClockSkewMs != nil {
		maxClockSkewMs = *c.Cluster.MaxClockSkewMs
	}
	clusterCfg := cluster.Config{
		NodeID:            strings.TrimSpace(c.Node.ID),
		NodeSlot:          slot,
		AdvertisePath:     strings.TrimSpace(c.Cluster.AdvertisePath),
		ClusterSecret:     strings.TrimSpace(c.Cluster.Secret),
		Peers:             peers,
		MessageWindowSize: messageWindowSize,
		MaxClockSkewMs:    maxClockSkewMs,
	}
	if err := clusterCfg.Validate(); err != nil {
		return runtimeServeConfig{}, fmt.Errorf("invalid cluster config: %w", err)
	}
	if clusterCfg.Enabled() && strings.TrimSpace(c.Auth.TokenSecret) == clusterCfg.ClusterSecret {
		return runtimeServeConfig{}, fmt.Errorf("auth.token_secret must differ from cluster.secret")
	}

	return runtimeServeConfig{
		ConfigPath: configPath,
		APIAddr:    strings.TrimSpace(c.API.ListenAddr),
		DBPath:     filepath.Clean(strings.TrimSpace(c.Store.DBPath)),
		StoreOptions: store.Options{
			NodeID:            strings.TrimSpace(c.Node.ID),
			NodeSlot:          slot,
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
		Cluster: clusterCfg,
	}, nil
}
