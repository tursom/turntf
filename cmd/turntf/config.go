package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/store"
)

// 默认配置文件路径
const defaultConfigPath = "./config.toml"

// 默认 SQLite 数据库路径
const defaultSQLitePath = "./data/turntf.db"

// 默认 Pebble 数据库路径
const defaultPebblePath = "./data/turntf.pebble"

// 默认事件日志裁剪间隔（秒）
const defaultEventLogPruneIntervalSeconds = 60

// serveConfig 是 TOML 配置文件的顶级映射结构体。
// 对应 config.toml 中的 [services]、[store]、[auth]、[logging]、[cluster] 各节。
type serveConfig struct {
	// Services HTTP、ZeroMQ、LibP2P 等服务配置
	Services servicesConfig `toml:"services"`
	// Store 存储引擎配置（SQLite / Pebble）
	Store storeConfig `toml:"store"`
	// Auth 认证相关配置（Token 密钥、管理员账号）
	Auth authConfig `toml:"auth"`
	// Logging 日志级别和文件输出配置
	Logging loggingConfig `toml:"logging"`
	// Cluster 集群 Mesh 网络配置
	Cluster clusterFileConfig `toml:"cluster"`
}

// servicesConfig 包含所有传输层服务的配置。
type servicesConfig struct {
	// HTTP HTTP API 服务配置
	HTTP httpServiceConfig `toml:"http"`
	// ZeroMQ ZeroMQ 协议监听器配置
	ZeroMQ zeroMQFileConfig `toml:"zeromq"`
	// LibP2P LibP2P 协议配置
	LibP2P libP2PFileConfig `toml:"libp2p"`
}

// httpServiceConfig HTTP API 服务配置。
type httpServiceConfig struct {
	// ListenAddr HTTP 监听地址，如 "0.0.0.0:8080"
	ListenAddr string `toml:"listen_addr"`
}

// storeConfig 存储引擎配置。
type storeConfig struct {
	// MessageWindowSize 消息窗口大小，用于控制消息排序缓冲区容量，默认使用 store.DefaultMessageWindowSize
	MessageWindowSize int `toml:"message_window_size"`
	// Engine 存储引擎类型，可选 "sqlite"（默认）或 "pebble"
	Engine string `toml:"engine"`
	// EventLog 事件日志裁剪配置
	EventLog eventLogStoreConfig `toml:"event_log"`
	// SQLite SQLite 存储引擎路径配置
	SQLite sqliteStoreConfig `toml:"sqlite"`
	// Pebble Pebble 存储引擎路径和调优配置
	Pebble pebbleStoreConfig `toml:"pebble"`
}

// eventLogStoreConfig 事件日志裁剪配置。
// 事件日志用于记录消息投递状态和系统事件，定期裁剪防止无限增长。
type eventLogStoreConfig struct {
	// Enabled 是否启用自动裁剪，默认为 true
	Enabled *bool `toml:"enabled"`
	// MaxEventsPerOrigin 每个来源保留的最大事件数，默认为 store.DefaultEventLogMaxEventsPerOrigin
	MaxEventsPerOrigin int `toml:"max_events_per_origin"`
	// PruneIntervalSeconds 裁剪间隔（秒），默认为 60
	PruneIntervalSeconds int `toml:"prune_interval_seconds"`
}

// sqliteStoreConfig SQLite 存储引擎路径配置。
type sqliteStoreConfig struct {
	// DBPath SQLite 数据库文件路径，默认为 "./data/turntf.db"
	DBPath string `toml:"db_path"`
}

// pebbleStoreConfig Pebble 存储引擎路径与调优配置。
type pebbleStoreConfig struct {
	// Path Pebble 数据库目录路径，默认为 "./data/turntf.pebble"
	Path string `toml:"path"`
	// Profile Pebble 性能配置（test/dev/balanced/production），默认 balanced
	Profile string `toml:"profile"`
	// MessageSyncMode 消息同步模式，控制 fsync 行为
	MessageSyncMode string `toml:"message_sync_mode"`
}

// authConfig 认证相关配置。
type authConfig struct {
	// TokenSecret JWT Token 签名密钥，不能为空，且必须与 cluster.secret 不同
	TokenSecret string `toml:"token_secret"`
	// TokenTTLMinutes Token 有效期（分钟），默认 1440（24 小时）
	TokenTTLMinutes int `toml:"token_ttl_minutes"`
	// ReconnectTokenTTLMinutes 客户端重连凭据有效期（分钟），默认 5
	ReconnectTokenTTLMinutes int `toml:"reconnect_token_ttl_minutes"`
	// BootstrapAdmin 初始管理员账号配置
	BootstrapAdmin bootstrapAdminConfig `toml:"bootstrap_admin"`
}

// bootstrapAdminConfig 引导管理员账号配置。
// 第一次启动时自动创建该管理员用户。
type bootstrapAdminConfig struct {
	// Username 用户名
	Username string `toml:"username"`
	// PasswordHash 密码的 bcrypt 哈希值
	PasswordHash string `toml:"password_hash"`
	// LoginName 登录名，可选，默认与 Username 相同
	LoginName string `toml:"login_name"`
}

// loggingConfig 日志记录配置（TOML 文件映射）。
type loggingConfig struct {
	// Level 日志级别，可选 debug / info / warn / error，默认 info
	Level string `toml:"level"`
	// FilePath 日志文件输出路径，为空时仅输出到控制台
	FilePath string `toml:"file_path"`
}

// clusterFileConfig 集群 Mesh 网络配置（TOML 文件映射）。
type clusterFileConfig struct {
	// Secret 集群共享密钥，用于节点间认证
	Secret string `toml:"secret"`
	// DisconnectSuspicionGraceMs 节点断开后的怀疑期（毫秒），超时后标记为离线
	DisconnectSuspicionGraceMs *int64 `toml:"disconnect_suspicion_grace_ms"`
	// Forwarding 消息转发策略配置
	Forwarding clusterForwardingFileConfig `toml:"forwarding"`
	// Clock 时钟同步参数，用于分布式时钟一致性
	Clock clockFileConfig `toml:"clock"`
	// Peers 对等节点列表
	Peers []peerFileConfig `toml:"peers"`
}

// clusterForwardingFileConfig 消息转发策略配置。
type clusterForwardingFileConfig struct {
	// Enabled 是否启用消息转发，默认 false
	Enabled *bool `toml:"enabled"`
	// BridgeEnabled 是否启用网桥模式，允许跨集群转发
	BridgeEnabled *bool `toml:"bridge_enabled"`
	// NodeFeeWeight 节点转发费用权重，用于路由选择
	NodeFeeWeight *int64 `toml:"node_fee_weight"`
	// Traffic 各类流量的转发策略
	Traffic clusterForwardingTrafficFileConfig `toml:"traffic"`
}

// clusterForwardingTrafficFileConfig 不同流量类型的转发策略配置。
// 每种流量类型可配置为 "default"（默认）、"forward"（转发）、"local_only"（仅本地）等。
type clusterForwardingTrafficFileConfig struct {
	// ControlCritical 关键控制消息流量
	ControlCritical string `toml:"control_critical"`
	// ControlQuery 查询类控制消息流量
	ControlQuery string `toml:"control_query"`
	// TransientInteractive 瞬时交互流量（如实时消息）
	TransientInteractive string `toml:"transient_interactive"`
	// ReplicationStream 数据复制流
	ReplicationStream string `toml:"replication_stream"`
	// SnapshotBulk 快照批量传输流量
	SnapshotBulk string `toml:"snapshot_bulk"`
}

// clockFileConfig 集群时钟同步参数（TOML 文件映射）。
// 所有字段均为可选的指针类型，未设置时使用 cluster 包中的默认值。
type clockFileConfig struct {
	// MaxSkewMs 允许的最大时钟偏差（毫秒），超过此值认为时钟不同步
	MaxSkewMs *int64 `toml:"max_skew_ms"`
	// SyncTimeoutMs 时钟同步超时时间（毫秒）
	SyncTimeoutMs *int64 `toml:"sync_timeout_ms"`
	// CredibleRttMs 可信往返时间（毫秒），用于 NTP 风格的时间同步
	CredibleRttMs *int64 `toml:"credible_rtt_ms"`
	// TrustedFreshMs 可信新鲜度阈值（毫秒），在此时间内的时间戳被认为是可信的
	TrustedFreshMs *int64 `toml:"trusted_fresh_ms"`
	// ObserveGraceMs 观测宽限期（毫秒）
	ObserveGraceMs *int64 `toml:"observe_grace_ms"`
	// WriteGateGraceMs 写入门控宽限期（毫秒），时钟不同步时允许写入的缓冲时间
	WriteGateGraceMs *int64 `toml:"write_gate_grace_ms"`
	// RejectAfterFailures 连续失败多少次后拒绝时钟同步
	RejectAfterFailures *int `toml:"reject_after_failures"`
	// RejectAfterSkewSamples 连续偏斜采样多少次后拒绝
	RejectAfterSkewSamples *int `toml:"reject_after_skew_samples"`
	// RecoverAfterHealthySamples 连续健康采样多少次后恢复
	RecoverAfterHealthySamples *int `toml:"recover_after_healthy_samples"`
}

// peerFileConfig 集群对等节点配置。
type peerFileConfig struct {
	// URL 对等节点的 WebSocket 连接地址
	URL string `toml:"url"`
	// ZeroMQ 对等节点的 ZeroMQ 连接配置
	ZeroMQ peerZeroMQFileConfig `toml:"zeromq"`
}

// peerZeroMQFileConfig 对等节点的 ZeroMQ CURVE 认证配置。
type peerZeroMQFileConfig struct {
	// CurveServerPublicKey 对端 ZeroMQ CURVE 服务器公钥
	CurveServerPublicKey string `toml:"curve_server_public_key"`
}

// zeroMQFileConfig ZeroMQ 协议监听器配置（TOML 文件映射）。
type zeroMQFileConfig struct {
	// Enabled 是否启用 ZeroMQ 监听器
	Enabled bool `toml:"enabled"`
	// BindURL ZeroMQ 绑定地址，如 "tcp://0.0.0.0:5555"
	BindURL string `toml:"bind_url"`
	// Security 安全模式，如 "curve"（启用 CURVE 加密）
	Security string `toml:"security"`
	// ForwardingEnabled 是否允许转发 ZeroMQ 连接
	ForwardingEnabled *bool `toml:"forwarding_enabled"`
	// Curve ZeroMQ CURVE 加密密钥对配置
	Curve zeroMQCurveFileConfig `toml:"curve"`
}

// zeroMQCurveFileConfig ZeroMQ CURVE 加密密钥对配置。
type zeroMQCurveFileConfig struct {
	// ServerPublicKey 服务器公钥
	ServerPublicKey string `toml:"server_public_key"`
	// ServerSecretKey 服务器私钥（保密）
	ServerSecretKey string `toml:"server_secret_key"`
	// ClientPublicKey 客户端公钥
	ClientPublicKey string `toml:"client_public_key"`
	// ClientSecretKey 客户端私钥（保密）
	ClientSecretKey string `toml:"client_secret_key"`
	// AllowedClientPublicKeys 允许连接的客户端公钥白名单
	AllowedClientPublicKeys []string `toml:"allowed_client_public_keys"`
}

// libP2PFileConfig LibP2P 协议配置（TOML 文件映射）。
type libP2PFileConfig struct {
	// Enabled 是否启用 LibP2P 传输层
	Enabled bool `toml:"enabled"`
	// PrivateKeyPath LibP2P 节点私钥文件路径
	PrivateKeyPath string `toml:"private_key_path"`
	// ListenAddrs LibP2P 监听地址列表
	ListenAddrs []string `toml:"listen_addrs"`
	// BootstrapPeers 引导节点列表，用于加入 P2P 网络
	BootstrapPeers []string `toml:"bootstrap_peers"`
	// EnableDHT 是否启用分布式哈希表（Kademlia DHT），默认 true
	EnableDHT *bool `toml:"enable_dht"`
	// EnableMDNS 是否启用 mDNS 局域网节点发现
	EnableMDNS bool `toml:"enable_mdns"`
	// RelayPeers 中继节点列表，用于 NAT 穿透
	RelayPeers []string `toml:"relay_peers"`
	// EnableHolePunching 是否启用 NAT 打洞，默认 true
	EnableHolePunching *bool `toml:"enable_hole_punching"`
	// GossipSubEnabled 是否启用 GossipSub 发布订阅，默认 true
	GossipSubEnabled *bool `toml:"gossipsub_enabled"`
	// NativeRelayClientEnabled 是否作为中继客户端（连接中继服务）
	NativeRelayClientEnabled bool `toml:"native_relay_client_enabled"`
	// NativeRelayServiceEnabled 是否作为中继服务节点（为其他节点提供中继）
	NativeRelayServiceEnabled bool `toml:"native_relay_service_enabled"`
}

// runtimeServeConfig 是经过验证和填充默认值后的运行时服务配置。
// 由 serveConfig.runtimeConfig() 从 TOML 文件配置转换而来。
type runtimeServeConfig struct {
	// ConfigPath 实际使用的配置文件路径
	ConfigPath string
	// Services 运行时服务配置（HTTP、ZeroMQ、LibP2P）
	Services runtimeServicesConfig
	// SQLitePath SQLite 数据库文件路径（已通过 filepath.Clean 规范化）
	SQLitePath string
	// PebblePath Pebble 数据库目录路径（已通过 filepath.Clean 规范化）
	PebblePath string
	// EventLogPruneEnabled 是否启用事件日志自动裁剪
	EventLogPruneEnabled bool
	// EventLogPruneInterval 事件日志裁剪间隔
	EventLogPruneInterval time.Duration
	// StoreOptions 存储引擎完整配置选项
	StoreOptions store.Options
	// Auth 运行时认证配置
	Auth runtimeAuthConfig
	// Logging 运行时日志配置
	Logging runtimeLoggingConfig
	// Cluster 集群 Mesh 网络配置（含默认值和验证后结果）
	Cluster cluster.Config
}

// runtimeServicesConfig 运行时服务配置聚合。
type runtimeServicesConfig struct {
	// HTTP HTTP API 监听配置
	HTTP runtimeHTTPServiceConfig
	// ZeroMQ 运行时 ZeroMQ 配置（已转换为 cluster 包类型）
	ZeroMQ cluster.ZeroMQConfig
	// LibP2P 运行时 LibP2P 配置（已转换为 cluster 包类型）
	LibP2P cluster.LibP2PConfig
}

// runtimeHTTPServiceConfig HTTP 服务运行时配置。
type runtimeHTTPServiceConfig struct {
	// ListenAddr HTTP 监听地址
	ListenAddr string
}

// runtimeAuthConfig 认证模块运行时配置。
type runtimeAuthConfig struct {
	// TokenSecret JWT Token 签名密钥
	TokenSecret string
	// TokenTTLMinutes Token 有效期（分钟）
	TokenTTLMinutes int
	// ReconnectTokenTTLMinutes 客户端重连凭据有效期（分钟）
	ReconnectTokenTTLMinutes int
	// BootstrapAdmin 引导管理员配置（已转换为 store 包类型）
	BootstrapAdmin store.BootstrapAdminConfig
}

// loadServeRuntimeConfig 加载并解析 TOML 配置文件，返回运行时配置。
// 步骤：
//  1. 解析配置文件路径（空值使用默认路径）
//  2. 使用 toml.DecodeFile 解析文件到 serveConfig
//  3. 检测未识别的 TOML 字段并报错（防止拼写错误）
//  4. 调用 runtimeConfig() 填充默认值、执行验证、转换为运行时类型
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

// resolveConfigPath 规范化配置文件路径。
// 空字符串或纯空白字符串使用默认值 "./config.toml"。
func resolveConfigPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return defaultConfigPath
	}
	return trimmed
}

// trimStringSlice 对字符串切片中每个元素做 TrimSpace，移除空字符串。
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

// runtimeConfig 将 TOML 文件配置结构体 serveConfig 转换为运行时配置 runtimeServeConfig。
// 主要工作：
//   - 验证必填字段（listen_addr、token_secret、bootstrap_admin）
//   - 为未设置的可选字段填充默认值
//   - 将 TOML 字符串值解析为内部包的类型（如 store.Options、cluster.Config）
//   - 执行集群配置的交叉验证（token_secret 必须与 cluster.secret 不同）
func (c serveConfig) runtimeConfig(configPath string) (runtimeServeConfig, error) {
	httpListenAddr := strings.TrimSpace(c.Services.HTTP.ListenAddr)
	if httpListenAddr == "" {
		return runtimeServeConfig{}, fmt.Errorf("services.http.listen_addr cannot be empty")
	}
	if c.Store.MessageWindowSize < 0 {
		return runtimeServeConfig{}, fmt.Errorf("store.message_window_size must be positive")
	}
	if c.Store.EventLog.MaxEventsPerOrigin < 0 {
		return runtimeServeConfig{}, fmt.Errorf("store.event_log.max_events_per_origin must be positive")
	}
	if c.Store.EventLog.PruneIntervalSeconds < 0 {
		return runtimeServeConfig{}, fmt.Errorf("store.event_log.prune_interval_seconds must be positive")
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
	if c.Auth.ReconnectTokenTTLMinutes < 0 {
		return runtimeServeConfig{}, fmt.Errorf("auth.reconnect_token_ttl_minutes must be non-negative")
	}
	loggingCfg, err := c.Logging.runtimeConfig()
	if err != nil {
		return runtimeServeConfig{}, err
	}

	messageWindowSize := c.Store.MessageWindowSize
	if messageWindowSize == 0 {
		messageWindowSize = store.DefaultMessageWindowSize
	}
	eventLogMaxEventsPerOrigin := c.Store.EventLog.MaxEventsPerOrigin
	if eventLogMaxEventsPerOrigin == 0 {
		eventLogMaxEventsPerOrigin = store.DefaultEventLogMaxEventsPerOrigin
	}
	eventLogPruneEnabled := true
	if c.Store.EventLog.Enabled != nil {
		eventLogPruneEnabled = *c.Store.EventLog.Enabled
	}
	eventLogPruneIntervalSeconds := c.Store.EventLog.PruneIntervalSeconds
	if eventLogPruneIntervalSeconds == 0 {
		eventLogPruneIntervalSeconds = defaultEventLogPruneIntervalSeconds
	}
	sqlitePath := strings.TrimSpace(c.Store.SQLite.DBPath)
	if sqlitePath == "" {
		sqlitePath = defaultSQLitePath
	}
	pebblePath := strings.TrimSpace(c.Store.Pebble.Path)
	if pebblePath == "" {
		pebblePath = defaultPebblePath
	}
	pebbleMessageSyncMode, err := store.NormalizePebbleMessageSyncMode(c.Store.Pebble.MessageSyncMode)
	if err != nil {
		return runtimeServeConfig{}, fmt.Errorf("store.pebble.message_sync_mode: %w", err)
	}
	if pebbleMessageSyncMode == store.PebbleMessageSyncModeDefault {
		pebbleMessageSyncMode = store.PebbleMessageSyncModeNoSync
	}
	pebbleProfile, err := store.NormalizePebbleProfile(c.Store.Pebble.Profile)
	if err != nil {
		return runtimeServeConfig{}, fmt.Errorf("store.pebble.profile: %w", err)
	}
	if pebbleProfile == store.PebbleProfileDefault {
		pebbleProfile = store.PebbleProfileBalanced
	}
	tokenTTLMinutes := c.Auth.TokenTTLMinutes
	if tokenTTLMinutes == 0 {
		tokenTTLMinutes = 1440
	}
	reconnectTokenTTLMinutes := c.Auth.ReconnectTokenTTLMinutes
	if reconnectTokenTTLMinutes == 0 {
		reconnectTokenTTLMinutes = 5
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
	disconnectSuspicionGraceMs := cluster.DefaultDisconnectSuspicionGraceMs
	if c.Cluster.DisconnectSuspicionGraceMs != nil {
		disconnectSuspicionGraceMs = *c.Cluster.DisconnectSuspicionGraceMs
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
		DisconnectSuspicionGraceMs:      disconnectSuspicionGraceMs,
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
		SQLitePath:            filepath.Clean(sqlitePath),
		PebblePath:            filepath.Clean(pebblePath),
		EventLogPruneEnabled:  eventLogPruneEnabled,
		EventLogPruneInterval: time.Duration(eventLogPruneIntervalSeconds) * time.Second,
		StoreOptions: store.Options{
			Engine:                     engine,
			PebblePath:                 filepath.Clean(pebblePath),
			PebbleProfile:              pebbleProfile,
			PebbleMessageSyncMode:      pebbleMessageSyncMode,
			MessageWindowSize:          messageWindowSize,
			EventLogMaxEventsPerOrigin: eventLogMaxEventsPerOrigin,
		},
		Auth: runtimeAuthConfig{
			TokenSecret:              strings.TrimSpace(c.Auth.TokenSecret),
			TokenTTLMinutes:          tokenTTLMinutes,
			ReconnectTokenTTLMinutes: reconnectTokenTTLMinutes,
			BootstrapAdmin: store.BootstrapAdminConfig{
				Username:     strings.TrimSpace(c.Auth.BootstrapAdmin.Username),
				PasswordHash: strings.TrimSpace(c.Auth.BootstrapAdmin.PasswordHash),
				LoginName:    strings.TrimSpace(c.Auth.BootstrapAdmin.LoginName),
			},
		},
		Logging: loggingCfg,
		Cluster: clusterCfg,
	}, nil
}

// runtimeConfig 将 libp2p 的 TOML 配置转换为 cluster.LibP2PConfig 运行时类型。
// 为可选的布尔指针字段（EnableDHT、EnableHolePunching、GossipSubEnabled）设置默认值 true。
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

// runtimeConfig 将消息转发策略的 TOML 配置转换为 cluster.ForwardingConfig 运行时类型。
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

// runtimeConfig 将流量转发策略的 TOML 配置字符串转换为 cluster.ForwardingTrafficConfig 运行时类型。
// 每种流量类型的字符串值通过 cluster.ParseForwardingDisposition 解析为枚举值。
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

// validateClusterFileConfig 验证集群配置的合法性。
// 设置默认 NodeID（为 0 时设为 1），然后调用 cluster.Config.Validate()。
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
