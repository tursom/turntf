package cluster

import (
	"fmt"
	"strings"
)

// Peer 表示集群中的一个静态配置的对等节点。
type Peer struct {
	// URL 是对等节点的地址，支持 ws、wss 或 zmq+tcp scheme。
	URL string
	// ZeroMQCurveServerPublicKey 是ZeroMQ Curve加密中该对等节点的Z85服务器公钥。
	// 仅当URL使用 zmq+tcp scheme 且启用了 curve 安全时使用。
	ZeroMQCurveServerPublicKey string
}

// LibP2PConfig 包含libp2p传输和发现相关的配置。
type LibP2PConfig struct {
	// Enabled 是否启用libp2p传输。
	Enabled bool
	// PrivateKeyPath 是libp2p节点私钥的文件路径。
	PrivateKeyPath string
	// ListenAddrs 是libp2p监听的multiaddr地址列表。
	ListenAddrs []string
	// BootstrapPeers 是启动时连接的引导对等节点multiaddr列表。
	BootstrapPeers []string
	// EnableDHT 是否启用Kademlia DHT用于对等节点发现。
	EnableDHT bool
	// EnableMDNS 是否启用mDNS用于局域网对等节点发现。
	EnableMDNS bool
	// RelayPeers 是中继对等节点的multiaddr列表。
	RelayPeers []string
	// EnableHolePunching 是否启用NAT穿透。
	EnableHolePunching bool
	// GossipSubEnabled 是否启用GossipSub pubsub协议。
	GossipSubEnabled bool
	// NativeRelayClientEnabled 是否启用libp2p原生中继客户端。
	NativeRelayClientEnabled bool
	// NativeRelayServiceEnabled 是否启用libp2p原生中继服务。
	NativeRelayServiceEnabled bool
}

// ZeroMQConfig 包含ZeroMQ传输相关的配置。
type ZeroMQConfig struct {
	// Enabled 是否启用ZeroMQ传输。
	Enabled bool
	// BindURL 是ZeroMQ绑定的TCP地址。
	BindURL string
	// Security 是安全模式：none 或 curve。
	Security string
	// ForwardingEnabled 控制是否允许通过ZeroMQ进行转发。
	// nil表示使用Forwarding.Enabled的默认值。
	ForwardingEnabled *bool
	// Curve 包含CurveZMQ安全相关的密钥配置。
	Curve ZeroMQCurveConfig
}

// ZeroMQCurveConfig 包含CurveZMQ椭圆曲线加密的密钥配置。
// 所有密钥均为40字符的Z85编码字符串。
type ZeroMQCurveConfig struct {
	// ServerPublicKey 是服务器端的长期公钥。
	ServerPublicKey string
	// ServerSecretKey 是服务器端的长期私钥。
	ServerSecretKey string
	// ClientPublicKey 是客户端的长期公钥。
	ClientPublicKey string
	// ClientSecretKey 是客户端的长期私钥。
	ClientSecretKey string
	// AllowedClientPublicKeys 是允许连接的客户端公钥白名单。
	AllowedClientPublicKeys []string
}

// Config 是集群模块的完整配置。
// 零值字段将在WithDefaults和Validate过程中填充为合理的默认值。
type Config struct {
	// NodeID 是当前节点的唯一标识符，必须大于0。
	NodeID int64
	// AdvertisePath 是对外通告的HTTP WebSocket路径（如 /internal/cluster/ws）。
	AdvertisePath string
	// ClusterSecret 是用于HMAC信封签名的共享密钥。
	ClusterSecret string
	// DisconnectSuspicionGraceMs 是断开连接怀疑的容忍期（毫秒）。
	DisconnectSuspicionGraceMs int64
	// Forwarding 是网状路由的转发策略配置。
	Forwarding ForwardingConfig
	// ZeroMQ 是ZeroMQ传输的配置。
	ZeroMQ ZeroMQConfig
	// LibP2P 是libp2p传输的配置。
	LibP2P LibP2PConfig
	// Peers 是静态配置的对等节点列表。
	Peers []Peer
	// DiscoveryDisabled 是否禁用自动对等节点发现。
	DiscoveryDisabled bool
	// MessageWindowSize 是每个事件流保留的最大事件数。
	MessageWindowSize int
	// MaxClockSkewMs 是允许的最大时钟偏差（毫秒）。超过此偏差将导致时钟状态变为rejected。
	MaxClockSkewMs int64
	// ClockSyncTimeoutMs 是时间同步请求的超时时间（毫秒）。
	ClockSyncTimeoutMs int64
	// ClockCredibleRttMs 是判定时钟同步样本可信的RTT上限（毫秒）。
	ClockCredibleRttMs int64
	// ClockTrustedFreshMs 是从最近的可信时钟同步到节点时钟变为observing的时间（毫秒）。
	ClockTrustedFreshMs int64
	// ClockObserveGraceMs 是从observing状态到降级为degraded的容忍期（毫秒）。
	ClockObserveGraceMs int64
	// ClockWriteGateGraceMs 是写门控保持在degraded状态直到变为unwritable的时间（毫秒）。
	ClockWriteGateGraceMs int64
	// ClockRejectAfterFailures 是连续的时钟同步失败次数，超过此次数后将拒绝该对等节点。
	ClockRejectAfterFailures int
	// ClockRejectAfterSkewSamples 是确认时钟偏差所需的连续异常样本数。
	ClockRejectAfterSkewSamples int
	// ClockRecoverAfterHealthySamples 是从observing恢复到trusted所需的连续健康样本数。
	ClockRecoverAfterHealthySamples int
}

// 时钟相关配置的默认值（毫秒）。
const DefaultMaxClockSkewMs int64 = 1000
const DefaultClockSyncTimeoutMs int64 = 8000
const DefaultClockCredibleRTTMs int64 = 4000
const DefaultClockTrustedFreshMs int64 = 60_000
const DefaultClockObserveGraceMs int64 = 180_000
const DefaultClockWriteGateGraceMs int64 = 300_000
const DefaultDisconnectSuspicionGraceMs int64 = 120_000
const DefaultClockRejectAfterFailures = 3
const DefaultClockRejectAfterSkewSamples = 3
const DefaultClockRecoverAfterHealthySamples = 2
const DefaultLibP2PPrivateKeyPath = "./data/libp2p.key"

// ZeroMQ安全模式常量。
const (
	ZeroMQSecurityNone  = "none"
	ZeroMQSecurityCurve = "curve"
)

// WithDefaults 返回填充了所有零值字段默认值的Config副本。
func (c Config) WithDefaults() Config {
	c.Forwarding = c.Forwarding.withDefaults()
	if c.ZeroMQ.ForwardingEnabled == nil {
		c.ZeroMQ.ForwardingEnabled = boolPtr(boolValue(c.Forwarding.Enabled, true))
	}
	if c.LibP2P.Enabled {
		if strings.TrimSpace(c.LibP2P.PrivateKeyPath) == "" {
			c.LibP2P.PrivateKeyPath = DefaultLibP2PPrivateKeyPath
		}
		// 如果未显式启用任何libp2p功能，则默认启用DHT、NAT穿透和GossipSub
		if !c.LibP2P.EnableDHT && !c.LibP2P.EnableMDNS && !c.LibP2P.EnableHolePunching && !c.LibP2P.GossipSubEnabled {
			c.LibP2P.EnableDHT = true
			c.LibP2P.EnableHolePunching = true
			c.LibP2P.GossipSubEnabled = true
		}
	}
	if c.ClockSyncTimeoutMs == 0 {
		c.ClockSyncTimeoutMs = DefaultClockSyncTimeoutMs
	}
	if c.ClockCredibleRttMs == 0 {
		c.ClockCredibleRttMs = DefaultClockCredibleRTTMs
	}
	if c.ClockTrustedFreshMs == 0 {
		c.ClockTrustedFreshMs = DefaultClockTrustedFreshMs
	}
	if c.ClockObserveGraceMs == 0 {
		c.ClockObserveGraceMs = DefaultClockObserveGraceMs
	}
	if c.ClockWriteGateGraceMs == 0 {
		c.ClockWriteGateGraceMs = DefaultClockWriteGateGraceMs
	}
	if c.DisconnectSuspicionGraceMs == 0 {
		c.DisconnectSuspicionGraceMs = DefaultDisconnectSuspicionGraceMs
	}
	if c.ClockRejectAfterFailures == 0 {
		c.ClockRejectAfterFailures = DefaultClockRejectAfterFailures
	}
	if c.ClockRejectAfterSkewSamples == 0 {
		c.ClockRejectAfterSkewSamples = DefaultClockRejectAfterSkewSamples
	}
	if c.ClockRecoverAfterHealthySamples == 0 {
		c.ClockRecoverAfterHealthySamples = DefaultClockRecoverAfterHealthySamples
	}
	return c
}

// Enabled 返回集群模式是否已启用。
// 当设置了集群密钥、配置了对等节点或启用了libp2p时，集群模式生效。
func (c Config) Enabled() bool {
	return strings.TrimSpace(c.ClusterSecret) != "" || len(c.Peers) > 0 || c.LibP2P.Enabled
}

// Validate 验证配置的有效性，并在验证前填充默认值。
// 它会规范化所有URL、验证密钥格式并检查一致性。
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("cluster config cannot be nil")
	}
	*c = c.WithDefaults()
	if c.NodeID <= 0 {
		return fmt.Errorf("node id cannot be empty")
	}
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
	if c.DisconnectSuspicionGraceMs < 0 {
		return fmt.Errorf("cluster disconnect suspicion grace must be non-negative")
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

	c.AdvertisePath = strings.TrimSpace(c.AdvertisePath)
	c.ClusterSecret = strings.TrimSpace(c.ClusterSecret)
	c.ZeroMQ.BindURL = strings.TrimSpace(c.ZeroMQ.BindURL)
	c.ZeroMQ.Security = strings.ToLower(strings.TrimSpace(c.ZeroMQ.Security))
	c.LibP2P = normalizeLibP2PConfig(c.LibP2P)
	if c.ZeroMQ.Security == "" {
		c.ZeroMQ.Security = ZeroMQSecurityNone
	}
	c.ZeroMQ.Curve = normalizeZeroMQCurveConfig(c.ZeroMQ.Curve)
	c.Forwarding = c.Forwarding.withDefaults()
	if err := c.Forwarding.validate(); err != nil {
		return err
	}

	if c.Enabled() {
		if c.ClusterSecret == "" {
			return fmt.Errorf("cluster secret cannot be empty")
		}
		if c.AdvertisePath == "" {
			return fmt.Errorf("cluster advertise path cannot be empty when cluster mode is enabled")
		}
		if !strings.HasPrefix(c.AdvertisePath, "/") {
			return fmt.Errorf("cluster advertise path must start with /")
		}
	}
	if c.ZeroMQ.Enabled {
		if c.ZeroMQ.BindURL != "" {
			normalizedBindURL, err := normalizeZeroMQBindURL(c.ZeroMQ.BindURL)
			if err != nil {
				return err
			}
			c.ZeroMQ.BindURL = normalizedBindURL
		}
	}
	if err := c.validateZeroMQSecurity(); err != nil {
		return err
	}
	if err := c.validateLibP2P(); err != nil {
		return err
	}

	// 验证对等节点URL并检查重复
	seenPeers := make(map[string]struct{}, len(c.Peers))
	for idx := range c.Peers {
		c.Peers[idx].ZeroMQCurveServerPublicKey = strings.TrimSpace(c.Peers[idx].ZeroMQCurveServerPublicKey)
		normalizedURL, err := normalizeConfiguredPeerURL(c.Peers[idx].URL)
		if err != nil {
			return err
		}
		if isZeroMQPeerURL(normalizedURL) && !c.ZeroMQ.Enabled {
			return fmt.Errorf("zeromq peer url %q requires services.zeromq.enabled", normalizedURL)
		}
		if isLibP2PPeerURL(normalizedURL) && !c.LibP2P.Enabled {
			return fmt.Errorf("libp2p peer url %q requires services.libp2p.enabled", normalizedURL)
		}
		if !isZeroMQPeerURL(normalizedURL) && c.Peers[idx].ZeroMQCurveServerPublicKey != "" {
			return fmt.Errorf("zeromq curve server public key requires a zmq+tcp peer url")
		}
		if isZeroMQPeerURL(normalizedURL) && c.ZeroMQ.Security == ZeroMQSecurityCurve {
			if err := validateZeroMQCurveKey("zeromq peer curve server public key", c.Peers[idx].ZeroMQCurveServerPublicKey); err != nil {
				return err
			}
		}
		if _, ok := seenPeers[normalizedURL]; ok {
			return fmt.Errorf("duplicate peer url %q", normalizedURL)
		}
		seenPeers[normalizedURL] = struct{}{}
		c.Peers[idx].URL = normalizedURL
	}
	return nil
}

// normalizeLibP2PConfig 规范化libp2p配置中的字符串字段。
func normalizeLibP2PConfig(cfg LibP2PConfig) LibP2PConfig {
	cfg.PrivateKeyPath = strings.TrimSpace(cfg.PrivateKeyPath)
	cfg.ListenAddrs = trimNonEmptyStrings(cfg.ListenAddrs)
	cfg.BootstrapPeers = trimNonEmptyStrings(cfg.BootstrapPeers)
	cfg.RelayPeers = trimNonEmptyStrings(cfg.RelayPeers)
	return cfg
}

// trimNonEmptyStrings 去除字符串切片中的空白，并过滤掉空字符串。
func trimNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// validateLibP2P 验证libp2p配置，规范化监听地址、引导节点和中继节点。
func (c *Config) validateLibP2P() error {
	if !c.LibP2P.Enabled {
		return nil
	}
	if strings.TrimSpace(c.LibP2P.PrivateKeyPath) == "" {
		return fmt.Errorf("libp2p private key path cannot be empty")
	}
	seenListen := make(map[string]struct{}, len(c.LibP2P.ListenAddrs))
	for idx, raw := range c.LibP2P.ListenAddrs {
		normalized, err := normalizeLibP2PListenAddr(raw)
		if err != nil {
			return err
		}
		if _, ok := seenListen[normalized]; ok {
			return fmt.Errorf("duplicate libp2p listen addr %q", normalized)
		}
		seenListen[normalized] = struct{}{}
		c.LibP2P.ListenAddrs[idx] = normalized
	}
	// 无监听地址时默认监听随机端口
	if len(c.LibP2P.ListenAddrs) == 0 {
		c.LibP2P.ListenAddrs = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	if err := normalizeLibP2PPeerList(c.LibP2P.BootstrapPeers, "libp2p bootstrap peer"); err != nil {
		return err
	}
	if err := normalizeLibP2PPeerList(c.LibP2P.RelayPeers, "libp2p relay peer"); err != nil {
		return err
	}
	return nil
}

// normalizeLibP2PPeerList 规范化libp2p对等节点地址列表，检查重复。
func normalizeLibP2PPeerList(peers []string, label string) error {
	seen := make(map[string]struct{}, len(peers))
	for idx, raw := range peers {
		normalized, err := normalizeLibP2PPeerAddr(raw)
		if err != nil {
			return fmt.Errorf("%s %q: %w", label, raw, err)
		}
		if _, ok := seen[normalized]; ok {
			return fmt.Errorf("duplicate %s %q", label, normalized)
		}
		seen[normalized] = struct{}{}
		peers[idx] = normalized
	}
	return nil
}

// zeroMQDialEnabled 返回是否启用了ZeroMQ出站拨号。
func (c Config) zeroMQDialEnabled() bool {
	return c.ZeroMQ.Enabled
}

// zeroMQListenerEnabled 返回是否启用了ZeroMQ入站监听（需要Enabled且设置了BindURL）。
func (c Config) zeroMQListenerEnabled() bool {
	return c.ZeroMQ.Enabled && strings.TrimSpace(c.ZeroMQ.BindURL) != ""
}

// zeroMQMode 返回ZeroMQ的运行模式：disabled、outbound_only或listening。
func (c Config) zeroMQMode() string {
	switch {
	case !c.ZeroMQ.Enabled:
		return "disabled"
	case strings.TrimSpace(c.ZeroMQ.BindURL) == "":
		return "outbound_only"
	default:
		return "listening"
	}
}

// zeroMQSecurity 返回ZeroMQ的安全模式，空值默认为none。
func (c Config) zeroMQSecurity() string {
	security := strings.ToLower(strings.TrimSpace(c.ZeroMQ.Security))
	if security == "" {
		return ZeroMQSecurityNone
	}
	return security
}

// zeroMQCurveEnabled 返回是否启用了CurveZMQ加密。
func (c Config) zeroMQCurveEnabled() bool {
	return c.zeroMQSecurity() == ZeroMQSecurityCurve
}

// zeroMQCurveServerPublicKey 返回CurveZMQ服务器公钥（仅在启用curve时有效）。
func (c Config) zeroMQCurveServerPublicKey() string {
	if !c.zeroMQCurveEnabled() {
		return ""
	}
	return strings.TrimSpace(c.ZeroMQ.Curve.ServerPublicKey)
}

// libP2PMode 返回libp2p的运行模式：disabled、outbound_only或listening。
func (c Config) libP2PMode() string {
	if !c.LibP2P.Enabled {
		return "disabled"
	}
	if len(c.LibP2P.ListenAddrs) == 0 {
		return "outbound_only"
	}
	return "listening"
}

// validateZeroMQSecurity 验证ZeroMQ安全配置。
// none模式无需额外验证。curve模式需要所有四个密钥和允许的客户端公钥。
func (c *Config) validateZeroMQSecurity() error {
	switch c.ZeroMQ.Security {
	case ZeroMQSecurityNone:
		return nil
	case ZeroMQSecurityCurve:
		if !c.ZeroMQ.Enabled {
			return fmt.Errorf("zeromq curve security requires services.zeromq.enabled")
		}
		if err := validateZeroMQCurveKey("zeromq curve server public key", c.ZeroMQ.Curve.ServerPublicKey); err != nil {
			return err
		}
		if err := validateZeroMQCurveKey("zeromq curve server secret key", c.ZeroMQ.Curve.ServerSecretKey); err != nil {
			return err
		}
		if err := validateZeroMQCurveKey("zeromq curve client public key", c.ZeroMQ.Curve.ClientPublicKey); err != nil {
			return err
		}
		if err := validateZeroMQCurveKey("zeromq curve client secret key", c.ZeroMQ.Curve.ClientSecretKey); err != nil {
			return err
		}
		if c.zeroMQListenerEnabled() && len(c.ZeroMQ.Curve.AllowedClientPublicKeys) == 0 {
			return fmt.Errorf("zeromq curve allowed client public keys cannot be empty when bind_url is set")
		}
		for _, key := range c.ZeroMQ.Curve.AllowedClientPublicKeys {
			if err := validateZeroMQCurveKey("zeromq curve allowed client public key", key); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("zeromq security must be none or curve")
	}
}

// normalizeZeroMQCurveConfig 规范化CurveZMQ配置中的密钥字段，去重空格。
func normalizeZeroMQCurveConfig(curve ZeroMQCurveConfig) ZeroMQCurveConfig {
	curve.ServerPublicKey = strings.TrimSpace(curve.ServerPublicKey)
	curve.ServerSecretKey = strings.TrimSpace(curve.ServerSecretKey)
	curve.ClientPublicKey = strings.TrimSpace(curve.ClientPublicKey)
	curve.ClientSecretKey = strings.TrimSpace(curve.ClientSecretKey)
	seen := make(map[string]struct{}, len(curve.AllowedClientPublicKeys))
	keys := make([]string, 0, len(curve.AllowedClientPublicKeys))
	for _, raw := range curve.AllowedClientPublicKeys {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	curve.AllowedClientPublicKeys = keys
	return curve
}

// validateZeroMQCurveKey 验证Z85密钥：必须非空且长度恰好为40字符。
func validateZeroMQCurveKey(name, key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}
	if len(key) != 40 {
		return fmt.Errorf("%s must be a 40-character z85 key", name)
	}
	return nil
}
