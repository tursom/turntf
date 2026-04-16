package cluster

import (
	"fmt"
	"strings"
)

type Peer struct {
	URL                        string
	ZeroMQCurveServerPublicKey string
}

type LibP2PConfig struct {
	Enabled                   bool
	PrivateKeyPath            string
	ListenAddrs               []string
	BootstrapPeers            []string
	EnableDHT                 bool
	EnableMDNS                bool
	RelayPeers                []string
	EnableHolePunching        bool
	GossipSubEnabled          bool
	NativeRelayClientEnabled  bool
	NativeRelayServiceEnabled bool
}

type ZeroMQConfig struct {
	Enabled           bool
	BindURL           string
	Security          string
	ForwardingEnabled *bool
	Curve             ZeroMQCurveConfig
}

type ZeroMQCurveConfig struct {
	ServerPublicKey         string
	ServerSecretKey         string
	ClientPublicKey         string
	ClientSecretKey         string
	AllowedClientPublicKeys []string
}

type Config struct {
	NodeID                          int64
	AdvertisePath                   string
	ClusterSecret                   string
	Forwarding                      ForwardingConfig
	ZeroMQ                          ZeroMQConfig
	LibP2P                          LibP2PConfig
	Peers                           []Peer
	DiscoveryDisabled               bool
	MessageWindowSize               int
	MaxClockSkewMs                  int64
	ClockSyncTimeoutMs              int64
	ClockCredibleRttMs              int64
	ClockTrustedFreshMs             int64
	ClockObserveGraceMs             int64
	ClockWriteGateGraceMs           int64
	ClockRejectAfterFailures        int
	ClockRejectAfterSkewSamples     int
	ClockRecoverAfterHealthySamples int
}

const DefaultMaxClockSkewMs int64 = 1000
const DefaultClockSyncTimeoutMs int64 = 8000
const DefaultClockCredibleRTTMs int64 = 4000
const DefaultClockTrustedFreshMs int64 = 60_000
const DefaultClockObserveGraceMs int64 = 180_000
const DefaultClockWriteGateGraceMs int64 = 300_000
const DefaultClockRejectAfterFailures = 3
const DefaultClockRejectAfterSkewSamples = 3
const DefaultClockRecoverAfterHealthySamples = 2
const DefaultLibP2PPrivateKeyPath = "./data/libp2p.key"

const (
	ZeroMQSecurityNone  = "none"
	ZeroMQSecurityCurve = "curve"
)

func (c Config) WithDefaults() Config {
	c.Forwarding = c.Forwarding.withDefaults()
	if c.ZeroMQ.ForwardingEnabled == nil {
		c.ZeroMQ.ForwardingEnabled = boolPtr(boolValue(c.Forwarding.Enabled, true))
	}
	if c.LibP2P.Enabled {
		if strings.TrimSpace(c.LibP2P.PrivateKeyPath) == "" {
			c.LibP2P.PrivateKeyPath = DefaultLibP2PPrivateKeyPath
		}
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

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.ClusterSecret) != "" || len(c.Peers) > 0 || c.LibP2P.Enabled
}

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

func normalizeLibP2PConfig(cfg LibP2PConfig) LibP2PConfig {
	cfg.PrivateKeyPath = strings.TrimSpace(cfg.PrivateKeyPath)
	cfg.ListenAddrs = trimNonEmptyStrings(cfg.ListenAddrs)
	cfg.BootstrapPeers = trimNonEmptyStrings(cfg.BootstrapPeers)
	cfg.RelayPeers = trimNonEmptyStrings(cfg.RelayPeers)
	return cfg
}

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

func (c Config) zeroMQDialEnabled() bool {
	return c.ZeroMQ.Enabled
}

func (c Config) zeroMQListenerEnabled() bool {
	return c.ZeroMQ.Enabled && strings.TrimSpace(c.ZeroMQ.BindURL) != ""
}

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

func (c Config) zeroMQSecurity() string {
	security := strings.ToLower(strings.TrimSpace(c.ZeroMQ.Security))
	if security == "" {
		return ZeroMQSecurityNone
	}
	return security
}

func (c Config) zeroMQCurveEnabled() bool {
	return c.zeroMQSecurity() == ZeroMQSecurityCurve
}

func (c Config) zeroMQCurveServerPublicKey() string {
	if !c.zeroMQCurveEnabled() {
		return ""
	}
	return strings.TrimSpace(c.ZeroMQ.Curve.ServerPublicKey)
}

func (c Config) libP2PMode() string {
	if !c.LibP2P.Enabled {
		return "disabled"
	}
	if len(c.LibP2P.ListenAddrs) == 0 {
		return "outbound_only"
	}
	return "listening"
}

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

func validateZeroMQCurveKey(name, key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}
	if len(key) != 40 {
		return fmt.Errorf("%s must be a 40-character z85 key", name)
	}
	return nil
}
