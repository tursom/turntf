package cluster

import (
	"fmt"
	"strings"
)

type Peer struct {
	URL string
}

type Config struct {
	NodeID                          int64
	AdvertisePath                   string
	ClusterSecret                   string
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

func (c Config) WithDefaults() Config {
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
	return strings.TrimSpace(c.AdvertisePath) != "" || strings.TrimSpace(c.ClusterSecret) != "" || len(c.Peers) > 0
}

func (c Config) Validate() error {
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
