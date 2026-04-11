package cluster

import (
	"fmt"
	"strings"

	"notifier/internal/clock"
)

type Peer struct {
	NodeID int64
	URL    string
}

type Config struct {
	NodeID            int64
	NodeSlot          uint16
	AdvertisePath     string
	ClusterSecret     string
	Peers             []Peer
	MessageWindowSize int
	MaxClockSkewMs    int64
}

const DefaultMaxClockSkewMs int64 = 1000

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.AdvertisePath) != "" || strings.TrimSpace(c.ClusterSecret) != "" || len(c.Peers) > 0
}

func (c Config) Validate() error {
	if c.NodeID <= 0 {
		return fmt.Errorf("node id cannot be empty")
	}
	if c.NodeSlot > clock.MaxNodeID {
		return fmt.Errorf("node slot %d exceeds max %d", c.NodeSlot, clock.MaxNodeID)
	}
	nodeSlot, err := clock.NodeSlotFromID(c.NodeID)
	if err != nil {
		return fmt.Errorf("node id %d is invalid: %w", c.NodeID, err)
	}
	if c.NodeSlot != 0 && c.NodeSlot != nodeSlot {
		return fmt.Errorf("node slot %d does not match node id slot %d", c.NodeSlot, nodeSlot)
	}
	if c.MaxClockSkewMs < 0 {
		return fmt.Errorf("cluster max clock skew must be non-negative")
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

	seenPeers := make(map[int64]struct{}, len(c.Peers))
	for _, peer := range c.Peers {
		if peer.NodeID <= 0 {
			return fmt.Errorf("peer node id cannot be empty")
		}
		if strings.TrimSpace(peer.URL) == "" {
			return fmt.Errorf("peer url cannot be empty")
		}
		if peer.NodeID == c.NodeID {
			return fmt.Errorf("peer node id %d cannot match local node id", peer.NodeID)
		}
		if _, ok := seenPeers[peer.NodeID]; ok {
			return fmt.Errorf("duplicate peer node id %d", peer.NodeID)
		}
		peerSlot, err := clock.NodeSlotFromID(peer.NodeID)
		if err != nil {
			return fmt.Errorf("peer node id %d is invalid: %w", peer.NodeID, err)
		}
		if peerSlot == c.NodeSlot {
			return fmt.Errorf("peer node id %d uses local node slot %d", peer.NodeID, c.NodeSlot)
		}
		seenPeers[peer.NodeID] = struct{}{}
	}
	return nil
}
