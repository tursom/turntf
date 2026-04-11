package cluster

import (
	"fmt"
	"strings"

	"notifier/internal/clock"
)

type Peer struct {
	NodeID string
	URL    string
}

type Config struct {
	NodeID        string
	NodeSlot      uint16
	ListenAddr    string
	AdvertiseAddr string
	ClusterSecret string
	Peers         []Peer
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("node id cannot be empty")
	}
	if c.NodeSlot > clock.MaxNodeID {
		return fmt.Errorf("node slot %d exceeds max %d", c.NodeSlot, clock.MaxNodeID)
	}

	enabled := strings.TrimSpace(c.ListenAddr) != "" || strings.TrimSpace(c.AdvertiseAddr) != "" || len(c.Peers) > 0
	if enabled {
		if strings.TrimSpace(c.ClusterSecret) == "" {
			return fmt.Errorf("cluster secret cannot be empty")
		}
		if strings.TrimSpace(c.ListenAddr) == "" {
			return fmt.Errorf("cluster listen addr cannot be empty when cluster mode is enabled")
		}
		if strings.TrimSpace(c.AdvertiseAddr) == "" {
			return fmt.Errorf("cluster advertise addr cannot be empty when cluster mode is enabled")
		}
	}

	seenPeers := make(map[string]struct{}, len(c.Peers))
	for _, peer := range c.Peers {
		if strings.TrimSpace(peer.NodeID) == "" {
			return fmt.Errorf("peer node id cannot be empty")
		}
		if strings.TrimSpace(peer.URL) == "" {
			return fmt.Errorf("peer url cannot be empty")
		}
		if peer.NodeID == c.NodeID {
			return fmt.Errorf("peer node id %q cannot match local node id", peer.NodeID)
		}
		if _, ok := seenPeers[peer.NodeID]; ok {
			return fmt.Errorf("duplicate peer node id %q", peer.NodeID)
		}
		seenPeers[peer.NodeID] = struct{}{}
	}
	return nil
}
