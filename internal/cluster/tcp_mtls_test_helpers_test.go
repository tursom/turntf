package cluster

import (
	"testing"

	"github.com/tursom/turntf/internal/store"
)

func newDiscoveryTestManager(t *testing.T, st *store.Store) *Manager {
	t.Helper()
	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
	}, st)
	if err != nil {
		t.Fatalf("new discovery manager: %v", err)
	}
	return mgr
}
