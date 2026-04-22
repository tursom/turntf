package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerReplicatesEventBatchViaMeshMultiHop(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficReplicationStream)
	waitForMeshRoute(t, mgrC, testNodeID(1), mesh.TrafficControlCritical)

	user, event, err := mgrA.store.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "mesh-replication-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("create source user: %v", err)
	}

	mgrA.broadcastEvent(event)

	waitFor(t, 5*time.Second, func() bool {
		replicated, err := mgrC.store.GetUser(context.Background(), user.Key())
		return err == nil && replicated.Username == user.Username
	})

	waitFor(t, 5*time.Second, func() bool {
		cursor, err := mgrA.store.GetPeerAckCursor(context.Background(), testNodeID(3), testNodeID(1))
		return err == nil && cursor.AckedEventID >= event.EventID
	})
}

func TestManagerRepairsSnapshotViaMeshMultiHop(t *testing.T) {
	t.Parallel()

	mgrA, _, mgrC := startLinearMeshManagers(t)

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficSnapshotBulk)
	waitForMeshRoute(t, mgrC, testNodeID(1), mesh.TrafficSnapshotBulk)

	user, _, err := mgrA.store.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "mesh-snapshot-user",
		PasswordHash: "hash-2",
	})
	if err != nil {
		t.Fatalf("create snapshot source user: %v", err)
	}

	envelope, err := mgrA.buildSnapshotDigestEnvelope()
	if err != nil {
		t.Fatalf("build snapshot digest: %v", err)
	}
	if err := mgrA.routeMeshSnapshotManifest(context.Background(), testNodeID(3), envelope.GetSnapshotDigest()); err != nil {
		t.Fatalf("route snapshot digest via mesh: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		replicated, err := mgrC.store.GetUser(context.Background(), user.Key())
		return err == nil && replicated.Username == user.Username
	})
}

func TestManagerMeshHighFeeTransitRejectsReplicationAndSnapshot(t *testing.T) {
	t.Parallel()

	mgrA, _, _ := startLinearMeshManagers(t, func(nodeSlot int, cfg *Config) {
		if nodeSlot == 2 {
			cfg.Forwarding.NodeFeeWeight = 2
		}
	})

	waitForMeshRoute(t, mgrA, testNodeID(3), mesh.TrafficControlCritical)

	err := mgrA.routeMeshReplicationBatch(context.Background(), testNodeID(3), 1, "2026-01-01T00:00:00.000000000Z/1/1", &internalproto.EventBatch{
		OriginNodeId: testNodeID(1),
	})
	if !errors.Is(err, mesh.ErrNoRoute) {
		t.Fatalf("expected replication no route over high-fee transit, got %v", err)
	}

	err = mgrA.routeMeshSnapshotManifest(context.Background(), testNodeID(3), &internalproto.SnapshotDigest{
		SnapshotVersion: internalproto.SnapshotVersion,
	})
	if !errors.Is(err, mesh.ErrNoRoute) {
		t.Fatalf("expected snapshot no route over high-fee transit, got %v", err)
	}

	metrics := mgrA.meshMetricsSnapshot()
	if !hasMeshNoPathMetric(metrics, "replication_stream") {
		t.Fatalf("expected replication no-path metric, got %+v", metrics.RoutingNoPath)
	}
	if !hasMeshNoPathMetric(metrics, "snapshot_bulk") {
		t.Fatalf("expected snapshot no-path metric, got %+v", metrics.RoutingNoPath)
	}
}

func hasMeshNoPathMetric(metrics meshMetricsSnapshot, trafficClass string) bool {
	for _, sample := range metrics.RoutingNoPath {
		if sample.TrafficClass == trafficClass && sample.PathClass == "no_path" && sample.Value > 0 {
			return true
		}
	}
	return false
}
