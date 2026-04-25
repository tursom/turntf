package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func (m *Manager) buildSnapshotDigestEnvelope() (*internalproto.Envelope, error) {
	if m.store == nil {
		return nil, errors.New("snapshot digest requires store")
	}
	digest, err := m.store.BuildSnapshotDigest(context.Background(), m.snapshotProducerNodeIDs())
	if err != nil {
		return nil, err
	}
	digest.SnapshotVersion = internalproto.SnapshotVersion
	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_SnapshotDigest{
			SnapshotDigest: digest,
		},
	}, nil
}

func (m *Manager) sendSnapshotDigest(sess *session) bool {
	if m.store == nil || sess == nil || sess.peerID == 0 {
		return false
	}
	if !sess.isReplicationReady() || sess.hasPendingPulls() || sess.pendingSnapshotCount() > 0 || sess.isClosed() {
		return false
	}
	if sess.remoteSnapshotVersion != internalproto.SnapshotVersion {
		return false
	}
	m.mu.Lock()
	if err := m.allowSnapshotTrafficForSessionLocked(sess); err != nil {
		m.mu.Unlock()
		m.logSessionWarn("snapshot_digest_skipped_by_clock", sess, nil).
			Str("reason", err.Error()).
			Msg("skipping snapshot digest due to clock protection")
		return false
	}
	m.mu.Unlock()

	envelope, err := m.buildSnapshotDigestEnvelope()
	if err != nil {
		m.logSessionWarn("build_snapshot_digest_failed", sess, err).
			Msg("build snapshot digest failed")
		return false
	}
	m.markSnapshotDigestSent(sess.peerID)
	digest := envelope.GetSnapshotDigest()
	partitionCount := 0
	if digest != nil {
		partitionCount = len(digest.GetPartitions())
	}
	m.logSessionDebug("snapshot_digest_sent", sess).
		Int("partition_count", partitionCount).
		Msg("snapshot digest sent")
	if sess.conn == nil && m.MeshRuntime() != nil {
		if err := m.routeMeshSnapshotManifest(context.Background(), sess.peerID, digest); err != nil {
			m.logSessionWarn("mesh_snapshot_manifest_forward_failed", sess, err).
				Msg("failed to forward snapshot manifest over mesh")
			return false
		}
		return true
	}
	sess.enqueue(envelope)
	return true
}

func (m *Manager) handleSnapshotDigest(sess *session, envelope *internalproto.Envelope) error {
	if !sess.isReplicationReady() {
		return nil
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	if m.store == nil {
		return nil
	}
	m.mu.Lock()
	if err := m.allowSnapshotTrafficForSessionLocked(sess); err != nil {
		m.mu.Unlock()
		m.logSessionWarn("snapshot_digest_rejected_by_clock", sess, nil).
			Str("reason", err.Error()).
			Msg("snapshot digest rejected by clock protection")
		return err
	}
	m.mu.Unlock()
	m.markSnapshotDigestReceived(sess.peerID)

	digest := envelope.GetSnapshotDigest()
	if digest == nil {
		return errors.New("snapshot digest body cannot be empty")
	}
	if digest.SnapshotVersion != internalproto.SnapshotVersion {
		return fmt.Errorf("unsupported snapshot digest version %q", digest.SnapshotVersion)
	}
	m.logSessionDebug("snapshot_digest_received", sess).
		Int("partition_count", len(digest.GetPartitions())).
		Str("snapshot_version", digest.SnapshotVersion).
		Msg("snapshot digest received")

	localDigest, err := m.store.BuildSnapshotDigest(context.Background(), m.snapshotProducerNodeIDs())
	if err != nil {
		return err
	}

	local := snapshotDigestByPartition(localDigest.GetPartitions())
	remote := snapshotDigestByPartition(digest.GetPartitions())
	remoteUsers, ok := remote[store.SnapshotUsersPartition]
	if !ok {
		return fmt.Errorf("snapshot digest missing %s partition", store.SnapshotUsersPartition)
	}
	if !snapshotPartitionDigestEqual(local[store.SnapshotUsersPartition], remoteUsers) {
		m.logSessionEvent("snapshot_partition_mismatch", sess).
			Str("partition", remoteUsers.Partition).
			Stringer("kind", remoteUsers.Kind).
			Uint64("row_count", remoteUsers.RowCount).
			Msg("snapshot partition mismatch detected")
		m.requestSnapshotPartition(sess, remoteUsers)
		return nil
	}
	remoteSubscriptions, ok := remote[store.SnapshotSubscriptionsPartition]
	if !ok {
		return fmt.Errorf("snapshot digest missing %s partition", store.SnapshotSubscriptionsPartition)
	}
	if !snapshotPartitionDigestEqual(local[store.SnapshotSubscriptionsPartition], remoteSubscriptions) {
		m.logSessionEvent("snapshot_partition_mismatch", sess).
			Str("partition", remoteSubscriptions.Partition).
			Stringer("kind", remoteSubscriptions.Kind).
			Uint64("row_count", remoteSubscriptions.RowCount).
			Msg("snapshot partition mismatch detected")
		m.requestSnapshotPartition(sess, remoteSubscriptions)
		return nil
	}
	remoteBlacklists, ok := remote[store.SnapshotBlacklistsPartition]
	if !ok {
		return fmt.Errorf("snapshot digest missing %s partition", store.SnapshotBlacklistsPartition)
	}
	if !snapshotPartitionDigestEqual(local[store.SnapshotBlacklistsPartition], remoteBlacklists) {
		m.logSessionEvent("snapshot_partition_mismatch", sess).
			Str("partition", remoteBlacklists.Partition).
			Stringer("kind", remoteBlacklists.Kind).
			Uint64("row_count", remoteBlacklists.RowCount).
			Msg("snapshot partition mismatch detected")
		m.requestSnapshotPartition(sess, remoteBlacklists)
		return nil
	}
	if sess.remoteMessageWindowSize != m.cfg.MessageWindowSize {
		return nil
	}

	partitions := make([]string, 0, len(remote))
	for partition := range remote {
		if partition == store.SnapshotUsersPartition {
			continue
		}
		if partition == store.SnapshotSubscriptionsPartition {
			continue
		}
		if partition == store.SnapshotBlacklistsPartition {
			continue
		}
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)

	for _, partition := range partitions {
		remotePartition := remote[partition]
		if remotePartition.Kind != internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_MESSAGES {
			return fmt.Errorf("snapshot digest partition %s has unsupported kind %s", partition, remotePartition.Kind)
		}
		if !snapshotPartitionDigestEqual(local[partition], remotePartition) {
			m.logSessionEvent("snapshot_partition_mismatch", sess).
				Str("partition", remotePartition.Partition).
				Stringer("kind", remotePartition.Kind).
				Uint64("row_count", remotePartition.RowCount).
				Msg("snapshot partition mismatch detected")
			m.requestSnapshotPartition(sess, remotePartition)
		}
	}
	return nil
}

func (m *Manager) handleSnapshotChunk(sess *session, envelope *internalproto.Envelope) error {
	if !sess.isReplicationReady() {
		return nil
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	if m.store == nil {
		return nil
	}
	m.mu.Lock()
	if err := m.allowSnapshotTrafficForSessionLocked(sess); err != nil {
		m.mu.Unlock()
		m.logSessionWarn("snapshot_chunk_rejected_by_clock", sess, nil).
			Str("reason", err.Error()).
			Msg("snapshot chunk rejected by clock protection")
		return err
	}
	m.mu.Unlock()
	m.markSnapshotChunkReceived(sess.peerID)

	chunk := envelope.GetSnapshotChunk()
	if chunk == nil {
		return errors.New("snapshot chunk body cannot be empty")
	}
	if chunk.SnapshotVersion != internalproto.SnapshotVersion {
		return fmt.Errorf("unsupported snapshot chunk version %q", chunk.SnapshotVersion)
	}
	if strings.TrimSpace(chunk.Partition) == "" {
		return errors.New("snapshot chunk partition cannot be empty")
	}

	if chunk.Request {
		m.logSessionEvent("snapshot_partition_requested", sess).
			Str("partition", chunk.Partition).
			Stringer("kind", chunk.Kind).
			Msg("snapshot partition request received")
		response, err := m.store.BuildSnapshotChunk(context.Background(), chunk.Partition)
		if err != nil {
			return err
		}
		response.SnapshotVersion = internalproto.SnapshotVersion
		m.markSnapshotChunkSent(sess.peerID)
		m.logSessionDebug("snapshot_chunk_sent", sess).
			Str("partition", response.Partition).
			Stringer("kind", response.Kind).
			Int("row_count", len(response.GetRows())).
			Msg("snapshot chunk sent")
		if sess.conn == nil && m.MeshRuntime() != nil {
			return m.routeMeshSnapshotChunk(context.Background(), sess.peerID, response)
		}
		sess.enqueue(&internalproto.Envelope{
			NodeId: m.cfg.NodeID,
			Body: &internalproto.Envelope_SnapshotChunk{
				SnapshotChunk: response,
			},
		})
		return nil
	}

	defer sess.completeSnapshotRequest(chunk.Partition)
	maxTimestamp, err := store.MaxSnapshotChunkTimestamp(chunk)
	if err != nil {
		return err
	}
	if m.cfg.MaxClockSkewMs > 0 && maxTimestamp != (clock.Timestamp{}) {
		maxAllowedWallTime := m.clock.WallTimeMs() + m.cfg.MaxClockSkewMs
		if maxTimestamp.WallTimeMs > maxAllowedWallTime {
			return fmt.Errorf("snapshot chunk %s max hlc %s exceeds local wall time %d by more than %dms", chunk.Partition, maxTimestamp, m.clock.WallTimeMs(), m.cfg.MaxClockSkewMs)
		}
	}
	if err := m.store.ApplySnapshotChunk(context.Background(), chunk); err != nil {
		return err
	}
	m.logSessionEvent("snapshot_chunk_applied", sess).
		Str("partition", chunk.Partition).
		Stringer("kind", chunk.Kind).
		Int("row_count", len(chunk.GetRows())).
		Msg("snapshot chunk applied")
	m.markSnapshotDigestDirty(sess.peerID, snapshotDigestImmediateAfterRepair)
	return nil
}

func (m *Manager) requestSnapshotPartition(sess *session, partition *internalproto.SnapshotPartitionDigest) {
	if partition == nil {
		return
	}
	if strings.TrimSpace(partition.Partition) == "" {
		return
	}
	m.mu.Lock()
	if err := m.allowSnapshotTrafficForSessionLocked(sess); err != nil {
		m.mu.Unlock()
		m.logSessionWarn("snapshot_partition_request_skipped_by_clock", sess, nil).
			Str("reason", err.Error()).
			Msg("skipping snapshot partition request due to clock protection")
		return
	}
	m.mu.Unlock()
	if !sess.beginSnapshotRequest(partition.Partition) {
		return
	}
	m.markSnapshotChunkSent(sess.peerID)
	m.logSessionEvent("snapshot_partition_requested", sess).
		Str("partition", partition.Partition).
		Stringer("kind", partition.Kind).
		Uint64("row_count", partition.RowCount).
		Msg("requested snapshot partition from peer")
	chunk := &internalproto.SnapshotChunk{
		Partition:       partition.Partition,
		SnapshotVersion: internalproto.SnapshotVersion,
		Kind:            partition.Kind,
		Request:         true,
	}
	if sess.conn == nil && m.MeshRuntime() != nil {
		if err := m.routeMeshSnapshotChunk(context.Background(), sess.peerID, chunk); err != nil {
			m.logSessionWarn("mesh_snapshot_chunk_request_forward_failed", sess, err).
				Msg("failed to forward snapshot chunk request over mesh")
		}
		return
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_SnapshotChunk{
			SnapshotChunk: chunk,
		},
	})
}

func (m *Manager) requestSnapshotRepairForOrigin(sess *session, originNodeID int64) {
	if sess == nil || originNodeID <= 0 {
		return
	}
	m.markSnapshotDigestDirty(sess.peerID, snapshotDigestImmediateAfterRepair)
	partitions := []*internalproto.SnapshotPartitionDigest{
		{
			Partition: store.SnapshotUsersPartition,
			Kind:      internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS,
		},
		{
			Partition: store.SnapshotSubscriptionsPartition,
			Kind:      internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_SUBSCRIPTIONS,
		},
		{
			Partition: store.SnapshotBlacklistsPartition,
			Kind:      internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_BLACKLISTS,
		},
	}
	if sess.remoteMessageWindowSize == m.cfg.MessageWindowSize {
		partitions = append(partitions, &internalproto.SnapshotPartitionDigest{
			Partition: store.MessageSnapshotPartition(originNodeID),
			Kind:      internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_MESSAGES,
		})
	}
	for _, partition := range partitions {
		m.requestSnapshotPartition(sess, partition)
	}
}

func (m *Manager) markSnapshotDigestDirty(peerID int64, immediate bool) {
	if m == nil || peerID <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	peer := m.peers[peerID]
	if peer == nil {
		return
	}
	peer.snapshotDigestDirty = true
	if immediate && snapshotDigestImmediateAfterRepair {
		peer.snapshotDigestImmediate = true
	}
}

func (m *Manager) clearSnapshotDigestDirty(peerID int64) {
	if m == nil || peerID <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	peer := m.peers[peerID]
	if peer == nil {
		return
	}
	peer.snapshotDigestDirty = false
	peer.snapshotDigestImmediate = false
}

func (m *Manager) markSnapshotDigestQueued(peerID int64, queuedAt time.Time) {
	m.markSnapshotActivity(peerID, func(peer *peerState, _ time.Time) {
		peer.lastSnapshotDigestQueuedAt = queuedAt
	})
}

func (m *Manager) flushSnapshotDigestsDue(now time.Time) {
	if m == nil || m.store == nil {
		return
	}

	type candidate struct {
		peerID         int64
		sess           *session
		lastSentAt     time.Time
		lastQueuedAt   time.Time
		dirtyImmediate bool
	}

	m.mu.Lock()
	candidates := make([]candidate, 0, len(m.peers))
	for peerID, peer := range m.peers {
		if peerID <= 0 || peer == nil || !peer.snapshotDigestDirty || peer.active == nil {
			continue
		}
		candidates = append(candidates, candidate{
			peerID:         peerID,
			sess:           peer.active,
			lastSentAt:     peer.lastSnapshotDigestAt,
			lastQueuedAt:   peer.lastSnapshotDigestQueuedAt,
			dirtyImmediate: peer.snapshotDigestImmediate,
		})
	}
	m.mu.Unlock()

	for _, item := range candidates {
		if item.sess == nil || !item.sess.isReplicationReady() || item.sess.hasPendingPulls() || item.sess.pendingSnapshotCount() > 0 || item.sess.isClosed() {
			continue
		}
		if item.sess.snapshotVersion() != internalproto.SnapshotVersion {
			continue
		}
		if !item.dirtyImmediate && !item.lastSentAt.IsZero() && now.Sub(item.lastSentAt) < snapshotDigestMinInterval {
			continue
		}
		if !item.lastQueuedAt.IsZero() && now.Sub(item.lastQueuedAt) < snapshotDigestSweepInterval {
			continue
		}
		m.markSnapshotDigestQueued(item.peerID, now)
		if m.sendSnapshotDigest(item.sess) {
			m.clearSnapshotDigestDirty(item.peerID)
		}
	}
}

func (m *Manager) snapshotProducerNodeIDs() []int64 {
	producers := make([]int64, 0, 1+len(m.peers))
	producers = append(producers, m.cfg.NodeID)
	m.mu.Lock()
	defer m.mu.Unlock()
	for peerNodeID := range m.peers {
		producers = append(producers, peerNodeID)
	}
	return producers
}

func snapshotDigestByPartition(partitions []*internalproto.SnapshotPartitionDigest) map[string]*internalproto.SnapshotPartitionDigest {
	index := make(map[string]*internalproto.SnapshotPartitionDigest, len(partitions))
	for _, partition := range partitions {
		if partition == nil || strings.TrimSpace(partition.Partition) == "" {
			continue
		}
		index[partition.Partition] = partition
	}
	return index
}

func snapshotPartitionDigestEqual(local, remote *internalproto.SnapshotPartitionDigest) bool {
	if local == nil || remote == nil {
		return local == remote
	}
	return local.Kind == remote.Kind &&
		local.RowCount == remote.RowCount &&
		bytes.Equal(local.Hash, remote.Hash)
}
