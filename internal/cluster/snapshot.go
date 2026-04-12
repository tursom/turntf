package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"

	internalproto "notifier/internal/proto"
	"notifier/internal/store"
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

func (m *Manager) sendSnapshotDigest(sess *session) {
	if m.store == nil || sess == nil || sess.peerID == 0 {
		return
	}
	if !sess.isReplicationReady() || sess.hasPendingPull() || sess.isClosed() {
		return
	}
	if sess.remoteSnapshotVersion != internalproto.SnapshotVersion {
		return
	}

	envelope, err := m.buildSnapshotDigestEnvelope()
	if err != nil {
		log.Warn().Err(err).Str("component", "cluster").Int64("peer", sess.peerID).Str("event", "build_snapshot_digest_failed").Msg("build snapshot digest failed")
		return
	}
	m.markSnapshotDigestSent(sess.peerID)
	sess.enqueue(envelope)
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
	m.markSnapshotDigestReceived(sess.peerID)

	digest := envelope.GetSnapshotDigest()
	if digest == nil {
		return errors.New("snapshot digest body cannot be empty")
	}
	if digest.SnapshotVersion != internalproto.SnapshotVersion {
		return fmt.Errorf("unsupported snapshot digest version %q", digest.SnapshotVersion)
	}

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
		m.requestSnapshotPartition(sess, remoteUsers)
		return nil
	}
	remoteSubscriptions, ok := remote[store.SnapshotSubscriptionsPartition]
	if !ok {
		return fmt.Errorf("snapshot digest missing %s partition", store.SnapshotSubscriptionsPartition)
	}
	if !snapshotPartitionDigestEqual(local[store.SnapshotSubscriptionsPartition], remoteSubscriptions) {
		m.requestSnapshotPartition(sess, remoteSubscriptions)
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
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)

	for _, partition := range partitions {
		remotePartition := remote[partition]
		if remotePartition.Kind != internalproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_MESSAGES {
			return fmt.Errorf("snapshot digest partition %s has unsupported kind %s", partition, remotePartition.Kind)
		}
		if !snapshotPartitionDigestEqual(local[partition], remotePartition) {
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
		response, err := m.store.BuildSnapshotChunk(context.Background(), chunk.Partition)
		if err != nil {
			return err
		}
		response.SnapshotVersion = internalproto.SnapshotVersion
		m.markSnapshotChunkSent(sess.peerID)
		sess.enqueue(&internalproto.Envelope{
			NodeId: m.cfg.NodeID,
			Body: &internalproto.Envelope_SnapshotChunk{
				SnapshotChunk: response,
			},
		})
		return nil
	}

	defer sess.completeSnapshotRequest(chunk.Partition)
	if err := m.store.ApplySnapshotChunk(context.Background(), chunk); err != nil {
		return err
	}
	m.sendSnapshotDigest(sess)
	return nil
}

func (m *Manager) requestSnapshotPartition(sess *session, partition *internalproto.SnapshotPartitionDigest) {
	if partition == nil {
		return
	}
	if strings.TrimSpace(partition.Partition) == "" {
		return
	}
	if !sess.beginSnapshotRequest(partition.Partition) {
		return
	}
	m.markSnapshotChunkSent(sess.peerID)
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_SnapshotChunk{
			SnapshotChunk: &internalproto.SnapshotChunk{
				Partition:       partition.Partition,
				SnapshotVersion: internalproto.SnapshotVersion,
				Kind:            partition.Kind,
				Request:         true,
			},
		},
	})
}

func (m *Manager) snapshotProducerNodeIDs() []int64 {
	producers := make([]int64, 0, 1+len(m.cfg.Peers))
	producers = append(producers, m.cfg.NodeID)
	for _, peer := range m.cfg.Peers {
		producers = append(producers, peer.NodeID)
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
