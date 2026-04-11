package cluster

import (
	"context"
	"time"

	"notifier/internal/app"
)

func (m *Manager) Status(context.Context) (app.ClusterStatus, error) {
	if m == nil {
		return app.ClusterStatus{WriteGateReady: true}, nil
	}

	type peerSnapshot struct {
		status app.ClusterPeerStatus
		active *session
	}

	m.mu.Lock()
	peers := make([]peerSnapshot, 0, len(m.cfg.Peers))
	for _, configured := range m.cfg.Peers {
		peer := m.peers[configured.NodeID]
		item := app.ClusterPeerStatus{
			NodeID:                   configured.NodeID,
			ConfiguredURL:            configured.URL,
			SnapshotDigestsSentTotal: 0,
			SnapshotDigestsRecvTotal: 0,
			SnapshotChunksSentTotal:  0,
			SnapshotChunksRecvTotal:  0,
		}
		var active *session
		if peer != nil {
			active = peer.active
			item.Connected = peer.active != nil
			item.LastAck = peer.lastAck
			item.ClockOffsetMs = peer.clockOffsetMs
			item.LastClockSync = timePointer(peer.lastClockSync)
			item.SnapshotDigestsSentTotal = peer.snapshotDigestsSent
			item.SnapshotDigestsRecvTotal = peer.snapshotDigestsReceived
			item.SnapshotChunksSentTotal = peer.snapshotChunksSent
			item.SnapshotChunksRecvTotal = peer.snapshotChunksReceived
			item.LastSnapshotDigestAt = timePointer(peer.lastSnapshotDigestAt)
			item.LastSnapshotChunkAt = timePointer(peer.lastSnapshotChunkAt)
		}
		peers = append(peers, peerSnapshot{status: item, active: active})
	}
	writeGateReady := m.hasWritableClockSyncLocked()
	m.mu.Unlock()

	status := app.ClusterStatus{
		NodeID:            m.cfg.NodeID,
		MessageWindowSize: m.cfg.MessageWindowSize,
		WriteGateReady:    writeGateReady,
		Peers:             make([]app.ClusterPeerStatus, 0, len(peers)),
	}
	for _, peer := range peers {
		item := peer.status
		if peer.active != nil {
			item.SessionDirection = peer.active.direction()
			item.RemoteLastSequence = peer.active.remoteSequence()
			item.PendingCatchup = peer.active.hasPendingPull()
			item.PendingSnapshotPartitions = peer.active.pendingSnapshotCount()
			item.RemoteSnapshotVersion = peer.active.snapshotVersion()
			item.RemoteMessageWindowSize = peer.active.messageWindowSize()
		}
		status.Peers = append(status.Peers, item)
	}
	return status, nil
}

func (m *Manager) ConfiguredPeerNodeIDs() []int64 {
	if m == nil {
		return nil
	}
	ids := make([]int64, 0, len(m.cfg.Peers))
	for _, peer := range m.cfg.Peers {
		ids = append(ids, peer.NodeID)
	}
	return ids
}

func (m *Manager) markSnapshotDigestSent(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotDigestsSent++
		peer.lastSnapshotDigestAt = now
	})
}

func (m *Manager) markSnapshotDigestReceived(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotDigestsReceived++
		peer.lastSnapshotDigestAt = now
	})
}

func (m *Manager) markSnapshotChunkSent(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotChunksSent++
		peer.lastSnapshotChunkAt = now
	})
}

func (m *Manager) markSnapshotChunkReceived(peerID int64) {
	m.markSnapshotActivity(peerID, func(peer *peerState, now time.Time) {
		peer.snapshotChunksReceived++
		peer.lastSnapshotChunkAt = now
	})
}

func (m *Manager) markSnapshotActivity(peerID int64, update func(*peerState, time.Time)) {
	if m == nil || update == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[peerID]
	if !ok {
		return
	}
	update(peer, time.Now().UTC())
}

func (s *session) direction() string {
	if s.outbound {
		return "outbound"
	}
	return "inbound"
}

func (s *session) pendingSnapshotCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingSnapshotParts)
}

func (s *session) snapshotVersion() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteSnapshotVersion
}

func (s *session) messageWindowSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteMessageWindowSize
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copied := value.UTC()
	return &copied
}
