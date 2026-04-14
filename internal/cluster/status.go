package cluster

import (
	"context"
	"sort"
	"time"

	"github.com/tursom/turntf/internal/app"
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
	peers := make([]peerSnapshot, 0, len(m.configuredPeers)+len(m.peers))
	transitions := make([]app.ClockStateTransition, 0, len(m.clockStateTransitions))
	seen := make(map[int64]struct{}, len(m.peers))
	for _, configured := range m.configuredPeers {
		item := app.ClusterPeerStatus{
			NodeID:                   configured.nodeID,
			ConfiguredURL:            configured.URL,
			SnapshotDigestsSentTotal: 0,
			SnapshotDigestsRecvTotal: 0,
			SnapshotChunksSentTotal:  0,
			SnapshotChunksRecvTotal:  0,
		}
		var active *session
		if configured.nodeID > 0 {
			seen[configured.nodeID] = struct{}{}
		}
		if peer := m.peers[configured.nodeID]; peer != nil {
			active = peer.active
			item.Connected = peer.active != nil
			item.ClockState = string(peer.clockState)
			item.ClockOffsetMs = peer.clockOffsetMs
			item.ClockUncertaintyMs = peer.clockUncertaintyMs
			item.ClockFailures = peer.clockFailures
			item.LastClockError = peer.clockLastError
			item.LastClockSync = timePointer(peer.lastClockSync)
			item.LastCredibleClockSync = timePointer(peer.lastCredibleClockSync)
			item.TrustedForOffset = peer.trustedSession != nil
			item.SnapshotDigestsSentTotal = peer.snapshotDigestsSent
			item.SnapshotDigestsRecvTotal = peer.snapshotDigestsReceived
			item.SnapshotChunksSentTotal = peer.snapshotChunksSent
			item.SnapshotChunksRecvTotal = peer.snapshotChunksReceived
			item.LastSnapshotDigestAt = timePointer(peer.lastSnapshotDigestAt)
			item.LastSnapshotChunkAt = timePointer(peer.lastSnapshotChunkAt)
		}
		peers = append(peers, peerSnapshot{status: item, active: active})
	}
	for nodeID, peer := range m.peers {
		if _, ok := seen[nodeID]; ok {
			continue
		}
		item := app.ClusterPeerStatus{
			NodeID:                   nodeID,
			SnapshotDigestsSentTotal: peer.snapshotDigestsSent,
			SnapshotDigestsRecvTotal: peer.snapshotDigestsReceived,
			SnapshotChunksSentTotal:  peer.snapshotChunksSent,
			SnapshotChunksRecvTotal:  peer.snapshotChunksReceived,
			LastSnapshotDigestAt:     timePointer(peer.lastSnapshotDigestAt),
			LastSnapshotChunkAt:      timePointer(peer.lastSnapshotChunkAt),
			Connected:                peer.active != nil,
			ClockState:               string(peer.clockState),
			ClockOffsetMs:            peer.clockOffsetMs,
			ClockUncertaintyMs:       peer.clockUncertaintyMs,
			ClockFailures:            peer.clockFailures,
			LastClockError:           peer.clockLastError,
			LastClockSync:            timePointer(peer.lastClockSync),
			LastCredibleClockSync:    timePointer(peer.lastCredibleClockSync),
			TrustedForOffset:         peer.trustedSession != nil,
		}
		peers = append(peers, peerSnapshot{status: item, active: peer.active})
	}
	m.refreshNodeClockStateLocked()
	writeGateReady := m.hasWritableClockSyncLocked()
	clockState := m.clockState
	clockReason := m.clockReason
	lastTrustedClockSync := timePointer(m.lastTrustedClockSync)
	for key, total := range m.clockStateTransitions {
		transitions = append(transitions, app.ClockStateTransition{
			FromState: key.FromState,
			ToState:   key.ToState,
			Reason:    key.Reason,
			Total:     total,
		})
	}
	m.mu.Unlock()

	status := app.ClusterStatus{
		NodeID:               m.cfg.NodeID,
		MessageWindowSize:    m.cfg.MessageWindowSize,
		WriteGateReady:       writeGateReady,
		ClockState:           string(clockState),
		ClockReason:          clockReason,
		LastTrustedClockSync: lastTrustedClockSync,
		ClockTransitions:     transitions,
		Peers:                make([]app.ClusterPeerStatus, 0, len(peers)),
	}
	for _, peer := range peers {
		item := peer.status
		if peer.active != nil {
			item.SessionDirection = peer.active.direction()
			item.Origins = peer.active.originStatuses()
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
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make([]int64, 0, len(m.configuredPeers))
	for _, peer := range m.configuredPeers {
		if peer.nodeID <= 0 {
			continue
		}
		ids = append(ids, peer.nodeID)
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

func (s *session) originStatuses() []app.ClusterPeerOriginStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	origins := make([]app.ClusterPeerOriginStatus, 0, len(s.remoteOriginProgress))
	for originNodeID, lastEventID := range s.remoteOriginProgress {
		_, pending := s.pendingPulls[originNodeID]
		origins = append(origins, app.ClusterPeerOriginStatus{
			OriginNodeID:      originNodeID,
			RemoteLastEventID: lastEventID,
			PendingCatchup:    pending,
		})
	}
	sort.Slice(origins, func(i, j int) bool {
		return origins[i].OriginNodeID < origins[j].OriginNodeID
	})
	return origins
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copied := value.UTC()
	return &copied
}
