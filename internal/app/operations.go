package app

import "time"

type ClusterStatus struct {
	NodeID            int64
	MessageWindowSize int
	WriteGateReady    bool
	Peers             []ClusterPeerStatus
}

type LoggedInUserSummary struct {
	NodeID   int64
	UserID   int64
	Username string
}

type ClusterPeerOriginStatus struct {
	OriginNodeID      int64
	RemoteLastEventID uint64
	PendingCatchup    bool
}

type ClusterPeerStatus struct {
	NodeID                    int64
	ConfiguredURL             string
	Connected                 bool
	SessionDirection          string
	Origins                   []ClusterPeerOriginStatus
	PendingSnapshotPartitions int
	RemoteSnapshotVersion     string
	RemoteMessageWindowSize   int
	ClockOffsetMs             int64
	LastClockSync             *time.Time
	SnapshotDigestsSentTotal  uint64
	SnapshotDigestsRecvTotal  uint64
	SnapshotChunksSentTotal   uint64
	SnapshotChunksRecvTotal   uint64
	LastSnapshotDigestAt      *time.Time
	LastSnapshotChunkAt       *time.Time
}
