package app

import "time"

type ClusterStatus struct {
	NodeID            int64
	MessageWindowSize int
	WriteGateReady    bool
	Peers             []ClusterPeerStatus
}

type ClusterPeerStatus struct {
	NodeID                    int64
	ConfiguredURL             string
	Connected                 bool
	SessionDirection          string
	LastAck                   uint64
	RemoteLastSequence        uint64
	PendingCatchup            bool
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
