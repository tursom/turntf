package cluster

import (
	"time"

	"google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type replicationBatcher struct {
	buckets map[replicationBatchKey]*replicationBatch
}

type replicationBatchKey struct {
	peerID       int64
	originNodeID int64
}

type replicationBatch struct {
	peerID        int64
	originNodeID  int64
	firstQueuedAt time.Time
	totalBytes    int
	entries       []queuedReplicationEvent
}

type queuedReplicationEvent struct {
	event      store.Event
	replicated *internalproto.ReplicatedEvent
	size       int
}

type flushedReplicationBatch struct {
	peerID       int64
	originNodeID int64
	sequence     uint64
	sentAtHLC    string
	events       []*internalproto.ReplicatedEvent
}

func newReplicationBatcher() *replicationBatcher {
	return &replicationBatcher{
		buckets: make(map[replicationBatchKey]*replicationBatch),
	}
}

func (b *replicationBatcher) enqueue(peerID int64, event store.Event, queuedAt time.Time) []*flushedReplicationBatch {
	if b == nil || peerID <= 0 || event.OriginNodeID <= 0 {
		return nil
	}

	replicated := store.ToReplicatedEvent(event)
	entry := queuedReplicationEvent{
		event:      event,
		replicated: replicated,
	}
	if replicated != nil {
		entry.size = proto.Size(replicated)
	}

	key := replicationBatchKey{peerID: peerID, originNodeID: event.OriginNodeID}
	batch := b.buckets[key]
	if batch == nil {
		batch = &replicationBatch{
			peerID:        peerID,
			originNodeID:  event.OriginNodeID,
			firstQueuedAt: queuedAt,
		}
		b.buckets[key] = batch
	}

	batch.entries = append(batch.entries, entry)
	batch.totalBytes += entry.size
	if len(batch.entries) >= maxBatchEvents || batch.totalBytes >= maxBatchBytes {
		return []*flushedReplicationBatch{b.flushKey(key)}
	}
	return nil
}

func (b *replicationBatcher) flushDue(now time.Time) []*flushedReplicationBatch {
	if b == nil {
		return nil
	}
	flushes := make([]*flushedReplicationBatch, 0)
	for key, batch := range b.buckets {
		if batch == nil || len(batch.entries) == 0 {
			delete(b.buckets, key)
			continue
		}
		if now.Sub(batch.firstQueuedAt) < maxBatchDelay {
			continue
		}
		flushes = append(flushes, b.flushKey(key))
	}
	return flushes
}

func (b *replicationBatcher) flushAll() []*flushedReplicationBatch {
	if b == nil {
		return nil
	}
	flushes := make([]*flushedReplicationBatch, 0, len(b.buckets))
	for key := range b.buckets {
		flushes = append(flushes, b.flushKey(key))
	}
	return flushes
}

func (b *replicationBatcher) flushKey(key replicationBatchKey) *flushedReplicationBatch {
	batch := b.buckets[key]
	delete(b.buckets, key)
	if batch == nil || len(batch.entries) == 0 {
		return nil
	}

	last := batch.entries[len(batch.entries)-1].event
	events := make([]*internalproto.ReplicatedEvent, 0, len(batch.entries))
	for _, entry := range batch.entries {
		if entry.replicated != nil {
			events = append(events, entry.replicated)
		}
	}
	return &flushedReplicationBatch{
		peerID:       batch.peerID,
		originNodeID: batch.originNodeID,
		sequence:     uint64(last.Sequence),
		sentAtHLC:    last.HLC.String(),
		events:       events,
	}
}
