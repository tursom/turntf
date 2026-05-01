package cluster

import (
	"time"

	"google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

// replicationBatcher 将复制事件按 (peerID, originNodeID) 分组并合并成批次。
// 批次在满足大小限制或达到最大延迟时自动刷新，以减少网络消息数量。
type replicationBatcher struct {
	buckets map[replicationBatchKey]*replicationBatch
}

// replicationBatchKey 是批次的唯一键：每个对等节点和事件原始节点的组合。
type replicationBatchKey struct {
	peerID       int64
	originNodeID int64
}

// replicationBatch 是一个待发送的复制事件批次。
type replicationBatch struct {
	peerID        int64
	originNodeID  int64
	firstQueuedAt time.Time
	totalBytes    int
	entries       []queuedReplicationEvent
}

// queuedReplicationEvent 是批次中的单个待发送事件。
type queuedReplicationEvent struct {
	event      store.Event
	replicated *internalproto.ReplicatedEvent
	size       int
}

// flushedReplicationBatch 是一个已刷新的批次，准备好发送到传输层。
type flushedReplicationBatch struct {
	peerID       int64
	originNodeID int64
	sequence     uint64
	sentAtHLC    string
	events       []*internalproto.ReplicatedEvent
}

// newReplicationBatcher 创建一个新的复制批次管理器。
func newReplicationBatcher() *replicationBatcher {
	return &replicationBatcher{
		buckets: make(map[replicationBatchKey]*replicationBatch),
	}
}

// enqueue 将一个事件放入对应批次的队列。
// 如果批次达到大小限制（maxBatchEvents个事件或maxBatchBytes字节），
// 则立即刷新并返回该批次。
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
	// 达到批次大小限制 → 立即刷新
	if len(batch.entries) >= maxBatchEvents || batch.totalBytes >= maxBatchBytes {
		return []*flushedReplicationBatch{b.flushKey(key)}
	}
	return nil
}

// flushDue 刷新所有超过maxBatchDelay的批次。
// 由定时器周期性调用，防止事件在批次中滞留过久。
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

// flushAll 刷新所有未完成的批次（在关闭时调用）。
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

// flushKey 刷新指定键的批次并返回已刷新的事件列表。
// 序列号取批次中最后一个事件的序列号，HLC时间戳也取最后一个事件的HLC。
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
