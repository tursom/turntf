package store

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// pebbleLocalMessageBatchMaxOps 单次批处理的最大操作数。
// 限制每个批次的请求数量，防止单个批次占用过多内存或导致事务过大。
const pebbleLocalMessageBatchMaxOps = 128

// pebbleLocalMessageWriteRequest 本地消息写入请求。
// 通过 channel 提交给后台循环，包含消息参数和异步返回结果的通道。
type pebbleLocalMessageWriteRequest struct {
	params   CreateMessageParams
	response chan pebbleLocalMessageWriteResult
}

// pebbleLocalMessageWriteResult 本地消息写入结果。
// 包含写入后的消息对象、事件对象和可能的错误。
type pebbleLocalMessageWriteResult struct {
	message Message
	event   Event
	err     error
}

// pebbleSequenceReservation 序列号预留信息。
// 在批处理期间为某个用户的某条消息预留的序列号，
// 后续提交批次时将序列号持久化到 Pebble 并更新内存缓存。
type pebbleSequenceReservation struct {
	// cacheKey 缓存键，用于更新内存中缓存的已提交序列号
	cacheKey string
	// key 序列号在 Pebble 中的存储键
	key []byte
	// next 预留的下一个可用序列号（当前消息使用 next-1）
	next int64
}

// pebbleLocalMessageBatchStats 批处理统计计数器。
// 记录不同同步模式（NoSync / ForceSync）下提交的批次数量，用于监控和调试。
type pebbleLocalMessageBatchStats struct {
	noSyncBatches    atomic.Uint64
	forceSyncBatches atomic.Uint64
}

// pebbleLocalMessageBatchStatsSnapshot 批处理统计的快照（线程安全，可导出字段）。
type pebbleLocalMessageBatchStatsSnapshot struct {
	NoSyncBatches    uint64
	ForceSyncBatches uint64
}

// record 记录一次批次提交的同步模式（用于统计）。
func (s *pebbleLocalMessageBatchStats) record(mode PebbleMessageSyncMode) {
	switch mode {
	case PebbleMessageSyncModeForceSync:
		s.forceSyncBatches.Add(1)
	default:
		s.noSyncBatches.Add(1)
	}
}

// snapshot 返回批处理统计的线程安全快照。
func (s *pebbleLocalMessageBatchStats) snapshot() pebbleLocalMessageBatchStatsSnapshot {
	return pebbleLocalMessageBatchStatsSnapshot{
		NoSyncBatches:    s.noSyncBatches.Load(),
		ForceSyncBatches: s.forceSyncBatches.Load(),
	}
}

// startLocalMessageLoop 启动本地消息写入的后台协程。
// 采用单协程串行处理模型，所有写入请求通过 channel 提交，确保写入顺序一致性。
// 首次调用时初始化相关 channel 并启动后台循环；后续调用为幂等操作，不会重复启动。
func (b *pebbleStoreBackend) startLocalMessageLoop() {
	if b == nil {
		return
	}
	b.localMessageMu.Lock()
	defer b.localMessageMu.Unlock()

	if b.localMessageRequests != nil {
		return
	}
	b.localMessageRequests = make(chan pebbleLocalMessageWriteRequest, pebbleLocalMessageBatchMaxOps*8)
	b.localMessageCloseCh = make(chan chan error, 1)
	b.localMessageDone = make(chan struct{})
	go b.runLocalMessageLoop()
}

// submitLocalMessage 提交一条本地消息写入请求并等待处理结果。
// 通过 channel 将请求投递给后台协程，阻塞等待异步返回 (Message, Event, error)。
// 支持上下文取消：如果 ctx 已取消则直接返回错误而不投递请求。
func (b *pebbleStoreBackend) submitLocalMessage(ctx context.Context, params CreateMessageParams) (Message, Event, error) {
	if b == nil {
		return Message{}, Event{}, fmt.Errorf("pebble local message loop is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return Message{}, Event{}, err
	}

	response := make(chan pebbleLocalMessageWriteResult, 1)

	b.localMessageMu.Lock()
	if b.localMessageClosed || b.localMessageRequests == nil {
		b.localMessageMu.Unlock()
		return Message{}, Event{}, fmt.Errorf("pebble local message loop is closed")
	}
	b.localMessageMu.Unlock()

	b.localMessageRequests <- pebbleLocalMessageWriteRequest{
		params:   params,
		response: response,
	}
	result := <-response
	return result.message, result.event, result.err
}

// closeLocalMessageLoop 优雅关闭本地消息写入循环。
// 向后台协程发送关闭信号，等待已提交的所有请求处理完毕后返回。
// 幂等操作：多次调用只会执行一次关闭逻辑。
func (b *pebbleStoreBackend) closeLocalMessageLoop() error {
	if b == nil {
		return nil
	}

	b.localMessageMu.Lock()
	if b.localMessageRequests == nil {
		b.localMessageMu.Unlock()
		return nil
	}
	if b.localMessageClosed {
		b.localMessageMu.Unlock()
		<-b.localMessageDone
		return nil
	}
	b.localMessageClosed = true
	b.localMessageMu.Unlock()

	response := make(chan error, 1)
	b.localMessageCloseCh <- response
	err := <-response
	<-b.localMessageDone
	return err
}

// runLocalMessageLoop 本地消息写入后台协程的主循环。
//
// 工作流程：
//  1. 从 channel 接收第一个写入请求
//  2. 调用 runtime.Gosched() 让出 P，使更多写入请求有机会到达
//  3. 无阻塞 drain 所有已到达的请求，构成一个处理批次
//  4. 按 pebbleLocalMessageBatchMaxOps 切分批次，保证每批不超过上限
//  5. 对每个批次内的请求，按同步模式分组（contiguousLocalMessageSyncModePrefix）
//  6. 逐组调用 processLocalMessageBatch 处理
//  7. 处理完后检查 channel 是否有新到达的请求，若有则继续
//  8. 收到关闭信号时退出循环
//
// 这种设计通过批量处理提高 Pebble 写入吞吐量，同时保证同一个同步模式的请求在同一批次中处理。
func (b *pebbleStoreBackend) runLocalMessageLoop() {
	defer close(b.localMessageDone)

	drainQueued := func() []pebbleLocalMessageWriteRequest {
		pending := make([]pebbleLocalMessageWriteRequest, 0, pebbleLocalMessageBatchMaxOps)
		for len(pending) < pebbleLocalMessageBatchMaxOps {
			select {
			case req := <-b.localMessageRequests:
				pending = append(pending, req)
			default:
				return pending
			}
		}
		return pending
	}

	for {
		select {
		case req := <-b.localMessageRequests:
			pending := []pebbleLocalMessageWriteRequest{req}
			runtime.Gosched()
			pending = append(pending, drainQueued()...)
			for len(pending) > 0 {
				limit := len(pending)
				if limit > pebbleLocalMessageBatchMaxOps {
					limit = pebbleLocalMessageBatchMaxOps
				}
				chunk := pending[:limit]
				for len(chunk) > 0 {
					segmentEnd := contiguousLocalMessageSyncModePrefix(chunk)
					b.processLocalMessageBatch(chunk[:segmentEnd])
					chunk = chunk[segmentEnd:]
				}
				pending = pending[limit:]
				if len(pending) == 0 {
					pending = append(pending, drainQueued()...)
				}
			}
		case response := <-b.localMessageCloseCh:
			response <- nil
			return
		}
	}
}

// processLocalMessageBatch 处理一批本地消息写入请求。
//
// 这是消息写入的核心方法，在 eventLog.mu 持有锁的情况下执行以下操作：
//
//  1. 获取当前事件日志序列号，为每个请求分配递增的 nextEventSequence
//  2. 加载或初始化每个接收用户的消息状态（userStates）
//  3. 调用 messageSequences.LoadNextSequence 为每个用户预留序列号
//  4. 构造 Message 和 Event 对象，设置 HLC 时间戳
//  5. 将事件写入事件日志（eventLog writeStoredEventToBatch）
//  6. 调用 projection.prepareMessageWrite 写入消息的主键索引、用户索引、生产者索引
//  7. 判断是否超过 trimming 阈值（普通 trim 或 hard trim）
//  8. 批次提交前，将最后一条事件序列号和所有预留序列号写入 Pebble batch
//  9. 调用 commitLocalMessageBatch 提交批处理
//
// 10. 提交成功后更新事件日志的内存缓存和序列号内存缓存
// 11. 对需要 hard trim 的用户立即执行 trim，对普通 trim 的调度给后台 worker
// 12. 将结果通过 response channel 返回给各个调用方
//
// 关键设计：
//   - 同一批次内所有请求必须具有相同的 PebbleMessageSyncMode
//   - 用户级别的锁在 projection.lockUsers 中获取，防止并发写入同一用户的消息
//   - 序列号通过预留+提交两阶段完成：先在内存中分配（预留），批次成功提交后更新 Pebble
func (b *pebbleStoreBackend) processLocalMessageBatch(requests []pebbleLocalMessageWriteRequest) {
	if len(requests) == 0 {
		return
	}
	if b == nil || b.db == nil || b.eventLog == nil || b.messageSequences == nil || b.messageProjectionRepo == nil {
		respondLocalMessageBatchError(requests, fmt.Errorf("pebble local message loop is not initialized"))
		return
	}

	projection := b.messageProjectionRepo
	unlockUsers := projection.lockUsers(uniqueMessageRecipients(requests))
	defer unlockUsers()

	ctx := context.Background()
	userStates := make(map[UserKey]pebbleMessageUserState)
	sequenceReservations := make(map[UserKey]pebbleSequenceReservation)
	committedNextByCacheKey := make(map[string]int64)
	dirtyUsers := make(map[UserKey]struct{})
	hardTrimUsers := make(map[UserKey]struct{})
	results := make([]pebbleLocalMessageWriteResult, len(requests))

	batch := b.db.NewBatch()
	batchOwned := true
	defer func() {
		if batchOwned {
			_ = batch.Close()
		}
	}()

	b.eventLog.mu.Lock()
	defer b.eventLog.mu.Unlock()

	currentEventSequence, err := b.eventLog.lastEventSequenceLocked()
	if err != nil {
		respondLocalMessageBatchError(requests, err)
		return
	}

	nextEventSequence := currentEventSequence + 1
	windowSize := normalizeMessageWindowSize(projection.messageWindowSize)
	trimThreshold := int64(pebbleMessageTrimThreshold(windowSize))
	hardTrimThreshold := int64(pebbleMessageTrimHardThreshold(windowSize))
	syncMode := requests[0].params.PebbleMessageSyncMode
	forceSync := syncMode == PebbleMessageSyncModeForceSync

	for i, request := range requests {
		if request.params.PebbleMessageSyncMode != syncMode {
			respondLocalMessageBatchError(requests, fmt.Errorf("%w: local pebble message batch contains mixed sync modes", ErrInvalidInput))
			return
		}
		key := request.params.UserKey

		state, ok := userStates[key]
		if !ok {
			state, err = projection.messageUserStateLocked(ctx, key)
			if err != nil {
				respondLocalMessageBatchError(requests, err)
				return
			}
		}

		reservation, ok := sequenceReservations[key]
		if !ok {
			cacheKey, sequenceKey, next, err := b.messageSequences.LoadNextSequence(ctx, key, b.eventLog.nodeID)
			if err != nil {
				respondLocalMessageBatchError(requests, err)
				return
			}
			reservation = pebbleSequenceReservation{
				cacheKey: cacheKey,
				key:      sequenceKey,
				next:     next,
			}
		}

		now := b.eventLog.clock.Now()
		message := Message{
			Recipient: key,
			NodeID:    b.eventLog.nodeID,
			Seq:       reservation.next,
			Sender:    request.params.Sender,
			Body:      append([]byte(nil), request.params.Body...),
			CreatedAt: now,
		}
		reservation.next++

		event := Event{
			Sequence:        nextEventSequence,
			EventID:         b.eventLog.ids.Next(),
			EventType:       EventTypeMessageCreated,
			Aggregate:       "message",
			AggregateNodeID: message.NodeID,
			AggregateID:     message.Seq,
			HLC:             now,
			OriginNodeID:    b.eventLog.nodeID,
			Body:            messageCreatedProtoFromMessage(message),
		}
		value, err := eventLogValue(event)
		if err != nil {
			respondLocalMessageBatchError(requests, err)
			return
		}
		if err := b.eventLog.writeStoredEventToBatch(batch, event, value); err != nil {
			respondLocalMessageBatchError(requests, err)
			return
		}

		state, _, err = projection.prepareMessageWrite(batch, message, state)
		if err != nil {
			respondLocalMessageBatchError(requests, err)
			return
		}

		userStates[key] = state
		sequenceReservations[key] = reservation
		committedNextByCacheKey[reservation.cacheKey] = reservation.next

		switch {
		case state.StoredCount > hardTrimThreshold:
			hardTrimUsers[key] = struct{}{}
		case state.StoredCount > trimThreshold:
			dirtyUsers[key] = struct{}{}
		}

		results[i] = pebbleLocalMessageWriteResult{
			message: message,
			event:   event,
		}
		nextEventSequence++
	}

	if len(results) > 0 {
		if err := batch.Set(pebbleEventSequenceKey, encodeInt64(results[len(results)-1].event.Sequence), nil); err != nil {
			respondLocalMessageBatchError(requests, fmt.Errorf("write event sequence meta: %w", err))
			return
		}
	}
	for _, reservation := range sequenceReservations {
		if err := batch.Set(reservation.key, encodeInt64(reservation.next), nil); err != nil {
			respondLocalMessageBatchError(requests, fmt.Errorf("write pebble message sequence: %w", err))
			return
		}
	}

	closedByCommit, err := b.commitLocalMessageBatch(batch, forceSync)
	if err != nil {
		respondLocalMessageBatchError(requests, fmt.Errorf("commit local pebble message batch: %w", err))
		return
	}
	if closedByCommit {
		batchOwned = false
	}
	b.localMessageStats.record(syncMode)

	b.eventLog.lastSequence = results[len(results)-1].event.Sequence
	b.eventLog.sequenceLoaded = true
	b.messageSequences.StoreCommittedNextByCacheKey(committedNextByCacheKey)

	for _, key := range sortedUserKeys(hardTrimUsers) {
		if err := projection.trimMessagesForUserLocked(ctx, key, forceSync); err != nil {
			dirtyUsers[key] = struct{}{}
			continue
		}
		delete(dirtyUsers, key)
	}

	for key := range dirtyUsers {
		projection.scheduleTrim(key)
	}

	for i, request := range requests {
		request.response <- results[i]
	}
}

// commitLocalMessageBatch 提交批处理到 Pebble。
// forceSync 为 true 时使用 pebble.Sync（等待数据落盘），否则使用 pebble.NoSync。
// 返回的第一个值表示 batch 是否已被外部关闭（当前实现始终返回 false）。
func (b *pebbleStoreBackend) commitLocalMessageBatch(batch *pebble.Batch, forceSync bool) (bool, error) {
	if batch == nil {
		return false, fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
	}
	commitOptions := pebble.NoSync
	if forceSync {
		commitOptions = pebble.Sync
	}
	return false, batch.Commit(commitOptions)
}

// respondLocalMessageBatchError 向批次中所有请求发送相同的错误响应。
// 用于批处理过程中发生不可恢复错误时的快速失败处理。
func respondLocalMessageBatchError(requests []pebbleLocalMessageWriteRequest, err error) {
	for _, request := range requests {
		request.response <- pebbleLocalMessageWriteResult{err: err}
	}
}

// contiguousLocalMessageSyncModePrefix 查找请求切片前缀中同步模式连续相同的长度。
// 返回从索引 0 开始的、与第一个请求具有相同 PebbleMessageSyncMode 的连续请求个数。
// 用于在批处理中将不同同步模式的请求分组处理，因为同一批次中所有请求必须具有相同的同步模式。
func contiguousLocalMessageSyncModePrefix(requests []pebbleLocalMessageWriteRequest) int {
	if len(requests) == 0 {
		return 0
	}
	mode := requests[0].params.PebbleMessageSyncMode
	for i := 1; i < len(requests); i++ {
		if requests[i].params.PebbleMessageSyncMode != mode {
			return i
		}
	}
	return len(requests)
}

// uniqueMessageRecipients 提取请求列表中所有不重复的消息接收者（UserKey）。
// 返回结果按 NodeID 和 UserID 排序，保证确定性的锁获取顺序以避免死锁。
func uniqueMessageRecipients(requests []pebbleLocalMessageWriteRequest) []UserKey {
	seen := make(map[UserKey]struct{}, len(requests))
	keys := make([]UserKey, 0, len(requests))
	for _, request := range requests {
		if _, ok := seen[request.params.UserKey]; ok {
			continue
		}
		seen[request.params.UserKey] = struct{}{}
		keys = append(keys, request.params.UserKey)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].NodeID != keys[j].NodeID {
			return keys[i].NodeID < keys[j].NodeID
		}
		return keys[i].UserID < keys[j].UserID
	})
	return keys
}
