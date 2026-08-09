package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	clusterproto "github.com/tursom/turntf/internal/proto"
)

// pebbleEventSequenceKey 是存储全局事件序列号（自增 ID）的元数据键。
var pebbleEventSequenceKey = []byte{metaEventSequenceTag}

const (
	// pebbleMessageIndexRefMarker 是索引值中使用引用的标记字节。
	// 索引值可以是内联完整的消息数据，也可以是通过此标记引用主键的 33 字节引用。
	pebbleMessageIndexRefMarker byte = 0
	// pebbleMessageTrimSlack 是消息裁剪的松弛空间。
	// 当用户消息数超过 windowSize + slack 时才会触发硬阈值裁剪。
	pebbleMessageTrimSlack int = 32
)

// pebbleEventLogRepository 是 Pebble 后端的事件日志仓库。
//
// 事件日志是仅追加的日志，每条事件被分配一个全局递增的序列号（sequence）。
// 维护两个索引：
//   - 按序列号索引（eventSeqTag + sequence）：主存储，按写入顺序排列
//   - 按来源索引（eventOriginTag + originNodeID + eventID）：用于按来源节点查询
//
// 并发安全：本地追加和复制追加通过 mu 互斥锁序列化，保证序列号分配和索引写入的原子性。
type pebbleEventLogRepository struct {
	// db 是底层的 Pebble 数据库实例
	db *pebble.DB
	// writes 是写入协调器，用于组提交优化
	writes *pebbleWriteCoordinator
	// ids 是事件 ID 生成器
	ids *clock.IDGenerator
	// nodeID 是本节点的 ID，用于标记本地事件的来源
	nodeID int64
	// clock 是混合逻辑时钟，用于为事件生成时间戳
	clock *clock.Clock
	// mu 保护序列号分配和元数据更新的原子性
	mu sync.Mutex

	// lastSequence 是缓存的最新序列号，避免每次追加都读盘
	lastSequence int64
	// sequenceLoaded 标记 lastSequence 是否已从磁盘加载
	sequenceLoaded bool
}

// pebbleMessageProjectionRepository 是 Pebble 后端的事件消息投影仓库。
//
// 此仓库维护消息的物化视图，支持多种查询索引：
//   - 主键索引（messageIDTag）：按 (Recipient, NodeID, Seq) 主键存储完整消息
//   - 用户索引（messageUserTag）：按收件人 + 时间戳排序，用于列出用户消息
//   - 生产者索引（messageProducerTag）：按发送节点 + 时间戳排序，用于快照构建
//   - 会话索引（messageSessionTag）：按会话标识 + 时间戳排序，用于按会话查询
//   - 收件箱索引（messageInboxTag）：按收件人 + 时间戳排序，支持频道广播订阅
//
// 裁剪机制：
//
//	为控制存储大小，每个用户的消息数被限制在 messageWindowSize 内。
//	超出部分由 trimWorker 异步清理。
//
// 锁分片策略：
//
//	使用 256 个互斥锁分片（pebbleProjectionLockShards）减少并发写入冲突。
//	锁分片索引基于 UserKey 的哈希值。
type pebbleMessageProjectionRepository struct {
	// db 是底层的 Pebble 数据库实例
	db *pebble.DB
	// profile 是 Pebble 性能配置（标准/吞吐优化），影响索引值是否内联
	profile PebbleProfile
	// writes 是写入协调器，用于组提交优化
	writes *pebbleWriteCoordinator
	// messageWindowSize 是每个用户保留的最大消息数
	messageWindowSize int
	// userRepository 用于查询用户信息（角色、是否可登录等）
	userRepository UserRepository
	// subscriptions 用于查询频道订阅关系
	subscriptions SubscriptionRepository
	// blacklists 用于查询用户黑名单
	blacklists BlacklistRepository
	// messageTrim 用于记录消息裁剪统计
	messageTrim MessageTrimRepository
	// shardLocks 是锁分片数组，减少并发写入冲突
	shardLocks [pebbleProjectionLockShards]sync.Mutex
	// trimMu 保护 trimWorker 状态（dirtyUsers、trimWake 等）
	trimMu sync.Mutex
	// dirtyUsers 是待裁剪的用户集合，由 scheduleTrim 添加
	dirtyUsers map[UserKey]struct{}
	// trimWake 是唤醒 trimWorker 的通道
	trimWake chan struct{}
	// trimClose 是关闭 trimWorker 的通道
	trimClose chan chan error
	// trimDone 在 trimWorker 退出时关闭，用于同步等待
	trimDone chan struct{}
	// trimClosed 标记 trimWorker 是否已关闭
	trimClosed bool
}

// Append 追加一个本地事件到事件日志。
// 自动生成 EventID（通过 IDGenerator）和 OriginNodeID（当前节点 ID）。
// 分配全局序列号并写入两个索引（序列号索引 + 来源索引）。
func (r *pebbleEventLogRepository) Append(ctx context.Context, event Event) (Event, error) {
	if r.ids == nil {
		return Event{}, fmt.Errorf("append event before id generator initialization")
	}
	if r.clock == nil {
		return Event{}, fmt.Errorf("append event before clock initialization")
	}
	event.EventID = r.ids.Next()
	event.OriginNodeID = r.nodeID
	return r.appendLocal(ctx, event)
}

// AppendReplicated 追加一个来自其他节点的复制事件。
// 事件必须包含有效的 EventID 和 OriginNodeID。
// 如果事件已存在（通过来源索引检测），返回已有事件和 false。
// 返回值 bool 表示是否是新插入的事件。
func (r *pebbleEventLogRepository) AppendReplicated(ctx context.Context, event Event) (Event, bool, error) {
	if event.EventID <= 0 || event.OriginNodeID <= 0 {
		return Event{}, false, fmt.Errorf("%w: replicated event id and origin node id are required", ErrInvalidInput)
	}
	return r.appendReplicated(ctx, event)
}

// appendLocal 在 mu 锁保护下执行本地事件的序列号分配和写入。
func (r *pebbleEventLogRepository) appendLocal(ctx context.Context, event Event) (Event, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, err
	}
	value, err := eventLogValue(event)
	if err != nil {
		return Event{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	sequence, err := r.nextSequenceLocked()
	if err != nil {
		return Event{}, err
	}
	event.Sequence = sequence
	if err := r.writeStoredEvent(event, value); err != nil {
		return Event{}, err
	}
	return event, nil
}

// appendReplicated 在 mu 锁保护下执行复制事件的去重检查和序列号分配。
// 先通过来源索引检查事件是否已存在，避免重复写入。
func (r *pebbleEventLogRepository) appendReplicated(ctx context.Context, event Event) (Event, bool, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, false, err
	}
	value, err := eventLogValue(event)
	if err != nil {
		return Event{}, false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	originKey := pebbleEventOriginKey(event.OriginNodeID, event.EventID)
	if sequence, ok, err := r.readSequence(originKey); err != nil {
		return Event{}, false, err
	} else if ok {
		stored, err := r.eventBySequence(sequence)
		return stored, false, err
	}

	sequence, err := r.nextSequenceLocked()
	if err != nil {
		return Event{}, false, err
	}
	event.Sequence = sequence
	if err := r.writeStoredEvent(event, value); err != nil {
		return Event{}, false, err
	}
	return event, true, nil
}

// writeStoredEvent 提交序列号和索引写入到 Pebble 批处理。
// 如果是非消息事件（如用户创建），forceSync 保证数据持久化。
func (r *pebbleEventLogRepository) writeStoredEvent(event Event, value []byte) error {
	batch := r.db.NewBatch()
	if err := r.writeStoredEventToBatch(batch, event, value); err != nil {
		_ = batch.Close()
		return err
	}
	if err := batch.Set(pebbleEventSequenceKey, encodeInt64(event.Sequence), nil); err != nil {
		return fmt.Errorf("write event sequence meta: %w", err)
	}
	if err := applyPebbleBatch(batch, r.writes, pebbleEventRequiresForceSync(event)); err != nil {
		return fmt.Errorf("commit event append: %w", err)
	}
	r.lastSequence = event.Sequence
	r.sequenceLoaded = true
	return nil
}

// writeStoredEventToBatch 将事件写入到 Pebble 批处理中（不提交）。
// 同时写入序列号索引和来源索引。
func (r *pebbleEventLogRepository) writeStoredEventToBatch(batch *pebble.Batch, event Event, value []byte) error {
	if batch == nil {
		return fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
	}
	originKey := pebbleEventOriginKey(event.OriginNodeID, event.EventID)
	if err := batch.Set(pebbleEventSeqKey(event.Sequence), value, nil); err != nil {
		return fmt.Errorf("write event sequence index: %w", err)
	}
	if err := batch.Set(originKey, encodeInt64(event.Sequence), nil); err != nil {
		return fmt.Errorf("write event origin index: %w", err)
	}
	return nil
}

// ListEvents 按序列号顺序列出指定序列号之后的事件。
// limit 限制返回数量（0-1000，超出时默认 100）。
// 用于事件复制协议中的增量同步。
func (r *pebbleEventLogRepository) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	lower := pebbleEventSeqKey(afterSequence + 1)
	upper := prefixUpperBound([]byte{eventSeqTag})
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("open event iterator: %w", err)
	}
	defer iter.Close()

	events := make([]Event, 0, limit)
	for valid := iter.First(); valid && len(events) < limit; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		event, err := eventFromPebbleValue(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

// ListEventsByOrigin 按事件 ID 顺序列出指定来源节点在指定事件 ID 之后的事件。
// 先通过来源索引获取序列号，再通过序列号索引读取完整事件。
// limit 限制返回数量。
func (r *pebbleEventLogRepository) ListEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64, limit int) ([]Event, error) {
	if originNodeID <= 0 {
		return nil, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	prefix := make([]byte, 0, 9)
	prefix = append(prefix, eventOriginTag)
	prefix = encodeUint64(prefix, uint64(originNodeID))
	lower := pebbleEventOriginKey(originNodeID, afterEventID+1)
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open origin event iterator: %w", err)
	}
	defer iter.Close()

	events := make([]Event, 0, limit)
	for valid := iter.First(); valid && len(events) < limit; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sequence := decodeInt64(iter.Value())
		event, err := r.eventBySequence(sequence)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate origin events: %w", err)
	}
	return events, nil
}

// CountEventsByOrigin 统计指定来源节点在指定事件 ID 之后的事件数量。
func (r *pebbleEventLogRepository) CountEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, nil
	}
	prefix := make([]byte, 0, 9)
	prefix = append(prefix, eventOriginTag)
	prefix = encodeUint64(prefix, uint64(originNodeID))
	lower := pebbleEventOriginKey(originNodeID, afterEventID+1)
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("open count origin event iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterate count origin events: %w", err)
	}
	return count, nil
}

// LastEventSequence 返回已写入的最大序列号。
func (r *pebbleEventLogRepository) LastEventSequence(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	sequence, err := r.lastEventSequenceLocked()
	if err != nil {
		return 0, err
	}
	return sequence, nil
}

// ListOriginProgress 列出所有来源节点的事件进度。
// 遍历事件来源索引（eventOriginTag 前缀），记录每个来源节点的最大事件 ID。
// 结果按 originNodeID 排序。
func (r *pebbleEventLogRepository) ListOriginProgress(ctx context.Context) ([]OriginProgress, error) {
	prefix := []byte{eventOriginTag}
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open origin progress iterator: %w", err)
	}
	defer iter.Close()

	byOrigin := make(map[int64]int64)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		originNodeID, eventID, err := parsePebbleOriginKey(iter.Key())
		if err != nil {
			return nil, err
		}
		if eventID > byOrigin[originNodeID] {
			byOrigin[originNodeID] = eventID
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate origin progress: %w", err)
	}

	progress := make([]OriginProgress, 0, len(byOrigin))
	for originNodeID, eventID := range byOrigin {
		progress = append(progress, OriginProgress{OriginNodeID: originNodeID, LastEventID: eventID})
	}
	sort.Slice(progress, func(i, j int) bool {
		return progress[i].OriginNodeID < progress[j].OriginNodeID
	})
	return progress, nil
}

// nextSequenceLocked 返回下一个可用的序列号（当前最大 + 1）。
// 必须在 mu 锁保护下调用。
func (r *pebbleEventLogRepository) nextSequenceLocked() (int64, error) {
	current, err := r.lastEventSequenceLocked()
	if err != nil {
		return 0, err
	}
	return current + 1, nil
}

// lastEventSequenceLocked 返回当前最大序列号。
// 缓存优先：如果已从磁盘加载，直接返回内存中的值。
// 磁盘读取仅在首次调用时执行。
func (r *pebbleEventLogRepository) lastEventSequenceLocked() (int64, error) {
	if r.sequenceLoaded {
		return r.lastSequence, nil
	}
	current, ok, err := r.readSequence(pebbleEventSequenceKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		r.lastSequence = 0
		r.sequenceLoaded = true
		return 0, nil
	}
	r.lastSequence = current
	r.sequenceLoaded = true
	return current, nil
}

// readSequence 从 Pebble 读取一个序列号值。
// 如果键不存在，返回 (0, false, nil)。
func (r *pebbleEventLogRepository) readSequence(key []byte) (int64, bool, error) {
	value, closer, err := r.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read pebble sequence: %w", err)
	}
	defer closer.Close()
	return decodeInt64(value), true, nil
}

// eventBySequence 通过序列号读取完整的事件记录。
func (r *pebbleEventLogRepository) eventBySequence(sequence int64) (Event, error) {
	value, closer, err := r.db.Get(pebbleEventSeqKey(sequence))
	if err != nil {
		return Event{}, fmt.Errorf("read event sequence %d: %w", sequence, err)
	}
	defer closer.Close()
	return eventFromPebbleValue(pebbleEventSeqKey(sequence), value)
}

// ApplyMessageCreated 将创建消息事件应用到消息投影。
//
// 处理流程:
//  1. 验证消息标识合法性
//  2. 查询收件人信息
//  3. 获取用户锁分片，避免并发冲突
//  4. 检查消息是否已存在（幂等性保证）
//  5. 写入消息主记录和所有索引
//  6. 检查是否需要触发裁剪（超过阈值时同步或异步执行）
//
// 裁剪策略:
//   - 超过硬阈值 (windowSize + slack + hardSlack): 同步裁剪
//   - 超过软阈值 (windowSize + slack): 异步调度给修剪工作器
func (r *pebbleMessageProjectionRepository) ApplyMessageCreated(ctx context.Context, message Message) error {
	key := message.UserKey()
	if err := validateMessageIdentity(key, message.NodeID, message.Seq); err != nil {
		return err
	}
	recipient, err := r.userRepository.GetUser(ctx, key, false)
	if err != nil {
		return err
	}

	unlock := r.lockUsers([]UserKey{key})
	defer unlock()

	if ok, err := r.messageExists(message); err != nil {
		return err
	} else if ok {
		return nil
	}
	state, err := r.putMessage(ctx, message, recipient, false)
	if err != nil {
		return err
	}

	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	switch {
	case state.StoredCount > int64(pebbleMessageTrimHardThreshold(windowSize)):
		if err := r.trimMessagesForUserLocked(ctx, key, false); err != nil {
			r.scheduleTrim(key)
		}
	case state.StoredCount > int64(pebbleMessageTrimThreshold(windowSize)):
		r.scheduleTrim(key)
	}
	return nil
}

// ListMessagesByUser 列出指定用户的消息列表。
//
// 查询逻辑因用户角色而异：
//   - 可登录用户：优先通过收件箱索引（inbox）查询，回退到旧式查询（直接消息 + 广播 + 订阅）
//   - 不可登录用户（如频道）：直接列出该用户索引下的所有消息
//
// 返回的消息按时间降序排列。
func (r *pebbleMessageProjectionRepository) ListMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	user, err := r.userRepository.GetUser(ctx, key, false)
	if err != nil {
		return nil, err
	}
	if !user.CanLogin() {
		return r.listRawMessagesByUser(ctx, key, limit, nil)
	}
	if messages, ok, err := r.listLoginMessagesByUserViaInbox(ctx, key, limit); err != nil {
		return nil, err
	} else if ok {
		return messages, nil
	}
	return r.listLoginMessagesByUserLegacy(ctx, key, limit)
}

// listLoginMessagesByUserLegacy 是可登录用户的旧式消息查询（回退方案）。
//
// 查询来源：
//  1. 直接发送给该用户的消息（经黑名单过滤）
//  2. 所有广播用户的消息
//  3. 该用户订阅的频道消息（仅订阅后的消息）
//
// 合并所有来源后按时间降序排序，取最新 limit 条。
func (r *pebbleMessageProjectionRepository) listLoginMessagesByUserLegacy(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	candidates := make([]Message, 0, limit)
	var seen map[messageIdentityKey]struct{}
	add := func(messages []Message) {
		if len(messages) > 0 && seen == nil {
			seen = make(map[messageIdentityKey]struct{}, len(messages))
		}
		for _, message := range messages {
			id := messageIdentity(message)
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			candidates = append(candidates, message)
		}
	}

	direct, err := r.listRawMessagesByUser(ctx, key, 0, nil)
	if err != nil {
		return nil, err
	}
	direct, err = filterDirectMessagesByBlacklist(ctx, r.userRepository, r.blacklists, key, direct)
	if err != nil {
		return nil, err
	}
	add(direct)

	broadcasts, err := r.userRepository.ListBroadcastUserKeys(ctx)
	if err != nil {
		return nil, err
	}
	for _, broadcast := range broadcasts {
		messages, err := r.listRawMessagesByUser(ctx, broadcast, 0, nil)
		if err != nil {
			return nil, err
		}
		add(messages)
	}

	subscriptions, err := r.subscriptions.ListActiveSubscriptions(ctx, key)
	if err != nil {
		return nil, err
	}
	for _, subscription := range subscriptions {
		since := subscription.SubscribedAt
		messages, err := r.listRawMessagesByUser(ctx, subscription.Channel, 0, &since)
		if err != nil {
			return nil, err
		}
		add(messages)
	}

	sortMessages(candidates)
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates, nil
}

// listLoginMessagesByUserViaInbox 通过收件箱索引查询用户消息（主要方案）。
//
// 收件箱索引为每个用户维护了一个按时间降序排列的消息视图。
// 返回的 bool 表示查询是否成功（false 时调用方应回退到旧式查询）。
//
// 可见性过滤：
//   - 直接消息：发件人不在黑名单中，或在拉黑前发送
//   - 频道消息：用户在订阅后发送
//
// 最后合并广播消息，去重后按时间排序返回。
func (r *pebbleMessageProjectionRepository) listLoginMessagesByUserViaInbox(ctx context.Context, key UserKey, limit int) ([]Message, bool, error) {
	scanLimit := limit
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	if scanLimit < windowSize {
		scanLimit = windowSize
	}
	inboxMessages, err := r.listInboxMessagesByUser(ctx, key, scanLimit)
	if err != nil {
		return nil, false, err
	}

	blockedAtBySender, err := r.blockedAtBySender(ctx, key)
	if err != nil {
		return nil, false, err
	}
	subscriptions, err := r.subscriptions.ListActiveSubscriptions(ctx, key)
	if err != nil {
		return nil, false, err
	}
	subscriptionByChannel := make(map[UserKey]clock.Timestamp, len(subscriptions))
	for _, subscription := range subscriptions {
		subscriptionByChannel[subscription.Channel] = subscription.SubscribedAt
	}
	senderRoleCache := make(map[UserKey]string, len(blockedAtBySender))
	candidates := make([]Message, 0, limit)
	for _, message := range inboxMessages {
		visible, err := r.inboxMessageVisible(ctx, key, message, blockedAtBySender, subscriptionByChannel, senderRoleCache)
		if err != nil {
			return nil, false, err
		}
		if !visible {
			continue
		}
		candidates = append(candidates, message)
		if len(candidates) >= limit {
			break
		}
	}
	if len(candidates) < limit {
		return nil, false, nil
	}

	broadcasts, err := r.listBroadcastMessages(ctx)
	if err != nil {
		return nil, false, err
	}
	if len(broadcasts) > 0 {
		seen := make(map[messageIdentityKey]struct{}, len(candidates)+len(broadcasts))
		merged := make([]Message, 0, len(candidates)+len(broadcasts))
		for _, message := range candidates {
			id := messageIdentity(message)
			seen[id] = struct{}{}
			merged = append(merged, message)
		}
		for _, message := range broadcasts {
			id := messageIdentity(message)
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			merged = append(merged, message)
		}
		candidates = merged
		sortMessages(candidates)
		if len(candidates) > limit {
			candidates = candidates[:limit]
		}
	}
	return candidates, true, nil
}

// listInboxMessagesByUser 从收件箱索引列出用户消息。
// 收件箱键按 (ownerNodeID, ownerUserID, createdAt DESC) 排序。
func (r *pebbleMessageProjectionRepository) listInboxMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = normalizeMessageWindowSize(r.messageWindowSize)
	}

	prefix := make([]byte, 0, 17)
	prefix = append(prefix, messageInboxTag)
	prefix = encodeUint64(prefix, uint64(key.NodeID))
	prefix = encodeUint64(prefix, uint64(key.UserID))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open inbox iterator: %w", err)
	}
	defer iter.Close()

	messages := make([]Message, 0, limit)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		message, err := r.messageFromIndexValue(iter.Value())
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
		if len(messages) >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate inbox messages: %w", err)
	}
	return messages, nil
}

// listBroadcastMessages 列出所有广播用户的消息。
// 广播用户是具有 RoleBroadcast 角色的用户，其消息对所有可登录用户可见。
func (r *pebbleMessageProjectionRepository) listBroadcastMessages(ctx context.Context) ([]Message, error) {
	broadcasts, err := r.userRepository.ListBroadcastUserKeys(ctx)
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0)
	for _, broadcast := range broadcasts {
		items, err := r.listRawMessagesByUser(ctx, broadcast, 0, nil)
		if err != nil {
			return nil, err
		}
		messages = append(messages, items...)
	}
	return messages, nil
}

// blockedAtBySender 查询指定用户的所有黑名单条目，返回发送者到屏蔽时间的映射。
// 用于收件箱消息的可见性过滤。
func (r *pebbleMessageProjectionRepository) blockedAtBySender(ctx context.Context, owner UserKey) (map[UserKey]clock.Timestamp, error) {
	if r.blacklists == nil {
		return nil, nil
	}
	entries, err := r.blacklists.ListActiveBlockedUsers(ctx, owner)
	if err != nil {
		return nil, err
	}
	blockedAtBySender := make(map[UserKey]clock.Timestamp, len(entries))
	for _, entry := range entries {
		blockedAtBySender[entry.Blocked] = entry.BlockedAt
	}
	return blockedAtBySender, nil
}

// inboxMessageVisible 判断收件箱中的一条消息对用户是否可见。
//
// 规则：
//   - 直接消息（owner 是收件人）：发件人未在屏蔽时间内屏蔽，或发件人具有非普通用户角色
//   - 频道消息（owner 是订阅者）：消息在订阅时间之后发送
func (r *pebbleMessageProjectionRepository) inboxMessageVisible(ctx context.Context, owner UserKey, message Message, blockedAtBySender map[UserKey]clock.Timestamp, subscriptionByChannel map[UserKey]clock.Timestamp, senderRoleCache map[UserKey]string) (bool, error) {
	if message.Recipient == owner {
		blockedAt, blocked := blockedAtBySender[message.Sender]
		if !blocked || message.CreatedAt.Compare(blockedAt) < 0 {
			return true, nil
		}
		role, ok := senderRoleCache[message.Sender]
		if !ok {
			sender, err := r.userRepository.GetUser(ctx, message.Sender, false)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					role = ""
				} else {
					return false, err
				}
			} else {
				role = sender.Role
			}
			senderRoleCache[message.Sender] = role
		}
		return role != RoleUser, nil
	}

	subscribedAt, ok := subscriptionByChannel[message.Recipient]
	if !ok {
		return false, nil
	}
	return message.CreatedAt.Compare(subscribedAt) >= 0, nil
}

// BuildMessageSnapshotRows 构建消息快照行，用于节点间状态同步。
//
// 遍历生产者索引（messageProducerTag）中指定生产者的所有消息，
// 仅保留每个用户最新的 windowSize 条消息（按时间降序）。
// 构建的快照行用于复制协议中的全量同步。
func (r *pebbleMessageProjectionRepository) BuildMessageSnapshotRows(ctx context.Context, producer int64) ([]*clusterproto.SnapshotRow, error) {
	if producer <= 0 {
		return nil, fmt.Errorf("%w: producer cannot be empty", ErrInvalidInput)
	}
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	prefix := make([]byte, 0, 9)
	prefix = append(prefix, messageProducerTag)
	prefix = encodeUint64(prefix, uint64(producer))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open snapshot message iterator: %w", err)
	}
	defer iter.Close()

	rows := make([]*clusterproto.SnapshotRow, 0)
	currentUser := UserKey{}
	userCount := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		message, err := r.messageFromIndexValue(iter.Value())
		if err != nil {
			return nil, err
		}
		if message.UserKey() != currentUser {
			currentUser = message.UserKey()
			userCount = 0
		}
		userCount++
		if userCount > windowSize {
			continue
		}
		rows = append(rows, snapshotRowFromMessage(message))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate snapshot messages: %w", err)
	}
	return rows, nil
}

// ApplyMessageSnapshotRows 应用消息快照行，从其他节点恢复消息投影状态。
//
// 处理流程：
//  1. 逐行应用快照数据（跳过已存在的消息和已删除用户的消息）
//  2. 收集受影响的用户列表
//  3. 对所有受影响用户执行消息裁剪
func (r *pebbleMessageProjectionRepository) ApplyMessageSnapshotRows(ctx context.Context, producer int64, rows []*clusterproto.SnapshotRow) error {
	affectedUsers := make(map[UserKey]struct{})
	for _, row := range rows {
		key, err := r.applyMessageSnapshotRow(ctx, producer, row)
		if err != nil {
			return err
		}
		if key != (UserKey{}) {
			affectedUsers[key] = struct{}{}
		}
	}
	for key := range affectedUsers {
		if err := r.trimMessagesForUser(ctx, key, true); err != nil {
			return err
		}
	}
	return nil
}

// applyMessageSnapshotRow 应用单行消息快照数据。
// 跳过接收者用户已删除的消息和已存在的消息（幂等性保证）。
// 返回受影响的用户 Key，如果消息被跳过则返回空 Key。
func (r *pebbleMessageProjectionRepository) applyMessageSnapshotRow(ctx context.Context, producer int64, row *clusterproto.SnapshotRow) (UserKey, error) {
	if row == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	messageRow := row.GetMessage()
	if messageRow == nil {
		return UserKey{}, fmt.Errorf("%w: messages snapshot contains non-message row", ErrInvalidInput)
	}
	if messageRow.Recipient == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot message recipient cannot be empty", ErrInvalidInput)
	}
	key := UserKey{NodeID: messageRow.Recipient.NodeId, UserID: messageRow.Recipient.UserId}
	if err := validateMessageIdentity(key, messageRow.NodeId, messageRow.Seq); err != nil {
		return UserKey{}, err
	}
	if messageRow.NodeId != producer {
		return UserKey{}, fmt.Errorf("%w: message node id %d does not match partition producer %d", ErrInvalidInput, messageRow.NodeId, producer)
	}
	createdAt, err := parseRequiredTimestamp(messageRow.CreatedAtHlc, "snapshot message created_at")
	if err != nil {
		return UserKey{}, err
	}
	if _, err := r.userRepository.GetUser(ctx, key, false); err != nil {
		if errors.Is(err, ErrNotFound) {
			return UserKey{}, nil
		}
		return UserKey{}, err
	}

	if messageRow.Sender == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot message sender cannot be empty", ErrInvalidInput)
	}
	message := Message{
		Recipient: key,
		NodeID:    messageRow.NodeId,
		Seq:       messageRow.Seq,
		Sender:    UserKey{NodeID: messageRow.Sender.NodeId, UserID: messageRow.Sender.UserId},
		Body:      messageRow.Body,
		CreatedAt: createdAt,
	}
	unlock := r.lockUsers([]UserKey{key})
	defer unlock()
	if ok, err := r.messageExists(message); err != nil {
		return UserKey{}, err
	} else if ok {
		return key, nil
	}
	recipient, err := r.userRepository.GetUser(ctx, key, false)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return UserKey{}, nil
		}
		return UserKey{}, err
	}
	if _, err := r.putMessage(ctx, message, recipient, true); err != nil {
		return UserKey{}, err
	}
	return key, nil
}

// messageExists 检查消息是否已存在于投影中（通过主键索引）。
// 用于幂等性判断。
func (r *pebbleMessageProjectionRepository) messageExists(message Message) (bool, error) {
	_, closer, err := r.db.Get(pebbleMessageIDKey(message))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("check message projection: %w", err)
	}
	return true, closer.Close()
}

// putMessage 写入消息到所有索引。
//
// 写入的索引包括：
//   - 主键索引（messageIDTag）
//   - 用户索引（messageUserTag）
//   - 生产者索引（messageProducerTag）
//   - 收件箱索引（messageInboxTag，按收件人角色决定：可登录用户直接写，频道转发给订阅者）
//   - 会话索引（messageSessionTag）
//   - 消息序列元数据（metaMessageSequenceTag）
//   - 用户状态元数据（metaMessageUserStateTag）
func (r *pebbleMessageProjectionRepository) putMessage(ctx context.Context, message Message, recipient User, forceSync bool) (pebbleMessageUserState, error) {
	state, err := r.messageUserStateLocked(ctx, message.UserKey())
	if err != nil {
		return pebbleMessageUserState{}, err
	}
	batch := r.db.NewBatch()
	state, value, err := r.prepareMessageWrite(batch, message, state)
	if err != nil {
		_ = batch.Close()
		return pebbleMessageUserState{}, err
	}
	if err := r.prepareInboxWrites(ctx, batch, message, recipient, value); err != nil {
		_ = batch.Close()
		return pebbleMessageUserState{}, err
	}
	if err := applyPebbleBatch(batch, r.writes, forceSync); err != nil {
		return pebbleMessageUserState{}, fmt.Errorf("commit message projection: %w", err)
	}
	return state, nil
}

// listRawMessagesByUser 列出用户索引下的所有消息（可见消息，受 windowSize 限制）。
func (r *pebbleMessageProjectionRepository) listRawMessagesByUser(ctx context.Context, key UserKey, limit int, since *clock.Timestamp) ([]Message, error) {
	return r.listMessagesByUserIndex(ctx, key, limit, since, true)
}

// listStoredMessagesByUser 列出用户索引下的所有消息（不限数量，用于裁剪判断）。
func (r *pebbleMessageProjectionRepository) listStoredMessagesByUser(ctx context.Context, key UserKey, since *clock.Timestamp) ([]Message, error) {
	return r.listMessagesByUserIndex(ctx, key, 0, since, false)
}

// listMessagesByUserIndex 列出用户索引下的消息。
//
// 参数：
//   - visibleOnly: 为 true 时限制数量为 windowSize；false 时返回所有消息（用于裁剪）
//   - since: 可选时间戳过滤，仅返回在此时间之后的消息
func (r *pebbleMessageProjectionRepository) listMessagesByUserIndex(ctx context.Context, key UserKey, limit int, since *clock.Timestamp, visibleOnly bool) ([]Message, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if visibleOnly {
		windowSize := normalizeMessageWindowSize(r.messageWindowSize)
		if limit <= 0 || limit > windowSize {
			limit = windowSize
		}
	}
	prefix := make([]byte, 0, 17)
	prefix = append(prefix, messageUserTag)
	prefix = encodeUint64(prefix, uint64(key.NodeID))
	prefix = encodeUint64(prefix, uint64(key.UserID))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open user message iterator: %w", err)
	}
	defer iter.Close()

	var messages []Message
	for valid := iter.First(); valid; valid = iter.Next() {
		if messages == nil {
			messages = make([]Message, 0, limit)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		message, err := r.messageFromIndexValue(iter.Value())
		if err != nil {
			return nil, err
		}
		if since != nil && message.CreatedAt.Compare(*since) < 0 {
			break
		}
		messages = append(messages, message)
		if limit > 0 && len(messages) >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate user messages: %w", err)
	}
	return messages, nil
}

// ListMessagesBySession 列出指定会话的消息。
// session 必须是 32 字节的会话标识（由 MessageSession 函数生成，基于发送者和接收者的 UserKey）。
// 结果经过黑名单过滤。
func (r *pebbleMessageProjectionRepository) ListMessagesBySession(ctx context.Context, session []byte, requester UserKey, limit int) ([]Message, error) {
	if len(session) != 32 {
		return nil, fmt.Errorf("%w: session must be exactly 32 bytes", ErrInvalidInput)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if err := requester.Validate(); err != nil {
		return nil, err
	}

	prefix := make([]byte, 0, 33)
	prefix = append(prefix, messageSessionTag)
	prefix = append(prefix, session...)
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open session message iterator: %w", err)
	}
	defer iter.Close()

	messages := make([]Message, 0, limit)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		message, err := r.messageFromIndexValue(iter.Value())
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
		if len(messages) >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate session messages: %w", err)
	}

	if len(messages) == 0 {
		return messages, nil
	}
	return filterDirectMessagesByBlacklist(ctx, r.userRepository, r.blacklists, requester, messages)
}

// pebbleEventRequiresForceSync 判断事件是否需要强制同步刷盘。
// 只有 MessageCreated 类型的事件可以使用 relaxed 模式异步刷盘，
// 其他类型事件（如系统事件）需要立即持久化以保证一致性。
func pebbleEventRequiresForceSync(event Event) bool {
	return event.EventType != EventTypeMessageCreated
}

// applyPebbleBatch 将 Pebble 批次提交到写入协调器或直接提交。
// 参数：
//   - batch: 待提交的 Pebble 批次
//   - writes: 写入协调器实例（nil 时直接提交到 DB）
//   - forceSync: 是否同步刷盘
//
// 当 writes 不为 nil 时委托给协调器处理，否则直接调用 batch.Commit。
func applyPebbleBatch(batch *pebble.Batch, writes *pebbleWriteCoordinator, forceSync bool) error {
	if batch == nil {
		return fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
	}
	if writes != nil {
		return writes.Apply(batch, forceSync)
	}
	defer batch.Close()
	if forceSync {
		return batch.Commit(pebble.Sync)
	}
	return batch.Commit(pebble.NoSync)
}

// pebbleMessageValue 将 Message 序列化为 Protobuf 字节切片。
// 用于在 Pebble 中存储消息的主记录值。
func pebbleMessageValue(message Message) ([]byte, error) {
	value, err := gproto.Marshal(snapshotRowFromMessage(message))
	if err != nil {
		return nil, fmt.Errorf("marshal pebble message: %w", err)
	}
	return value, nil
}

// pebbleMessageRef 是消息的轻量级引用，用于索引条目指向主记录。
// 包含接收者（Recipient）、节点 ID（NodeID）和序列号（Seq），
// 足以唯一标识一条消息并在主记录中查找。
type pebbleMessageRef struct {
	Recipient UserKey
	NodeID    int64
	Seq       int64
}

// pebbleMessageRefFromMessage 从 Message 结构体创建 pebbleMessageRef 引用。
func pebbleMessageRefFromMessage(message Message) pebbleMessageRef {
	return pebbleMessageRef{
		Recipient: message.Recipient,
		NodeID:    message.NodeID,
		Seq:       message.Seq,
	}
}

// pebbleMessageIndexValue 将 pebbleMessageRef 编码为索引条目值。
// 格式：[marker(1) + recipient_node(8) + recipient_user(8) + node(8) + seq(8)]，共 33 字节。
// 索引条目通过引用指向主记录，避免在多个索引中冗余存储完整消息数据。
func pebbleMessageIndexValue(ref pebbleMessageRef) []byte {
	value := make([]byte, 1+8*4)
	value[0] = pebbleMessageIndexRefMarker
	binary.BigEndian.PutUint64(value[1:9], uint64(ref.Recipient.NodeID))
	binary.BigEndian.PutUint64(value[9:17], uint64(ref.Recipient.UserID))
	binary.BigEndian.PutUint64(value[17:25], uint64(ref.NodeID))
	binary.BigEndian.PutUint64(value[25:33], uint64(ref.Seq))
	return value
}

// pebbleMessageRefFromValue 从索引值解码出 pebbleMessageRef。
// 返回值：
//   - pebbleMessageRef: 解码后的引用
//   - bool: 是否为引用格式（true）或内联格式（false）
//   - error: 解码错误
func pebbleMessageRefFromValue(value []byte) (pebbleMessageRef, bool, error) {
	if len(value) == 0 || value[0] != pebbleMessageIndexRefMarker {
		return pebbleMessageRef{}, false, nil
	}
	if len(value) != 33 {
		return pebbleMessageRef{}, true, fmt.Errorf("%w: invalid pebble message ref length %d", ErrInvalidInput, len(value))
	}
	return pebbleMessageRef{
		Recipient: UserKey{
			NodeID: int64(binary.BigEndian.Uint64(value[1:9])),
			UserID: int64(binary.BigEndian.Uint64(value[9:17])),
		},
		NodeID: int64(binary.BigEndian.Uint64(value[17:25])),
		Seq:    int64(binary.BigEndian.Uint64(value[25:33])),
	}, true, nil
}

// pebbleMessageTrimThreshold 计算消息裁剪触发阈值。
// 当用户消息数超过此阈值时触发裁剪，裁剪目标为 windowSize。
// 引入 pebbleMessageTrimSlack（松弛量）避免频繁触发裁剪。
func pebbleMessageTrimThreshold(windowSize int) int {
	if windowSize <= pebbleMessageTrimSlack {
		return windowSize
	}
	return windowSize + pebbleMessageTrimSlack
}

// messageFromIndexValue 从索引值解码 Message。
// 索引值可能是引用格式（指向主记录）或内联格式（直接包含消息数据）。
func (r *pebbleMessageProjectionRepository) messageFromIndexValue(value []byte) (Message, error) {
	ref, ok, err := pebbleMessageRefFromValue(value)
	if err != nil {
		return Message{}, err
	}
	if !ok {
		return messageFromPebbleValue(value)
	}
	return r.messageByRef(ref)
}

// messageByRef 通过 pebbleMessageRef 从主记录中读取完整 Message。
// 在主记录键空间（messageIDTag 前缀）中查找消息。
func (r *pebbleMessageProjectionRepository) messageByRef(ref pebbleMessageRef) (Message, error) {
	value, closer, err := r.db.Get(pebbleMessageIDKeyFromRef(ref))
	if err != nil {
		return Message{}, fmt.Errorf("read pebble message primary record: %w", err)
	}
	defer closer.Close()
	return messageFromPebbleValue(value)
}

// userIndexValue 为消息生成用户索引值（按接收者索引）。
// 在 PebbleProfileThroughput 且消息体较小时内联存储完整数据。
func (r *pebbleMessageProjectionRepository) userIndexValue(primaryValue []byte, message Message) []byte {
	return r.messageIndexValueForProfile(primaryValue, message, true)
}

// producerIndexValue 为消息生成生产者索引值（按发送者节点索引）。
func (r *pebbleMessageProjectionRepository) producerIndexValue(primaryValue []byte, message Message) []byte {
	return r.messageIndexValueForProfile(primaryValue, message, false)
}

// inboxIndexValue 为消息生成收件箱索引值（按频道订阅者索引）。
// trackSource 控制是否内联消息数据（false=内联，true=引用）。
func (r *pebbleMessageProjectionRepository) inboxIndexValue(primaryValue []byte, message Message, trackSource bool) []byte {
	return r.messageIndexValueForProfile(primaryValue, message, !trackSource)
}

// messageIndexValueForProfile 根据存储 profile 策略决定索引值格式。
// 参数：
//   - inlineAllowed: 是否允许内联存储（Throughput profile 且消息体小则内联）
//
// 内联存储避免额外的主记录读取，提高查询吞吐。
func (r *pebbleMessageProjectionRepository) messageIndexValueForProfile(primaryValue []byte, message Message, inlineAllowed bool) []byte {
	if inlineAllowed && r != nil && r.profile == PebbleProfileThroughput && len(primaryValue) > 0 && len(primaryValue) <= pebbleThroughputInlineValueMaxBytes {
		return primaryValue
	}
	return pebbleMessageIndexValue(pebbleMessageRefFromMessage(message))
}

// messageFromPebbleValue 从 Pebble 值字节反序列化 Message。
// 使用 Protobuf 解码 SnapshotRow，提取 message 字段并验证必要字段。
func messageFromPebbleValue(value []byte) (Message, error) {
	var row clusterproto.SnapshotRow
	if err := gproto.Unmarshal(value, &row); err != nil {
		return Message{}, fmt.Errorf("unmarshal pebble message: %w", err)
	}
	messageRow := row.GetMessage()
	if messageRow == nil {
		return Message{}, fmt.Errorf("%w: stored pebble message row is empty", ErrInvalidInput)
	}
	if messageRow.Recipient == nil {
		return Message{}, fmt.Errorf("%w: stored pebble message recipient is empty", ErrInvalidInput)
	}
	if messageRow.Sender == nil {
		return Message{}, fmt.Errorf("%w: stored pebble message sender is empty", ErrInvalidInput)
	}
	createdAt, err := parseRequiredTimestamp(messageRow.CreatedAtHlc, "stored message created_at")
	if err != nil {
		return Message{}, err
	}
	return Message{
		Recipient: UserKey{NodeID: messageRow.Recipient.NodeId, UserID: messageRow.Recipient.UserId},
		NodeID:    messageRow.NodeId,
		Seq:       messageRow.Seq,
		Sender:    UserKey{NodeID: messageRow.Sender.NodeId, UserID: messageRow.Sender.UserId},
		Body:      messageRow.Body,
		CreatedAt: createdAt,
	}, nil
}

// eventFromPebbleValue 从 Pebble 键值对反序列化 Event。
// 使用 Protobuf 解码 ReplicatedEvent，并从键中解析序列号。
func eventFromPebbleValue(key, value []byte) (Event, error) {
	var replicated clusterproto.ReplicatedEvent
	if err := gproto.Unmarshal(value, &replicated); err != nil {
		return Event{}, fmt.Errorf("unmarshal pebble event value: %w", err)
	}
	event, err := eventFromReplicatedEvent(&replicated)
	if err != nil {
		return Event{}, err
	}
	event.Sequence = parsePebbleSequenceKey(key)
	return event, nil
}

// pebbleEventSeqKey 生成事件序列号键。
// 格式：[eventSeqTag(1) + sequence(8)]，共 9 字节。
// 按序列号递增排列，用于事件日志的顺序扫描。
func pebbleEventSeqKey(sequence int64) []byte {
	buf := make([]byte, 0, 9)
	buf = append(buf, eventSeqTag)
	return encodeUint64(buf, uint64(sequence))
}

// pebbleEventOriginKey 生成事件来源键。
// 格式：[eventOriginTag(1) + origin_node(8) + event_id(8)]，共 17 字节。
// 用于按来源节点和事件 ID 查询事件（去重和复制追踪）。
func pebbleEventOriginKey(originNodeID, eventID int64) []byte {
	buf := make([]byte, 0, 17)
	buf = append(buf, eventOriginTag)
	buf = encodeUint64(buf, uint64(originNodeID))
	return encodeUint64(buf, uint64(eventID))
}

// pebbleMessageSequenceKey 生成消息序列号键（元数据）。
// 格式：[metaMessageSequenceTag(1) + user_node(8) + user_id(8) + node(8)]，共 25 字节。
// 用于存储每条消息在各个节点上的最新序列号。
func pebbleMessageSequenceKey(key UserKey, nodeID int64) []byte {
	buf := make([]byte, 0, 25)
	buf = append(buf, metaMessageSequenceTag)
	buf = encodeUint64(buf, uint64(key.NodeID))
	buf = encodeUint64(buf, uint64(key.UserID))
	return encodeUint64(buf, uint64(nodeID))
}

// pebbleMessageIDPrefix 生成消息主记录键的前缀。
// 格式：[messageIDTag(1) + user_node(8) + user_id(8) + node(8)]，共 25 字节。
// 用于按（接收者，节点）范围扫描消息主记录。
func pebbleMessageIDPrefix(key UserKey, nodeID int64) []byte {
	buf := make([]byte, 0, 25)
	buf = append(buf, messageIDTag)
	buf = encodeUint64(buf, uint64(key.NodeID))
	buf = encodeUint64(buf, uint64(key.UserID))
	return encodeUint64(buf, uint64(nodeID))
}

// pebbleMessageIDKey 从 Message 生成消息主记录键。
// 委托给 pebbleMessageIDKeyFromRef。
func pebbleMessageIDKey(message Message) []byte {
	return pebbleMessageIDKeyFromRef(pebbleMessageRefFromMessage(message))
}

// pebbleMessageIDKeyFromRef 从 pebbleMessageRef 生成消息主记录键。
// 格式：[messageIDTag(1) + recipient_node(8) + recipient_user(8) + node(8) + seq(8)]，共 33 字节。
// 这是消息的唯一主键，所有索引条目通过此键引用消息。
func pebbleMessageIDKeyFromRef(ref pebbleMessageRef) []byte {
	buf := make([]byte, 0, 33)
	buf = append(buf, messageIDTag)
	buf = encodeUint64(buf, uint64(ref.Recipient.NodeID))
	buf = encodeUint64(buf, uint64(ref.Recipient.UserID))
	buf = encodeUint64(buf, uint64(ref.NodeID))
	return encodeUint64(buf, uint64(ref.Seq))
}

// pebbleMessageUserKey 生成用户索引键（按接收者查询）。
// 格式：[messageUserTag(1) + recipient_node(8) + recipient_user(8) + timestamp_desc(8) + node(8) + seq_desc(8)]，共 51 字节。
// 时间戳和序列号使用降序编码，使最新消息排在扫描结果最前面。
func pebbleMessageUserKey(message Message) []byte {
	buf := make([]byte, 0, 51)
	buf = append(buf, messageUserTag)
	buf = encodeUint64(buf, uint64(message.Recipient.NodeID))
	buf = encodeUint64(buf, uint64(message.Recipient.UserID))
	buf = encodeTimestampDesc(buf, message.CreatedAt)
	buf = encodeUint64(buf, uint64(message.NodeID))
	return encodeUint64Desc(buf, uint64(message.Seq))
}

// pebbleMessageProducerKey 生成生产者索引键（按发送者查询）。
// 格式：[messageProducerTag(1) + sender_node(8) + recipient_node(8) + recipient_user(8) + timestamp_desc(8) + seq_desc(8)]，共 51 字节。
func pebbleMessageProducerKey(message Message) []byte {
	buf := make([]byte, 0, 51)
	buf = append(buf, messageProducerTag)
	buf = encodeUint64(buf, uint64(message.NodeID))
	buf = encodeUint64(buf, uint64(message.Recipient.NodeID))
	buf = encodeUint64(buf, uint64(message.Recipient.UserID))
	buf = encodeTimestampDesc(buf, message.CreatedAt)
	return encodeUint64Desc(buf, uint64(message.Seq))
}

// pebbleInboxUserKey 生成收件箱索引键（按频道订阅者查询）。
// 格式：[messageInboxTag(1) + owner_node(8) + owner_user(8) + timestamp_desc(8) + recipient(16) + node(8) + seq_desc(8)]，共 67 字节。
// 用于频道订阅者查看频道中的消息。
func pebbleInboxUserKey(owner UserKey, message Message) []byte {
	buf := make([]byte, 0, 67)
	buf = append(buf, messageInboxTag)
	buf = encodeUint64(buf, uint64(owner.NodeID))
	buf = encodeUint64(buf, uint64(owner.UserID))
	buf = encodeTimestampDesc(buf, message.CreatedAt)
	buf = encodeUint64(buf, uint64(message.Recipient.NodeID))
	buf = encodeUint64(buf, uint64(message.Recipient.UserID))
	buf = encodeUint64(buf, uint64(message.NodeID))
	return encodeUint64Desc(buf, uint64(message.Seq))
}

// pebbleInboxSourcePrefix 生成收件箱来源前缀键。
// 用于查找某条消息的所有收件箱副本。
func pebbleInboxSourcePrefix(message Message) []byte {
	buf := make([]byte, 0, 33)
	buf = append(buf, messageInboxSourceTag)
	buf = encodeUint64(buf, uint64(message.Recipient.NodeID))
	buf = encodeUint64(buf, uint64(message.Recipient.UserID))
	buf = encodeUint64(buf, uint64(message.NodeID))
	return encodeUint64(buf, uint64(message.Seq))
}

// pebbleInboxSourceKey 生成收件箱来源键（从消息到订阅者的映射）。
// 格式：[messageInboxSourceTag(1) + recipient(16) + node(8) + seq(8) + owner(16)]，共 49 字节。
func pebbleInboxSourceKey(message Message, owner UserKey) []byte {
	buf := make([]byte, 0, 49)
	buf = append(buf, messageInboxSourceTag)
	buf = encodeUint64(buf, uint64(message.Recipient.NodeID))
	buf = encodeUint64(buf, uint64(message.Recipient.UserID))
	buf = encodeUint64(buf, uint64(message.NodeID))
	buf = encodeUint64(buf, uint64(message.Seq))
	buf = encodeUint64(buf, uint64(owner.NodeID))
	return encodeUint64(buf, uint64(owner.UserID))
}

// pebbleMessageSessionKey 生成会话索引键（按两人会话查询）。
// 格式：[messageSessionTag(1) + session(32) + timestamp_desc(8) + recipient(16) + node(8) + seq_desc(8)]，共 83 字节。
// session 由 MessageSession 函数基于发送者和接收者 UserKey 生成（32 字节哈希）。
func pebbleMessageSessionKey(message Message) []byte {
	session := MessageSession(message.Sender, message.Recipient)
	buf := make([]byte, 0, 83)
	buf = append(buf, messageSessionTag)
	buf = append(buf, session...)
	buf = encodeTimestampDesc(buf, message.CreatedAt)
	buf = encodeUint64(buf, uint64(message.Recipient.NodeID))
	buf = encodeUint64(buf, uint64(message.Recipient.UserID))
	buf = encodeUint64(buf, uint64(message.NodeID))
	return encodeUint64Desc(buf, uint64(message.Seq))
}

// pebbleMessageKeys 返回消息的所有索引键列表。
// 包括：主记录键、用户索引键、生产者索引键、会话索引键。
// 用于批量写入时创建所有必要的索引条目。
func pebbleMessageKeys(message Message) [][]byte {
	return [][]byte{
		pebbleMessageIDKey(message),
		pebbleMessageUserKey(message),
		pebbleMessageProducerKey(message),
		pebbleMessageSessionKey(message),
	}
}

// parsePebbleSequenceKey 从事件序列号键中解析出序列号。
func parsePebbleSequenceKey(key []byte) int64 {
	if len(key) == 9 && key[0] == eventSeqTag {
		return int64(decodeUint64(key[1:9]))
	}
	return 0
}

// parsePebbleOriginKey 从事件来源键中解析出来源节点 ID 和事件 ID。
func parsePebbleOriginKey(key []byte) (int64, int64, error) {
	if len(key) != 17 || key[0] != eventOriginTag {
		return 0, 0, fmt.Errorf("parse event origin key %q: invalid format", key)
	}
	return int64(decodeUint64(key[1:9])), int64(decodeUint64(key[9:17])), nil
}

// parsePebbleMessageIDKey 从消息主记录键中解析出接收者 UserKey、节点 ID 和序列号。
func parsePebbleMessageIDKey(key []byte) (UserKey, int64, int64, error) {
	if len(key) != 33 || key[0] != messageIDTag {
		return UserKey{}, 0, 0, fmt.Errorf("parse message id key %q: invalid format", key)
	}
	return UserKey{NodeID: int64(decodeUint64(key[1:9])), UserID: int64(decodeUint64(key[9:17]))}, int64(decodeUint64(key[17:25])), int64(decodeUint64(key[25:33])), nil
}

// sortMessages 对消息切片按时间降序排序。
// 排序规则：按 CreatedAt 降序 -> NodeID 升序 -> Seq 降序。
// 时间戳相同时，使用 NodeID+Seq 作为稳定排序的辅助键。
func sortMessages(messages []Message) {
	sort.Slice(messages, func(i, j int) bool {
		if cmp := messages[i].CreatedAt.Compare(messages[j].CreatedAt); cmp != 0 {
			return cmp > 0
		}
		if messages[i].NodeID != messages[j].NodeID {
			return messages[i].NodeID < messages[j].NodeID
		}
		return messages[i].Seq > messages[j].Seq
	})
}
