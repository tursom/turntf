package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
)

// 消息投影相关的常量配置。
const (
	// pebbleProjectionLockShards 用户锁的分片数。
	// 通过哈希将 UserKey 映射到 256 个分片之一，减少锁竞争。
	pebbleProjectionLockShards = 256
	// pebbleMessageTrimWorkerMaxUsers 后台 trim worker 单次处理的最大用户数。
	// 超过此数量时立即触发 flush，避免积压。
	pebbleMessageTrimWorkerMaxUsers = 64
	// pebbleMessageTrimWorkerDelay trim worker 收到唤醒信号后的等待延迟。
	// 在延迟期间可以合并多个用户的 trim 请求，减少批处理次数。
	pebbleMessageTrimWorkerDelay = 25 * time.Millisecond
	// pebbleMessageTrimHardSlack 硬触发 trim 的松弛容量。
	// StoredCount 超过 (windowSize + hardSlack) 时会立即同步执行 trim，而非调度到后台。
	pebbleMessageTrimHardSlack = 128
	// pebbleMessageUserStateVersion 用户状态序列化版本号。
	pebbleMessageUserStateVersion = byte(1)
)

// pebbleMessageUserState 用户消息状态的运行时快照。
// 记录某个用户当前在 Pebble 中存储的消息数量和状态，用于判断是否需要 trim 以及
// 在 prepareMessageWrite 时更新。
type pebbleMessageUserState struct {
	// StoredCount 当前存储的消息数量
	StoredCount int64
	// MaxSeq 当前存储的最大消息序列号
	MaxSeq int64
	// TrimNeeded 标记是否需要执行 trim 操作
	TrimNeeded bool
}

// startTrimWorker 启动后台 trim worker 协程。
// 该协程负责在后台异步删除超出窗口大小的旧消息。幂等操作，不会重复启动。
func (r *pebbleMessageProjectionRepository) startTrimWorker() {
	if r == nil {
		return
	}

	r.trimMu.Lock()
	defer r.trimMu.Unlock()

	if r.trimWake != nil {
		return
	}
	r.dirtyUsers = make(map[UserKey]struct{})
	r.trimWake = make(chan struct{}, 1)
	r.trimClose = make(chan chan error, 1)
	r.trimDone = make(chan struct{})
	go r.runTrimWorker()
}

// close 关闭后台 trim worker，等待所有待处理的 trim 操作完成后返回。
func (r *pebbleMessageProjectionRepository) close() error {
	if r == nil {
		return nil
	}

	r.trimMu.Lock()
	if r.trimWake == nil {
		r.trimMu.Unlock()
		return nil
	}
	if r.trimClosed {
		r.trimMu.Unlock()
		<-r.trimDone
		return nil
	}
	r.trimClosed = true
	r.trimMu.Unlock()

	response := make(chan error, 1)
	r.trimClose <- response
	err := <-response
	<-r.trimDone
	return err
}

// scheduleTrim 将指定用户加入 trim 待处理队列并通过 channel 唤醒后台 worker。
// 用于 processLocalMessageBatch 中标记需要 trim 但不需要立即执行的用户。
func (r *pebbleMessageProjectionRepository) scheduleTrim(key UserKey) {
	if r == nil {
		return
	}

	r.trimMu.Lock()
	if r.trimClosed || r.trimWake == nil {
		r.trimMu.Unlock()
		return
	}
	if r.dirtyUsers == nil {
		r.dirtyUsers = make(map[UserKey]struct{})
	}
	r.dirtyUsers[key] = struct{}{}
	wake := r.trimWake
	r.trimMu.Unlock()

	select {
	case wake <- struct{}{}:
	default:
	}
}

// runTrimWorker 后台 trim worker 的主循环。
//
// 工作流程：
//  1. 收到唤醒信号后，将 r.dirtyUsers 合并到本地 pending 集合
//  2. 如果 pending 数量超过 pebbleMessageTrimWorkerMaxUsers，立即执行 flush
//  3. 否则启动延迟定时器（pebbleMessageTrimWorkerDelay），等待更多 trim 请求合并
//  4. 定时器到时后批量 trim 所有 pending 的用户
//  5. trim 失败的用户会被重新调度（scheduleTrim）
//  6. 收到关闭信号时，先合并并 flush 所有剩余请求
func (r *pebbleMessageProjectionRepository) runTrimWorker() {
	defer close(r.trimDone)

	var (
		pending map[UserKey]struct{}
		timer   *time.Timer
		timerC  <-chan time.Time
	)

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}

	mergeDirtyUsers := func() {
		r.trimMu.Lock()
		if len(r.dirtyUsers) == 0 {
			r.trimMu.Unlock()
			return
		}
		if pending == nil {
			pending = make(map[UserKey]struct{}, len(r.dirtyUsers))
		}
		for key := range r.dirtyUsers {
			pending[key] = struct{}{}
		}
		r.dirtyUsers = make(map[UserKey]struct{})
		r.trimMu.Unlock()
	}

	flushPending := func() {
		if len(pending) == 0 {
			stopTimer()
			return
		}
		keys := sortedUserKeys(pending)
		pending = nil
		stopTimer()
		for _, key := range keys {
			if err := r.trimMessagesForUser(context.Background(), key, false); err != nil {
				r.scheduleTrim(key)
			}
		}
	}

	for {
		select {
		case <-r.trimWake:
			mergeDirtyUsers()
			switch {
			case len(pending) >= pebbleMessageTrimWorkerMaxUsers:
				flushPending()
			case len(pending) > 0 && timer == nil:
				timer = time.NewTimer(pebbleMessageTrimWorkerDelay)
				timerC = timer.C
			}
		case <-timerC:
			flushPending()
		case response := <-r.trimClose:
			mergeDirtyUsers()
			flushPending()
			response <- nil
			return
		}
	}
}

// lockUsers 根据用户键获取对应分片的互斥锁。
// 按分片索引排序后依次加锁，避免死锁。返回解锁函数，调用方需确保最终执行。
// 同一个分片内的多个用户共享一把锁，因此对同一分片内用户的并发操作是串行化的。
func (r *pebbleMessageProjectionRepository) lockUsers(keys []UserKey) func() {
	if r == nil || len(keys) == 0 {
		return func() {}
	}

	indexes := make([]int, 0, len(keys))
	seen := make(map[int]struct{}, len(keys))
	for _, key := range keys {
		index := pebbleProjectionShardIndex(key)
		if _, ok := seen[index]; ok {
			continue
		}
		seen[index] = struct{}{}
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		r.shardLocks[index].Lock()
	}
	return func() {
		for i := len(indexes) - 1; i >= 0; i-- {
			r.shardLocks[indexes[i]].Unlock()
		}
	}
}

// pebbleProjectionShardIndex 计算 UserKey 对应的锁分片索引。
// 使用自定义哈希函数将 NodeID 和 UserID 混合后取模，均匀分布到 pebbleProjectionLockShards 个分片。
func pebbleProjectionShardIndex(key UserKey) int {
	hash := uint64(key.NodeID)*1_146_959_810_393_466_559 ^ uint64(key.UserID)*1_099_511_628_211
	return int(hash % pebbleProjectionLockShards)
}

// sortedUserKeys 将 UserKey 集合按 (NodeID, UserID) 排序后返回。
// 保证锁获取和 trim 处理的确定性顺序，避免死锁。
func sortedUserKeys(keys map[UserKey]struct{}) []UserKey {
	ordered := make([]UserKey, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].NodeID != ordered[j].NodeID {
			return ordered[i].NodeID < ordered[j].NodeID
		}
		return ordered[i].UserID < ordered[j].UserID
	})
	return ordered
}

// messageUserStateLocked 读取或初始化指定用户的消息状态。
// 优先从 Pebble 读取已持久化的状态；如果不存在则通过遍历已有消息初始化（seed）。
func (r *pebbleMessageProjectionRepository) messageUserStateLocked(ctx context.Context, key UserKey) (pebbleMessageUserState, error) {
	state, ok, err := r.readMessageUserStateLocked(key)
	if err != nil {
		return pebbleMessageUserState{}, err
	}
	if ok {
		return state, nil
	}
	return r.seedMessageUserStateLocked(ctx, key)
}

// readMessageUserStateLocked 从 Pebble 读取指定用户的持久化消息状态。
// 状态不存在时返回 (zeroState, false, nil)，由调用方决定是否 seed。
func (r *pebbleMessageProjectionRepository) readMessageUserStateLocked(key UserKey) (pebbleMessageUserState, bool, error) {
	value, closer, err := r.db.Get(pebbleMessageUserStateKey(key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return pebbleMessageUserState{}, false, nil
		}
		return pebbleMessageUserState{}, false, fmt.Errorf("read pebble message user state: %w", err)
	}
	defer closer.Close()

	state, err := decodePebbleMessageUserState(value)
	if err != nil {
		return pebbleMessageUserState{}, false, err
	}
	return state, true, nil
}

// seedMessageUserStateLocked 通过遍历指定用户的所有消息来初始化消息状态。
// 统计消息总数并判断是否需要 trim。仅在首次加载时执行（无已有状态）。
func (r *pebbleMessageProjectionRepository) seedMessageUserStateLocked(ctx context.Context, key UserKey) (pebbleMessageUserState, error) {
	if err := key.Validate(); err != nil {
		return pebbleMessageUserState{}, err
	}
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	hardThreshold := pebbleMessageTrimHardThreshold(windowSize)
	prefix := make([]byte, 0, 17)
	prefix = append(prefix, messageUserTag)
	prefix = encodeUint64(prefix, uint64(key.NodeID))
	prefix = encodeUint64(prefix, uint64(key.UserID))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return pebbleMessageUserState{}, fmt.Errorf("open user state seed iterator: %w", err)
	}
	defer iter.Close()

	count := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return pebbleMessageUserState{}, err
		}
		count++
		if count > hardThreshold {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return pebbleMessageUserState{}, fmt.Errorf("iterate user state seed: %w", err)
	}
	return pebbleMessageUserState{
		StoredCount: int64(count),
		TrimNeeded:  count > windowSize,
	}, nil
}

// prepareMessageWrite 将一条消息写入所有 Pebble 投影索引，并更新用户状态。
// 写入的索引包括：
//   - 主键索引（pebbleMessageIDKey）：按接收者+生产者+序列号定位消息
//   - 用户维度索引（pebbleMessageUserKey）：按接收者+时间倒序检索
//   - 生产者维度索引（pebbleMessageProducerKey）：按生产者+时间倒序检索
//   - 用户状态更新（pebbleMessageUserStateKey）：更新存储计数和最大序列号
//
// 返回值：
//   - pebbleMessageUserState: 更新后的用户状态
//   - []byte: 消息序列化值，用于后续写入收件箱索引
//   - error: 错误信息
func (r *pebbleMessageProjectionRepository) prepareMessageWrite(batch *pebble.Batch, message Message, state pebbleMessageUserState) (pebbleMessageUserState, []byte, error) {
	if batch == nil {
		return pebbleMessageUserState{}, nil, fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
	}
	value, err := pebbleMessageValue(message)
	if err != nil {
		return pebbleMessageUserState{}, nil, err
	}
	if err := batch.Set(pebbleMessageIDKey(message), value, nil); err != nil {
		return pebbleMessageUserState{}, nil, fmt.Errorf("write message primary projection: %w", err)
	}
	if err := batch.Set(pebbleMessageUserKey(message), r.userIndexValue(value, message), nil); err != nil {
		return pebbleMessageUserState{}, nil, fmt.Errorf("write message user index: %w", err)
	}
	if err := batch.Set(pebbleMessageProducerKey(message), r.producerIndexValue(value, message), nil); err != nil {
		return pebbleMessageUserState{}, nil, fmt.Errorf("write message producer index: %w", err)
	}

	state.StoredCount++
	if message.Seq > state.MaxSeq {
		state.MaxSeq = message.Seq
	}
	if state.StoredCount > int64(normalizeMessageWindowSize(r.messageWindowSize)) {
		state.TrimNeeded = true
	}
	if err := batch.Set(pebbleMessageUserStateKey(message.UserKey()), encodePebbleMessageUserState(state), nil); err != nil {
		return pebbleMessageUserState{}, nil, fmt.Errorf("write message user state: %w", err)
	}
	return state, value, nil
}

// prepareInboxWrites 为消息写入收件箱索引。
// 根据接收者类型不同，行为略有差异：
//   - 普通可登录用户：直接写入该用户的收件箱
//   - 频道（Channel）：遍历频道订阅者列表，为每个在消息时间之前订阅的用户写入收件箱
func (r *pebbleMessageProjectionRepository) prepareInboxWrites(ctx context.Context, batch *pebble.Batch, message Message, recipient User, primaryValue []byte) error {
	if batch == nil {
		return fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
	}

	switch {
	case recipient.CanLogin():
		return r.writeInboxEntry(batch, recipient.Key(), message, false, primaryValue)
	case recipient.Role == RoleChannel:
		subscribers, err := r.subscriptions.ListChannelSubscribers(ctx, recipient.Key())
		if err != nil {
			return err
		}
		for _, subscription := range subscribers {
			if message.CreatedAt.Compare(subscription.SubscribedAt) < 0 {
				continue
			}
			if err := r.writeInboxEntry(batch, subscription.Subscriber, message, true, primaryValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeInboxEntry 为单个收件人写入收件箱索引项。
// 当 trackSource 为 true 时，还会写入一条收件箱来源反向索引（用于频道消息的级联删除）。
func (r *pebbleMessageProjectionRepository) writeInboxEntry(batch *pebble.Batch, owner UserKey, message Message, trackSource bool, primaryValue []byte) error {
	if err := owner.Validate(); err != nil {
		return err
	}

	inboxKey := pebbleInboxUserKey(owner, message)
	if err := batch.Set(inboxKey, r.inboxIndexValue(primaryValue, message, trackSource), nil); err != nil {
		return fmt.Errorf("write inbox index: %w", err)
	}
	if !trackSource {
		return nil
	}
	if err := batch.Set(pebbleInboxSourceKey(message, owner), append([]byte(nil), inboxKey...), nil); err != nil {
		return fmt.Errorf("write inbox source index: %w", err)
	}
	return nil
}

// deleteMessageInboxEntries 删除指定消息的所有收件箱索引项。
// 如果 directOwner 为 true（普通用户），直接删除对应的收件箱键和来源键。
// 如果为 false（频道），通过遍历来源反向索引删除所有订阅者的收件箱副本。
func (r *pebbleMessageProjectionRepository) deleteMessageInboxEntries(ctx context.Context, batch *pebble.Batch, owner UserKey, message Message, directOwner bool) error {
	if directOwner {
		if err := batch.Delete(pebbleInboxUserKey(owner, message), nil); err != nil {
			return fmt.Errorf("delete inbox index: %w", err)
		}
		if err := batch.Delete(pebbleInboxSourceKey(message, owner), nil); err != nil {
			return fmt.Errorf("delete inbox source index: %w", err)
		}
		return nil
	}

	prefix := pebbleInboxSourcePrefix(message)
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return fmt.Errorf("open inbox source iterator: %w", err)
	}
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		inboxKey := append([]byte(nil), iter.Value()...)
		sourceKey := append([]byte(nil), iter.Key()...)
		if err := batch.Delete(inboxKey, nil); err != nil {
			return fmt.Errorf("delete inbox index: %w", err)
		}
		if err := batch.Delete(sourceKey, nil); err != nil {
			return fmt.Errorf("delete inbox source index: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterate inbox source index: %w", err)
	}
	return nil
}

// trimMessagesForUser 对指定用户执行消息修剪（外部加锁版本）。
// 获取用户锁后委托给 trimMessagesForUserLocked 执行实际 trim 逻辑。
func (r *pebbleMessageProjectionRepository) trimMessagesForUser(ctx context.Context, key UserKey, forceSync bool) error {
	unlock := r.lockUsers([]UserKey{key})
	defer unlock()
	return r.trimMessagesForUserLocked(ctx, key, forceSync)
}

// trimMessagesForUserLocked 在已持有用户锁的上下文中执行消息修剪。
//
// 修剪策略：
//  1. 读取当前用户的消息状态
//  2. 如果不需要 trim 且未超过硬阈值，直接跳过
//  3. 列出该用户所有已存储消息（按时间倒序）
//  4. 如果消息总数 <= windowSize，仅更新状态（纠正计数偏差），不删除消息
//  5. 否则，删除超出 windowSize 的旧消息及其收件箱索引
//  6. 更新用户状态中的 StoredCount 和 MaxSeq
//  7. 记录修剪统计信息（messageTrim.RecordMessageTrim）
//
// forceSync 控制是否等待 Pebble 数据落盘。
func (r *pebbleMessageProjectionRepository) trimMessagesForUserLocked(ctx context.Context, key UserKey, forceSync bool) error {
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	state, err := r.messageUserStateLocked(ctx, key)
	if err != nil {
		return err
	}
	if !forceSync && !state.TrimNeeded && state.StoredCount <= int64(pebbleMessageTrimThreshold(windowSize)) {
		return nil
	}

	messages, err := r.listStoredMessagesByUser(ctx, key, nil)
	if err != nil {
		return err
	}

	if len(messages) <= windowSize {
		updated := state
		updated.StoredCount = int64(len(messages))
		updated.TrimNeeded = false
		if maxSeq := maxMessageSequence(messages); maxSeq > updated.MaxSeq {
			updated.MaxSeq = maxSeq
		}
		if updated == state {
			return nil
		}
		batch := r.db.NewBatch()
		if err := batch.Set(pebbleMessageUserStateKey(key), encodePebbleMessageUserState(updated), nil); err != nil {
			_ = batch.Close()
			return fmt.Errorf("write message user state: %w", err)
		}
		if err := applyPebbleBatch(batch, r.writes, forceSync); err != nil {
			return fmt.Errorf("commit message state refresh: %w", err)
		}
		return nil
	}

	directInboxOwner := false
	recipient, err := r.userRepository.GetUser(ctx, key, false)
	switch {
	case err == nil:
		directInboxOwner = recipient.CanLogin()
	case errors.Is(err, ErrNotFound):
		directInboxOwner = false
	default:
		return err
	}

	batch := r.db.NewBatch()
	for _, message := range messages[windowSize:] {
		if err := r.deleteMessageInboxEntries(ctx, batch, key, message, directInboxOwner); err != nil {
			_ = batch.Close()
			return err
		}
		for _, key := range pebbleMessageKeys(message) {
			if err := batch.Delete(key, nil); err != nil {
				_ = batch.Close()
				return fmt.Errorf("delete trimmed message: %w", err)
			}
		}
	}
	updated := state
	updated.StoredCount = int64(windowSize)
	updated.TrimNeeded = false
	if maxSeq := maxMessageSequence(messages[:windowSize]); maxSeq > updated.MaxSeq {
		updated.MaxSeq = maxSeq
	}
	if err := batch.Set(pebbleMessageUserStateKey(key), encodePebbleMessageUserState(updated), nil); err != nil {
		_ = batch.Close()
		return fmt.Errorf("write trimmed message state: %w", err)
	}
	if err := applyPebbleBatch(batch, r.writes, forceSync); err != nil {
		return fmt.Errorf("commit message trim: %w", err)
	}
	return r.messageTrim.RecordMessageTrim(ctx, int64(len(messages)-windowSize))
}

// pebbleMessageTrimHardThreshold 计算硬触发 trim 的阈值。
// 当用户消息数超过此阈值时，在 processLocalMessageBatch 中同步执行 trim（而非调度到后台）。
// 硬阈值 = windowSize + pebbleMessageTrimHardSlack，提供 windowSize 之外的松弛容量。
func pebbleMessageTrimHardThreshold(windowSize int) int {
	if windowSize <= pebbleMessageTrimHardSlack {
		return windowSize + pebbleMessageTrimHardSlack
	}
	return windowSize + pebbleMessageTrimHardSlack
}

// pebbleMessageUserStateKey 构造用户消息状态的 Pebble 键。
// 格式：[metaMessageUserStateTag][NodeID:8 BE][UserID:8 BE]（共 17 字节）
func pebbleMessageUserStateKey(key UserKey) []byte {
	buf := make([]byte, 0, 17)
	buf = append(buf, metaMessageUserStateTag)
	buf = encodeUint64(buf, uint64(key.NodeID))
	return encodeUint64(buf, uint64(key.UserID))
}

// encodePebbleMessageUserState 将用户消息状态编码为 18 字节二进制值。
// 格式：[Version:1][StoredCount:8 BE][MaxSeq:8 BE][TrimNeededFlag:1]
func encodePebbleMessageUserState(state pebbleMessageUserState) []byte {
	value := make([]byte, 18)
	value[0] = pebbleMessageUserStateVersion
	binary.BigEndian.PutUint64(value[1:9], uint64(state.StoredCount))
	binary.BigEndian.PutUint64(value[9:17], uint64(state.MaxSeq))
	if state.TrimNeeded {
		value[17] = 1
	}
	return value
}

// decodePebbleMessageUserState 从 18 字节二进制值解码用户消息状态。
// 校验版本号和长度，不匹配时返回错误。
func decodePebbleMessageUserState(value []byte) (pebbleMessageUserState, error) {
	if len(value) != 18 {
		return pebbleMessageUserState{}, fmt.Errorf("%w: invalid pebble message user state length %d", ErrInvalidInput, len(value))
	}
	if value[0] != pebbleMessageUserStateVersion {
		return pebbleMessageUserState{}, fmt.Errorf("%w: unsupported pebble message user state version %d", ErrInvalidInput, value[0])
	}
	return pebbleMessageUserState{
		StoredCount: int64(binary.BigEndian.Uint64(value[1:9])),
		MaxSeq:      int64(binary.BigEndian.Uint64(value[9:17])),
		TrimNeeded:  value[17] != 0,
	}, nil
}

// maxMessageSequence 返回消息切片中的最大序列号。空切片返回 0。
func maxMessageSequence(messages []Message) int64 {
	var maxSeq int64
	for _, message := range messages {
		if message.Seq > maxSeq {
			maxSeq = message.Seq
		}
	}
	return maxSeq
}
