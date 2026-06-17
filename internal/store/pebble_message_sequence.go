package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

// pebbleMessageSequenceRepository 消息序列号仓库。
// 为每个用户的消息分配单调递增的序列号，支持预留/提交两阶段模式。
//
// 序列号的确定方式：
//  1. 优先从内存缓存 r.next 中读取（避免每次分配都读 Pebble）
//  2. 缓存未命中时从 Pebble 读取存储的序列值
//  3. Pebble 中不存在时，从已有消息的最大序列号或 SQL 后端的计数器初始化
//
// 并发安全：通过 sync.Mutex 保护 r.next 的读写。
type pebbleMessageSequenceRepository struct {
	// db Pebble 数据库实例
	db *pebble.DB
	// sqlDB SQL 数据库实例（可选），用于从 SQL 后端的消息表读取历史序列号做初始化
	sqlDB *sql.DB
	// writes Pebble 写入协调器，负责序列号键的写入
	writes *pebbleWriteCoordinator
	// mu 保护 next 缓存的互斥锁
	mu sync.Mutex
	// next 内存缓存的序列号值，key 为序列号键的字符串形式，value 为下一个可用序列号
	next map[string]int64
}

// NextSequenceTx 在事务中获取下一个消息序列号并提交到 Pebble。
// 用于需要跨 SQL 事务和 Pebble 写入的路径。执行步骤：
//  1. 加载当前序列号（缓存 > Pebble > 从已有消息初始化）
//  2. 将 (next+1) 写入 Pebble batch 并提交
//  3. 更新内存缓存
//  4. 返回当前序列号（next）
//
// 调用方需要在 SQL 事务上下文中处理并发一致性。
func (r *pebbleMessageSequenceRepository) NextSequenceTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	if err := key.Validate(); err != nil {
		return 0, err
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("%w: user id and node id are required for message sequence", ErrInvalidInput)
	}
	if r == nil || r.db == nil {
		return 0, fmt.Errorf("pebble message sequence repository is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	sequenceKey := pebbleMessageSequenceKey(key, nodeID)
	cacheKey := string(sequenceKey)
	next, ok, err := r.loadNextSequenceLocked(ctx, tx, key, nodeID, sequenceKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		next = 1
	}

	batch := r.db.NewBatch()
	if err := batch.Set(sequenceKey, encodeInt64(next+1), nil); err != nil {
		return 0, fmt.Errorf("write pebble message sequence: %w", err)
	}
	if err := applyPebbleBatch(batch, r.writes, false); err != nil {
		return 0, fmt.Errorf("commit pebble message sequence: %w", err)
	}

	r.storeCommittedNextLocked(cacheKey, next+1)
	return next, nil
}

// LoadNextSequence 加载下一个可用序列号但不写入。
// 用于本地消息写入器的批处理路径：先预留序列号（只读），等批次提交时再写入。
//
// 返回值：
//   - cacheKey: 序列号键的字符串形式，用于后续更新缓存
//   - key: 序列号在 Pebble 中的存储键（原始字节）
//   - next: 下一个可用序列号
//   - error: 错误信息
func (r *pebbleMessageSequenceRepository) LoadNextSequence(ctx context.Context, key UserKey, nodeID int64) (string, []byte, int64, error) {
	if err := key.Validate(); err != nil {
		return "", nil, 0, err
	}
	if nodeID <= 0 {
		return "", nil, 0, fmt.Errorf("%w: user id and node id are required for message sequence", ErrInvalidInput)
	}
	if r == nil || r.db == nil {
		return "", nil, 0, fmt.Errorf("pebble message sequence repository is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return "", nil, 0, err
	}

	sequenceKey := pebbleMessageSequenceKey(key, nodeID)
	cacheKey := string(sequenceKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	next, ok, err := r.loadNextSequenceLocked(ctx, nil, key, nodeID, sequenceKey)
	if err != nil {
		return "", nil, 0, err
	}
	if !ok {
		next = 1
	}
	return cacheKey, sequenceKey, next, nil
}

// StoreCommittedNextByCacheKey 更新多个序列号的内存缓存。
// 在批处理成功提交后调用，将预留阶段分配出去的序列号标记为已提交。
// 防止下次对同一个用户的序列号分配时重复使用已分配的值。
func (r *pebbleMessageSequenceRepository) StoreCommittedNextByCacheKey(nextByCacheKey map[string]int64) {
	if len(nextByCacheKey) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	for cacheKey, next := range nextByCacheKey {
		r.storeCommittedNextLocked(cacheKey, next)
	}
}

// loadNextSequenceLocked 在已持有 r.mu 锁的上下文中加载下一个序列号。
// 查找顺序：内存缓存 > Pebble 存储 > 从已有数据初始化（seed）。
// 返回 (nextValue, found, error)，found 为 false 表示需要调用方初始化默认值。
func (r *pebbleMessageSequenceRepository) loadNextSequenceLocked(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64, sequenceKey []byte) (int64, bool, error) {
	cacheKey := string(sequenceKey)
	if next, ok := r.next[cacheKey]; ok {
		return next, true, nil
	}

	if next, ok, err := r.readStoredNextSequenceLocked(sequenceKey); err != nil {
		return 0, false, err
	} else if ok {
		if r.next == nil {
			r.next = make(map[string]int64)
		}
		r.next[cacheKey] = next
		return next, true, nil
	}

	next, err := r.seedNextSequenceLocked(ctx, tx, key, nodeID)
	if err != nil {
		return 0, false, err
	}
	return next, true, nil
}

// storeCommittedNextLocked 在已持有 r.mu 锁的上下文中更新单个序列号的内存缓存。
func (r *pebbleMessageSequenceRepository) storeCommittedNextLocked(cacheKey string, next int64) {
	if r.next == nil {
		r.next = make(map[string]int64)
	}
	r.next[cacheKey] = next
}

// readStoredNextSequenceLocked 从 Pebble 读取已存储的序列号值。
// 如果键不存在则在返回 (0, false, nil)，由调用方决定是否需要初始化。
func (r *pebbleMessageSequenceRepository) readStoredNextSequenceLocked(sequenceKey []byte) (int64, bool, error) {
	value, closer, err := r.db.Get(sequenceKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read pebble message sequence: %w", err)
	}
	defer closer.Close()

	next := decodeInt64(value)
	if next <= 0 {
		return 0, false, fmt.Errorf("%w: invalid stored pebble message sequence %d", ErrInvalidInput, next)
	}
	return next, true, nil
}

// seedNextSequenceLocked 从多个数据源初始化序列号值。
// 适用于新用户或数据迁移场景。查找并取最大值：
//  1. 从 Pebble 已有的消息索引中查找最大序列号（readProjectedNextSequenceLocked）
//  2. 从 SQL 后端的 legacy 消息计数器读取（readStoredMessageCounterNextSeq）
//  3. 从 SQL 后端的消息投影表读取已投影消息的最大序列号（readProjectedMessageNextSeq）
//
// 以上步骤用于确保从 SQL 后端迁移到 Pebble 后端时，序列号不会回退。
func (r *pebbleMessageSequenceRepository) seedNextSequenceLocked(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	next := int64(1)

	if pebbleNext, ok, err := r.readProjectedNextSequenceLocked(ctx, key, nodeID); err != nil {
		return 0, err
	} else if ok && pebbleNext > next {
		next = pebbleNext
	}

	var querier sqlQueryRowContext
	if tx != nil {
		querier = tx
	} else {
		querier = r.sqlDB
	}
	if querier == nil {
		return next, nil
	}

	if legacyCounterNext, ok, err := readStoredMessageCounterNextSeq(ctx, querier, key, nodeID); err != nil {
		return 0, err
	} else if ok && legacyCounterNext > next {
		next = legacyCounterNext
	}

	sqlNext, err := readProjectedMessageNextSeq(ctx, querier, key, nodeID)
	if err != nil {
		return 0, err
	}
	if sqlNext > next {
		next = sqlNext
	}

	return next, nil
}

// readProjectedNextSequenceLocked 从 Pebble 已投影的消息索引中读取最大序列号。
// 通过遍历指定 (UserKey, nodeID) 的最后一条消息键来获取序列号。
// 用于新用户(0)的序列号初始化，确保新分配的序列号不会与已有消息冲突。
func (r *pebbleMessageSequenceRepository) readProjectedNextSequenceLocked(ctx context.Context, key UserKey, nodeID int64) (int64, bool, error) {
	prefix := pebbleMessageIDPrefix(key, nodeID)
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, false, fmt.Errorf("open pebble message sequence seed iterator: %w", err)
	}
	defer iter.Close()

	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, false, fmt.Errorf("iterate pebble message sequence seed: %w", err)
		}
		return 0, false, nil
	}
	_, storedNodeID, seq, err := parsePebbleMessageIDKey(iter.Key())
	if err != nil {
		return 0, false, err
	}
	if storedNodeID != nodeID {
		return 0, false, fmt.Errorf("%w: unexpected pebble message producer %d while seeding sequence for %d", ErrInvalidInput, storedNodeID, nodeID)
	}
	return seq + 1, true, nil
}
