package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

// storeBackend 是双后端（SQLite/Pebble）的抽象接口，定义所有存储操作契约。
// 由 sqliteStoreBackend 和 pebbleStoreBackend 实现。
// 在双后端架构中 storeBackend 分为两层：
//
//	上层 Store 提供事务管理和业务校验，下层 storeBackend 提供引擎特定的持久化实现。
//
// 上层通过该接口调用下层，切換后端只需替换接口实现，不影响业务逻辑。
type storeBackend interface {
	Name() string
	Bind(storeBackendBindings) error
	// CreateMessage 创建一条消息：校验用户、黑名单、分配 seq、写入事件日志。
	CreateMessage(context.Context, *Store, CreateMessageParams) (Message, Event, error)
	// EventLog 返回事件日志仓库接口。
	EventLog() EventLogRepository
	// MessageProjection 返回消息投影仓库接口。
	MessageProjection() MessageProjectionRepository
	// NextMessageSeqTx 在事务中分配消息的下一个序列号（按 UserKey+NodeID 递增）。
	NextMessageSeqTx(context.Context, *sql.Tx, UserKey, int64) (int64, error)
	// InsertLocalEventTx 在事务中插入一条本地事件，生成 EventID 和 OriginNodeID。
	InsertLocalEventTx(context.Context, *sql.Tx, Event) (Event, error)
	// StoreReplicatedEventTx 在事务中存储来自 peer 的复制事件。
	// 返回 (完整事件, 是否新插入, 错误)。如果事件已存在（唯一约束冲突），inserted 为 false。
	StoreReplicatedEventTx(context.Context, *sql.Tx, *internalproto.ReplicatedEvent, Event) (Event, bool, error)
	// ListPendingProjectionEvents 列出待重试的投影事件列表，按失败时间排序，最多返回 limit 条。
	ListPendingProjectionEvents(context.Context, *sql.DB, int) ([]Event, error)
	// ListLocalOriginEventStats 返回本地已知的所有来源节点的事件统计（最新事件 ID 和总数）。
	ListLocalOriginEventStats(context.Context, *sql.DB) (map[int64]localOriginEventStats, error)
	// CountUnconfirmedOriginEvents 统计某来源节点尚未被本节点确认的事件数。
	// ackedEventID 是已确认的最新事件 ID，为 0 时使用 fallbackCount 返回。
	CountUnconfirmedOriginEvents(context.Context, *sql.DB, int64, int64, int64) (int64, error)
	// PruneEventLogOrigin 裁剪某来源节点的事件日志，只保留最近的 maxEvents 条记录。
	PruneEventLogOrigin(context.Context, *sql.DB, *clock.Clock, int64, int) (int64, error)
	Close() error
}

// txMessageProjectionRepository 是事务内消息投影的接口窄化。
type txMessageProjectionRepository interface {
	applyMessageCreatedTx(context.Context, *sql.Tx, Message) error
}

// storeBackendBindings 包含 Bind() 时注入给后端的依赖项。
type storeBackendBindings struct {
	NodeID            int64                  // 本节点 ID，用于标识事件的来源节点
	Clock             *clock.Clock           // HLC 混合逻辑时钟，提供单调递增且因果一致的时间戳
	IDs               *clock.IDGenerator     // 事件 ID 生成器，用于生成全局唯一的事件 ID
	MessageWindowSize int                    // 消息滑动窗口大小，控制每个会话保留的最大消息数
	UserRepository    UserRepository         // 用户存储仓库
	Subscriptions     SubscriptionRepository // 订阅存储仓库
	Blacklists        BlacklistRepository    // 黑名单存储仓库
	MessageTrim       MessageTrimRepository  // 消息修剪存储仓库
}

// newStoreBackend 根据引擎名称创建对应的后端实现。
func newStoreBackend(engine string, db *sql.DB, pebbleDB *pebble.DB, pebbleProfile PebbleProfile) (storeBackend, error) {
	switch engine {
	case EngineSQLite:
		return &sqliteStoreBackend{db: db}, nil
	case EnginePebble:
		if pebbleDB == nil {
			return nil, fmt.Errorf("pebble backend requires pebble db")
		}
		return &pebbleStoreBackend{
			db:      pebbleDB,
			sqlDB:   db,
			profile: pebbleProfile,
			writes:  newPebbleWriteCoordinator(pebbleDB),
		}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported store engine %q", ErrInvalidInput, engine)
	}
}

// sqliteStoreBackend 是纯 SQLite 后端实现，所有数据存储在 SQLite 中。
// 适合小规模部署或不需要高性能消息写入的场景。
type sqliteStoreBackend struct {
	db                *sql.DB                     // SQLite 数据库连接
	nodeID            int64                       // 本节点 ID
	clock             *clock.Clock                // HLC 混合逻辑时钟
	ids               *clock.IDGenerator          // 事件 ID 生成器
	messageWindowSize int                         // 消息滑动窗口大小
	eventLog          *sqliteEventLogRepository   // SQLite 事件日志仓库
	messageProjection MessageProjectionRepository // 消息投影仓库
}

// Name 返回后端引擎名称。
func (b *sqliteStoreBackend) Name() string {
	return EngineSQLite
}

// Bind 注入运行时依赖并初始化事件日志和消息投影 Repository。
func (b *sqliteStoreBackend) Bind(bindings storeBackendBindings) error {
	b.nodeID = bindings.NodeID
	b.clock = bindings.Clock
	b.ids = bindings.IDs
	b.messageWindowSize = bindings.MessageWindowSize
	b.messageProjection = &sqliteMessageProjectionRepository{
		db:                b.db,
		clock:             b.clock,
		messageWindowSize: b.messageWindowSize,
		userRepository:    bindings.UserRepository,
		subscriptions:     bindings.Subscriptions,
		blacklists:        bindings.Blacklists,
	}
	b.eventLog = &sqliteEventLogRepository{
		db:     b.db,
		ids:    b.ids,
		nodeID: b.nodeID,
		clock:  b.clock,
	}
	return nil
}

// EventLog 返回 sqlite 实现的事件日志仓库。
func (b *sqliteStoreBackend) EventLog() EventLogRepository {
	return b.eventLog
}

// MessageProjection 返回 sqlite 实现的消息投影仓库。
func (b *sqliteStoreBackend) MessageProjection() MessageProjectionRepository {
	return b.messageProjection
}

// CreateMessage 创建一条消息：先校验用户和黑名单，再在事务中写入事件日志并投影消息。
// 支持两种模式：若消息投影接口支持事务内投影（txMessageProjectionRepository）则走快速路径，
// 否则使用传统路径（先提交事件再异步投影）。
func (b *sqliteStoreBackend) CreateMessage(ctx context.Context, s *Store, params CreateMessageParams) (Message, Event, error) {
	projection, ok := b.messageProjection.(txMessageProjectionRepository)
	if !ok {
		return b.createMessageLegacy(ctx, s, params)
	}
	return b.createMessageFast(ctx, s, params, projection)
}

// createMessageFast 是消息创建的快速路径：在 SQLite 保存点内尝试同步投影消息。
// 若投影失败则回滚到保存点并记录待重试投影，不阻塞消息创建流程。
func (b *sqliteStoreBackend) createMessageFast(ctx context.Context, s *Store, params CreateMessageParams, projection txMessageProjectionRepository) (Message, Event, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Message{}, Event{}, fmt.Errorf("begin create message: %w", err)
	}
	defer tx.Rollback()

	message, event, err := b.createMessageEventTx(ctx, s, tx, params)
	if err != nil {
		return Message{}, Event{}, err
	}

	const projectionSavepoint = "turntf_create_message_projection"
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+projectionSavepoint); err != nil {
		return Message{}, Event{}, fmt.Errorf("begin create message projection savepoint: %w", err)
	}
	projectionErr := projection.applyMessageCreatedTx(ctx, tx, message)
	if projectionErr != nil {
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+projectionSavepoint); err != nil {
			return Message{}, Event{}, fmt.Errorf("rollback create message projection savepoint: %w", err)
		}
		if err := s.recordPendingProjectionTx(ctx, tx, event, projectionErr); err != nil {
			return Message{}, Event{}, fmt.Errorf("record deferred message projection: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+projectionSavepoint); err != nil {
			return Message{}, Event{}, fmt.Errorf("release deferred create message projection savepoint: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return Message{}, Event{}, fmt.Errorf("commit create message: %w", err)
		}
		return message, event, fmt.Errorf("%w: %v", ErrProjectionDeferred, projectionErr)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+projectionSavepoint); err != nil {
		return Message{}, Event{}, fmt.Errorf("release create message projection savepoint: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Message{}, Event{}, fmt.Errorf("commit create message: %w", err)
	}
	return message, event, nil
}

// createMessageLegacy 是消息创建的传统路径：先提交事件，再在事务外同步投影。
// 当消息投影仓库未实现 txMessageProjectionRepository 接口时使用此路径。
func (b *sqliteStoreBackend) createMessageLegacy(ctx context.Context, s *Store, params CreateMessageParams) (Message, Event, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Message{}, Event{}, fmt.Errorf("begin create message: %w", err)
	}
	defer tx.Rollback()

	message, event, err := b.createMessageEventTx(ctx, s, tx, params)
	if err != nil {
		return Message{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return Message{}, Event{}, fmt.Errorf("commit create message: %w", err)
	}
	if err := s.projectMessageEvent(ctx, event); err != nil {
		if recordErr := s.recordPendingProjection(ctx, event, err); recordErr != nil {
			return Message{}, Event{}, fmt.Errorf("record deferred message projection: %w", recordErr)
		}
		return message, event, fmt.Errorf("%w: %v", ErrProjectionDeferred, err)
	}
	return message, event, nil
}

// createMessageEventTx 在事务中执行消息创建的核心逻辑：
// 校验收件人和发件人是否存在、检查黑名单、分配消息序列号、构建 Message 并写入事件日志。
func (b *sqliteStoreBackend) createMessageEventTx(ctx context.Context, s *Store, tx *sql.Tx, params CreateMessageParams) (Message, Event, error) {
	recipient, err := s.getUserTx(ctx, tx, params.UserKey, false)
	if err != nil {
		return Message{}, Event{}, err
	}

	sender := User{}
	senderExists := false
	if params.Sender == params.UserKey {
		sender = recipient
		senderExists = true
	} else {
		sender, err = s.getUserTx(ctx, tx, params.Sender, false)
		switch {
		case err == nil:
			senderExists = true
		case errors.Is(err, ErrNotFound):
		default:
			return Message{}, Event{}, err
		}
	}

	if recipient.CanLogin() && senderExists && sender.Role == RoleUser && sender.Key() != recipient.Key() {
		blocked, err := s.isBlockedByRecipientTx(ctx, tx, recipient.Key(), sender.Key(), nil)
		if err != nil {
			return Message{}, Event{}, err
		}
		if blocked {
			return Message{}, Event{}, ErrBlockedByBlacklist
		}
	}

	now := s.clock.Now()
	seq, err := s.nextMessageSeqTx(ctx, tx, params.UserKey, s.nodeID)
	if err != nil {
		return Message{}, Event{}, err
	}
	message := Message{
		Recipient: params.UserKey,
		NodeID:    s.nodeID,
		Seq:       seq,
		Sender:    params.Sender,
		Body:      append([]byte(nil), params.Body...),
		CreatedAt: now,
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeMessageCreated,
		Aggregate:       "message",
		AggregateNodeID: message.NodeID,
		AggregateID:     message.Seq,
		HLC:             now,
		Body:            messageCreatedProtoFromMessage(message),
	})
	if err != nil {
		return Message{}, Event{}, err
	}
	return message, event, nil
}

// NextMessageSeqTx 在事务中为指定用户+节点分配下一个消息序列号（SQLite 实现）。
func (b *sqliteStoreBackend) NextMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	return nextSQLiteMessageSeq(ctx, tx, key, nodeID)
}

// InsertLocalEventTx 在事务中插入一条本地事件：生成 EventID 和 OriginNodeID，
// 写入 event_log 表后获取自增 Sequence，同时更新来源节点游标（origin_cursors）。
func (b *sqliteStoreBackend) InsertLocalEventTx(ctx context.Context, tx *sql.Tx, event Event) (Event, error) {
	if b.ids == nil {
		return Event{}, fmt.Errorf("append event before id generator initialization")
	}
	if b.clock == nil {
		return Event{}, fmt.Errorf("append event before clock initialization")
	}

	event.EventID = b.ids.Next()
	event.OriginNodeID = b.nodeID

	value, err := eventLogValue(event)
	if err != nil {
		return Event{}, err
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO event_log(event_id, origin_node_id, value)
VALUES(?, ?, ?)
`, event.EventID, event.OriginNodeID, value)
	if err != nil {
		return Event{}, fmt.Errorf("insert event: %w", err)
	}

	event.Sequence, err = result.LastInsertId()
	if err != nil {
		return Event{}, fmt.Errorf("read event sequence: %w", err)
	}
	if err := upsertOriginCursorTx(ctx, tx, event.OriginNodeID, event.EventID, b.clock.Now().String()); err != nil {
		return Event{}, fmt.Errorf("record local origin cursor: %w", err)
	}
	return event, nil
}

// StoreReplicatedEventTx 在事务中存储来自 peer 的复制事件。
// 将原始 ReplicatedEvent 序列化后插入 event_log，利用唯一约束（origin_node_id, event_id）去重。
// 返回（解码后的事件, 是否新插入, 错误）。
func (b *sqliteStoreBackend) StoreReplicatedEventTx(ctx context.Context, tx *sql.Tx, rawEvent *internalproto.ReplicatedEvent, decoded Event) (Event, bool, error) {
	if rawEvent == nil {
		return Event{}, false, fmt.Errorf("%w: replicated event cannot be nil", ErrInvalidInput)
	}
	value, err := gproto.Marshal(rawEvent)
	if err != nil {
		return Event{}, false, fmt.Errorf("marshal replicated event: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO event_log(event_id, origin_node_id, value)
VALUES(?, ?, ?)
`, rawEvent.EventId, rawEvent.OriginNodeId, value)
	if err != nil {
		if isUniqueConstraint(err) {
			return decoded, false, nil
		}
		return Event{}, false, fmt.Errorf("insert replicated event log: %w", err)
	}
	decoded.Sequence, err = result.LastInsertId()
	if err != nil {
		return Event{}, false, fmt.Errorf("read replicated event sequence: %w", err)
	}
	return decoded, true, nil
}

// ListPendingProjectionEvents 联表查询 pending_projections 和 event_log，
// 按失败时间升序返回待重试投影的事件列表，用于后台重试失败的投影操作。
func (b *sqliteStoreBackend) ListPendingProjectionEvents(ctx context.Context, db *sql.DB, limit int) ([]Event, error) {
	rows, err := db.QueryContext(ctx, `
SELECT e.sequence, e.event_id, e.origin_node_id, e.value
FROM pending_projections p
JOIN event_log e
  ON e.origin_node_id = p.origin_node_id
 AND e.event_id = p.event_id
ORDER BY p.last_failed_at_hlc ASC, p.origin_node_id ASC, p.event_id ASC
LIMIT ?
`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending projection events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending projection events: %w", err)
	}
	return events, nil
}

// ListLocalOriginEventStats 按来源节点分组统计事件日志，返回每个来源的最新事件 ID 和事件总数。
func (b *sqliteStoreBackend) ListLocalOriginEventStats(ctx context.Context, db *sql.DB) (map[int64]localOriginEventStats, error) {
	rows, err := db.QueryContext(ctx, `
SELECT origin_node_id, COALESCE(MAX(event_id), 0), COUNT(*)
FROM event_log
GROUP BY origin_node_id
ORDER BY origin_node_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query local origin event stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[int64]localOriginEventStats)
	for rows.Next() {
		var originNodeID int64
		var item localOriginEventStats
		if err := rows.Scan(&originNodeID, &item.LastEventID, &item.EventCount); err != nil {
			return nil, fmt.Errorf("scan local origin event stats: %w", err)
		}
		stats[originNodeID] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate local origin event stats: %w", err)
	}
	return stats, nil
}

// CountUnconfirmedOriginEvents 统计某来源节点尚未被本节点确认的事件数。
// ackedEventID 是已确认的最新事件 ID，为 0 时使用 fallbackCount 作为估算值返回。
func (b *sqliteStoreBackend) CountUnconfirmedOriginEvents(ctx context.Context, db *sql.DB, originNodeID, ackedEventID, fallbackCount int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, nil
	}
	if ackedEventID <= 0 {
		return fallbackCount, nil
	}

	var count int64
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM event_log
WHERE origin_node_id = ? AND event_id > ?
`, originNodeID, ackedEventID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count unconfirmed origin events: %w", err)
	}
	return count, nil
}

// PruneEventLogOrigin 裁剪某来源节点的事件日志：当事件数超过 maxEvents 时，
// 删除最早的多余事件，记录截断边界到 event_log_truncation_meta 和统计到 event_log_trim_stats。
func (b *sqliteStoreBackend) PruneEventLogOrigin(ctx context.Context, db *sql.DB, clk *clock.Clock, originNodeID int64, maxEvents int) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin sqlite event log prune: %w", err)
	}
	defer tx.Rollback()

	var count int64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM event_log
WHERE origin_node_id = ?
`, originNodeID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count sqlite event log rows: %w", err)
	}
	if count <= int64(maxEvents) {
		return 0, nil
	}

	trimCount := count - int64(maxEvents)
	var truncatedBefore int64
	if err := tx.QueryRowContext(ctx, `
SELECT event_id
FROM event_log
WHERE origin_node_id = ?
ORDER BY event_id ASC
LIMIT 1 OFFSET ?
`, originNodeID, trimCount-1).Scan(&truncatedBefore); err != nil {
		return 0, fmt.Errorf("query sqlite event log truncation boundary: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
DELETE FROM event_log
WHERE origin_node_id = ? AND event_id <= ?
`, originNodeID, truncatedBefore)
	if err != nil {
		return 0, fmt.Errorf("delete sqlite event log rows: %w", err)
	}
	trimmed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count deleted sqlite event log rows: %w", err)
	}
	if trimmed > 0 {
		now := clk.Now().String()
		if err := upsertEventLogTruncationTx(ctx, tx, originNodeID, truncatedBefore, now); err != nil {
			return 0, err
		}
		if err := recordEventLogTrimTx(ctx, tx, trimmed, now); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit sqlite event log prune: %w", err)
	}
	return trimmed, nil
}

// Close 关闭 SQLite 后端（无额外资源需要释放）。
func (b *sqliteStoreBackend) Close() error {
	return nil
}

// pebbleStoreBackend 是 Pebble+SQLite 混合后端实现。
// 高性能消息数据和事件日志存储在 Pebble KV 中，关系型数据（用户、附件、元数据）存储在 SQLite 中。
// 使用异步 goroutine 批处理消息写入以提高吞吐，适用于高并发场景。
type pebbleStoreBackend struct {
	db                    *pebble.DB                          // Pebble KV 数据库实例
	sqlDB                 *sql.DB                             // SQLite 数据库实例（用于关系型数据）
	profile               PebbleProfile                       // Pebble 引擎配置
	writes                *pebbleWriteCoordinator             // Pebble 批量写入协调器，合并小写入以减少写放大
	eventLog              *pebbleEventLogRepository           // Pebble 事件日志仓库
	messageProjection     MessageProjectionRepository         // 消息投影接口
	messageProjectionRepo *pebbleMessageProjectionRepository  // Pebble 消息投影仓库（内含后台修剪 worker）
	messageSequences      *pebbleMessageSequenceRepository    // 消息序列号仓库
	peerAckCursors        *pebblePeerAckCursorRepository      // 对等节点确认游标仓库
	originCursors         *pebbleOriginCursorRepository       // 来源节点事件游标仓库
	pendingProjections    *pebblePendingProjectionRepository  // 待重试投影事件仓库
	localMessageRequests  chan pebbleLocalMessageWriteRequest // 本地消息异步写入请求通道
	localMessageCloseCh   chan chan error                     // 关闭本地消息循环的控制通道
	localMessageDone      chan struct{}                       // 本地消息循环 goroutine 退出信号
	localMessageStats     pebbleLocalMessageBatchStats        // 本地消息批处理统计
	localMessageMu        sync.Mutex                          // 保护 localMessageClosed 字段的互斥锁
	localMessageClosed    bool                                // 本地消息循环关闭标志
}

// Name 返回后端引擎名称。
func (b *pebbleStoreBackend) Name() string {
	return EnginePebble
}

// Bind 注入运行时依赖，初始化 Pebble 各 Repository 并启动异步消息写入循环。
func (b *pebbleStoreBackend) Bind(bindings storeBackendBindings) error {
	b.messageProjectionRepo = &pebbleMessageProjectionRepository{
		db:                b.db,
		profile:           b.profile,
		writes:            b.writes,
		messageWindowSize: bindings.MessageWindowSize,
		userRepository:    bindings.UserRepository,
		subscriptions:     bindings.Subscriptions,
		blacklists:        bindings.Blacklists,
		messageTrim:       bindings.MessageTrim,
	}
	b.messageProjectionRepo.startTrimWorker()
	b.messageProjection = b.messageProjectionRepo
	b.messageSequences = &pebbleMessageSequenceRepository{
		db:     b.db,
		sqlDB:  b.sqlDB,
		writes: b.writes,
	}
	b.peerAckCursors = &pebblePeerAckCursorRepository{
		db:     b.db,
		writes: b.writes,
		clock:  bindings.Clock,
	}
	b.originCursors = &pebbleOriginCursorRepository{
		db:     b.db,
		writes: b.writes,
		clock:  bindings.Clock,
	}
	b.pendingProjections = &pebblePendingProjectionRepository{
		db:     b.db,
		writes: b.writes,
		clock:  bindings.Clock,
	}
	b.eventLog = &pebbleEventLogRepository{
		db:     b.db,
		writes: b.writes,
		ids:    bindings.IDs,
		nodeID: bindings.NodeID,
		clock:  bindings.Clock,
	}
	b.startLocalMessageLoop()
	return nil
}

// EventLog 返回 pebble 实现的事件日志仓库。
func (b *pebbleStoreBackend) EventLog() EventLogRepository {
	return b.eventLog
}

// MessageProjection 返回 pebble 实现的消息投影仓库。
func (b *pebbleStoreBackend) MessageProjection() MessageProjectionRepository {
	return b.messageProjection
}

// CreateMessage 通过 Pebble 后端创建消息：校验用户和黑名单后通过异步通道提交消息写入请求。
// PebbleMessageSyncMode 参数控制写入的同步/异步模式。
func (b *pebbleStoreBackend) CreateMessage(ctx context.Context, s *Store, params CreateMessageParams) (Message, Event, error) {
	recipient, err := s.getUser(ctx, params.UserKey, false)
	if err != nil {
		return Message{}, Event{}, err
	}

	sender := User{}
	senderExists := false
	if params.Sender == params.UserKey {
		sender = recipient
		senderExists = true
	} else {
		sender, err = s.getUser(ctx, params.Sender, false)
		switch {
		case err == nil:
			senderExists = true
		case errors.Is(err, ErrNotFound):
		default:
			return Message{}, Event{}, err
		}
	}

	if recipient.CanLogin() && senderExists && sender.Role == RoleUser && sender.Key() != recipient.Key() {
		blocked, err := s.blacklists.HasActiveBlock(ctx, recipient.Key(), sender.Key(), nil)
		if err != nil {
			return Message{}, Event{}, err
		}
		if blocked {
			return Message{}, Event{}, ErrBlockedByBlacklist
		}
	}
	params.PebbleMessageSyncMode = resolvePebbleMessageSyncMode(params.PebbleMessageSyncMode, s.pebbleMessageSyncMode)
	return b.submitLocalMessage(ctx, params)
}

// NextMessageSeqTx 在事务中为指定用户+节点分配下一个消息序列号（Pebble 实现）。
func (b *pebbleStoreBackend) NextMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	if b.messageSequences == nil {
		return 0, fmt.Errorf("pebble message sequence repository is not initialized")
	}
	return b.messageSequences.NextSequenceTx(ctx, tx, key, nodeID)
}

// InsertLocalEventTx 委托 pebbleEventLogRepository 在 Pebble 中追加一条本地事件。
func (b *pebbleStoreBackend) InsertLocalEventTx(ctx context.Context, tx *sql.Tx, event Event) (Event, error) {
	return b.eventLog.Append(ctx, event)
}

// StoreReplicatedEventTx 委托 pebbleEventLogRepository 在 Pebble 中追加来自 peer 的复制事件。
func (b *pebbleStoreBackend) StoreReplicatedEventTx(ctx context.Context, tx *sql.Tx, rawEvent *internalproto.ReplicatedEvent, decoded Event) (Event, bool, error) {
	return b.eventLog.AppendReplicated(ctx, decoded)
}

// ListPendingProjectionEvents 列出待重试的投影事件。
// 优先从 Pebble 的 pendingProjections 仓库查询，回退到 SQLite 的 pending_projections 表。
// 查询到的事件 key 后通过 eventLog 获取完整事件数据。
func (b *pebbleStoreBackend) ListPendingProjectionEvents(ctx context.Context, db *sql.DB, limit int) ([]Event, error) {
	var events []Event
	var pending []pendingProjectionEnvelope
	if b.pendingProjections != nil {
		var err error
		pending, err = b.pendingProjections.List(ctx, limit)
		if err != nil {
			return nil, err
		}
	} else {
		rows, err := db.QueryContext(ctx, `
SELECT origin_node_id, event_id
FROM pending_projections
ORDER BY last_failed_at_hlc ASC, origin_node_id ASC, event_id ASC
LIMIT ?
`, limit)
		if err != nil {
			return nil, fmt.Errorf("list pending projection keys: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var originNodeID, eventID int64
			if err := rows.Scan(&originNodeID, &eventID); err != nil {
				return nil, fmt.Errorf("scan pending projection key: %w", err)
			}
			pending = append(pending, pendingProjectionEnvelope{OriginNodeID: originNodeID, EventID: eventID})
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate pending projection keys: %w", err)
		}
	}

	for _, item := range pending {
		found, err := b.eventLog.ListEventsByOrigin(ctx, item.OriginNodeID, item.EventID-1, 1)
		if err != nil {
			return nil, err
		}
		if len(found) == 1 && found[0].OriginNodeID == item.OriginNodeID && found[0].EventID == item.EventID {
			events = append(events, found[0])
		}
	}
	return events, nil
}

// ListLocalOriginEventStats 从 Pebble 事件日志仓库获取所有来源节点的事件进度和计数。
func (b *pebbleStoreBackend) ListLocalOriginEventStats(ctx context.Context, db *sql.DB) (map[int64]localOriginEventStats, error) {
	progress, err := b.eventLog.ListOriginProgress(ctx)
	if err != nil {
		return nil, err
	}
	stats := make(map[int64]localOriginEventStats, len(progress))
	for _, item := range progress {
		count, err := b.eventLog.CountEventsByOrigin(ctx, item.OriginNodeID, 0)
		if err != nil {
			return nil, err
		}
		stats[item.OriginNodeID] = localOriginEventStats{
			LastEventID: item.LastEventID,
			EventCount:  count,
		}
	}
	return stats, nil
}

// CountUnconfirmedOriginEvents 统计某来源节点尚未被本节点确认的事件数（Pebble 实现）。
func (b *pebbleStoreBackend) CountUnconfirmedOriginEvents(ctx context.Context, db *sql.DB, originNodeID, ackedEventID, fallbackCount int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, nil
	}
	if ackedEventID <= 0 {
		return fallbackCount, nil
	}
	return b.eventLog.CountEventsByOrigin(ctx, originNodeID, ackedEventID)
}

// PruneEventLogOrigin 裁剪 Pebble 中某来源节点的事件日志：先 flush 等待写入完成，
// 统计事件数，超过 maxEvents 时计算截断边界、记录截断元数据后批量删除最早的事件。
func (b *pebbleStoreBackend) PruneEventLogOrigin(ctx context.Context, db *sql.DB, clk *clock.Clock, originNodeID int64, maxEvents int) (int64, error) {
	if b.db == nil {
		return 0, fmt.Errorf("pebble event log prune requires pebble db")
	}
	if b.writes != nil {
		if err := b.writes.Flush(); err != nil {
			return 0, err
		}
	}

	total, err := countPebbleOriginEvents(ctx, b.db, originNodeID)
	if err != nil {
		return 0, err
	}
	if total <= int64(maxEvents) {
		return 0, nil
	}

	trimCount := total - int64(maxEvents)
	truncatedBefore, err := nthPebbleOriginEventID(ctx, b.db, originNodeID, trimCount-1)
	if err != nil {
		return 0, err
	}
	if err := upsertEventLogTruncation(ctx, db, clk, originNodeID, truncatedBefore); err != nil {
		return 0, err
	}

	trimmed, err := deletePebbleOriginEvents(ctx, b.db, originNodeID, trimCount)
	if err != nil {
		return 0, err
	}
	if trimmed > 0 {
		if err := recordEventLogTrim(ctx, db, clk, trimmed); err != nil {
			return 0, err
		}
	}
	return trimmed, nil
}

// Close 按顺序关闭 Pebble 后端：关闭本地消息循环、消息投影仓库、写入协调器和 Pebble 数据库。
func (b *pebbleStoreBackend) Close() error {
	var err error
	if closeErr := b.closeLocalMessageLoop(); err == nil {
		err = closeErr
	}
	if b.messageProjectionRepo != nil {
		if closeErr := b.messageProjectionRepo.close(); err == nil {
			err = closeErr
		}
	}
	if b.writes != nil {
		if closeErr := b.writes.Close(); err == nil {
			err = closeErr
		}
	}
	if b.db != nil {
		if closeErr := b.db.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}
