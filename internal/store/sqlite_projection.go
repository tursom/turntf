package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
	clusterproto "github.com/tursom/turntf/internal/proto"
)

// sqliteEventLogRepository 是事件日志的 SQLite 实现。
// 事件按全局序列号递增顺序存储，每条事件记录包含 event_id、origin_node_id 和 value。
// 支持按序列号范围和按来源节点查询。
type sqliteEventLogRepository struct {
	db     *sql.DB
	ids    *clock.IDGenerator
	nodeID int64
	clock  *clock.Clock
}

// sqliteMessageProjectionRepository 是消息投影的 SQLite 实现。
// 消息存储在 messages 表中，支持按用户、会话查询，
// 并集成黑名单过滤和频道订阅机制。
type sqliteMessageProjectionRepository struct {
	db                *sql.DB
	clock             *clock.Clock
	messageWindowSize int
	userRepository    UserRepository
	subscriptions     SubscriptionRepository
	blacklists        BlacklistRepository
}

// sqlExecContext 是 SQL 查询执行接口，同时支持 *sql.DB 和 *sql.Tx。
type sqlExecContext interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// Append 将本地事件追加到事件日志中。
// 使用事务写入 event_log 表，记录事件内容、生成序列号，并更新 origin cursor。
// 事件 ID 由 IDGenerator 分配，确保节点内唯一。
func (r *sqliteEventLogRepository) Append(ctx context.Context, event Event) (Event, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, fmt.Errorf("begin append event: %w", err)
	}
	defer tx.Rollback()

	if r.ids == nil {
		return Event{}, fmt.Errorf("append event before id generator initialization")
	}
	if r.clock == nil {
		return Event{}, fmt.Errorf("append event before clock initialization")
	}

	event.EventID = r.ids.Next()
	event.OriginNodeID = r.nodeID
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
	if err := upsertOriginCursorTx(ctx, tx, event.OriginNodeID, event.EventID, r.clock.Now().String()); err != nil {
		return Event{}, fmt.Errorf("record local origin cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Event{}, fmt.Errorf("commit append event: %w", err)
	}
	return event, nil
}

// AppendReplicated 追加从其他节点复制过来的事件。
// 返回值：
//   - Event: 追加或已存在的事件
//   - bool: 是否为首次插入（true=新插入，false=已存在）
//   - error: 操作错误
//
// 如果事件已存在（唯一约束冲突），返回已存储的事件版本。
func (r *sqliteEventLogRepository) AppendReplicated(ctx context.Context, event Event) (Event, bool, error) {
	value, err := eventLogValue(event)
	if err != nil {
		return Event{}, false, err
	}
	result, err := r.db.ExecContext(ctx, `
INSERT INTO event_log(event_id, origin_node_id, value)
VALUES(?, ?, ?)
`, event.EventID, event.OriginNodeID, value)
	if err != nil {
		if isUniqueConstraint(err) {
			events, listErr := r.ListEventsByOrigin(ctx, event.OriginNodeID, event.EventID-1, 1)
			if listErr != nil {
				return Event{}, false, listErr
			}
			if len(events) == 1 && events[0].EventID == event.EventID {
				return events[0], false, nil
			}
		}
		return Event{}, false, fmt.Errorf("insert replicated event log: %w", err)
	}
	event.Sequence, err = result.LastInsertId()
	if err != nil {
		return Event{}, false, fmt.Errorf("read replicated event sequence: %w", err)
	}
	return event, true, nil
}

// ListEvents 列出指定序列号之后的事件，按序列号升序排列。
// 参数 limit 有效范围 1-1000，默认 100。
func (r *sqliteEventLogRepository) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, `
SELECT sequence, event_id, origin_node_id, value
FROM event_log
WHERE sequence > ?
ORDER BY sequence ASC
LIMIT ?
`, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
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
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

// ListEventsByOrigin 按来源节点列出指定事件 ID 之后的事件，按 event_id 升序排列。
// 用于复制追踪：从指定来源节点获取尚未复制到本地的事件。
func (r *sqliteEventLogRepository) ListEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64, limit int) ([]Event, error) {
	if originNodeID <= 0 {
		return nil, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, `
SELECT sequence, event_id, origin_node_id, value
FROM event_log
WHERE origin_node_id = ? AND event_id > ?
ORDER BY event_id ASC
LIMIT ?
`, originNodeID, afterEventID, limit)
	if err != nil {
		return nil, fmt.Errorf("list events by origin: %w", err)
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
		return nil, fmt.Errorf("iterate origin events: %w", err)
	}
	return events, nil
}

// CountEventsByOrigin 统计指定来源节点在指定事件 ID 之后的事件数量。
func (r *sqliteEventLogRepository) CountEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, nil
	}
	var count int64
	if err := r.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM event_log
WHERE origin_node_id = ? AND event_id > ?
`, originNodeID, afterEventID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count origin events: %w", err)
	}
	return count, nil
}

// LastEventSequence 返回事件日志的最大序列号。
// 用于恢复时确定已处理到哪个序列号位置。
func (r *sqliteEventLogRepository) LastEventSequence(ctx context.Context) (int64, error) {
	var sequence int64
	if err := r.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM event_log`).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("query last event sequence: %w", err)
	}
	return sequence, nil
}

// ListOriginProgress 列出各来源节点的事件进度。
// 对于每个来源节点，返回该节点在本地已复制到的最大 event_id。
// 用于集群复制进度追踪。
func (r *sqliteEventLogRepository) ListOriginProgress(ctx context.Context) ([]OriginProgress, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT origin_node_id, COALESCE(MAX(event_id), 0)
FROM event_log
GROUP BY origin_node_id
ORDER BY origin_node_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list origin progress: %w", err)
	}
	defer rows.Close()

	var progress []OriginProgress
	for rows.Next() {
		var item OriginProgress
		if err := rows.Scan(&item.OriginNodeID, &item.LastEventID); err != nil {
			return nil, fmt.Errorf("scan origin progress: %w", err)
		}
		progress = append(progress, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate origin progress: %w", err)
	}
	return progress, nil
}

// ApplyMessageCreated 投影消息创建事件到 messages 表。
// 使用事务写入消息记录并在提交前触发裁剪。
func (r *sqliteMessageProjectionRepository) ApplyMessageCreated(ctx context.Context, message Message) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin apply message projection: %w", err)
	}
	defer tx.Rollback()

	if err := r.applyMessageCreatedTx(ctx, tx, message); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit apply message projection: %w", err)
	}
	return nil
}

// ListMessagesByUser 列出用户的消息列表。
// 合并以下来源的消息并去重：用户直发消息、广播消息、频道订阅消息。
// 结果按创建时间降序排列，受黑名单过滤。
func (r *sqliteMessageProjectionRepository) ListMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
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
		return r.listRawMessagesByUser(ctx, key, limit)
	}

	candidates := make([]Message, 0, limit)
	seen := make(map[string]struct{})
	add := func(messages []Message) {
		for _, message := range messages {
			id := messageIdentity(message)
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			candidates = append(candidates, message)
		}
	}

	direct, err := r.listRawMessagesByUser(ctx, key, 0)
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
		messages, err := r.listRawMessagesByUser(ctx, broadcast, 0)
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
		messages, err := r.listRawMessagesByUserSince(ctx, subscription.Channel, 0, &subscription.SubscribedAt)
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

// BuildMessageSnapshotRows 构建指定生产者节点的消息快照行。
// 用于集群状态同步：将本节点上指定 producer 的消息导出为 SnapshotRow 列表。
func (r *sqliteMessageProjectionRepository) BuildMessageSnapshotRows(ctx context.Context, producer int64) ([]*clusterproto.SnapshotRow, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session
FROM messages
WHERE node_id = ?
ORDER BY user_node_id ASC, user_id ASC, created_at_hlc DESC, node_id ASC, seq DESC
`, producer)
	if err != nil {
		return nil, fmt.Errorf("query snapshot messages: %w", err)
	}
	defer rows.Close()

	snapshotRows := make([]*clusterproto.SnapshotRow, 0)
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		snapshotRows = append(snapshotRows, snapshotRowFromMessage(message))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot messages: %w", err)
	}
	return snapshotRows, nil
}

// ApplyMessageSnapshotRows 应用来自其他节点的消息快照行。
// 逐条应用快照行，对受影响的用户执行消息裁剪，使用单事务保证原子性。
func (r *sqliteMessageProjectionRepository) ApplyMessageSnapshotRows(ctx context.Context, producer int64, rows []*clusterproto.SnapshotRow) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin apply message snapshot rows: %w", err)
	}
	defer tx.Rollback()

	affectedUsers := make(map[UserKey]struct{})
	for _, row := range rows {
		key, err := r.applyMessageSnapshotRowTx(ctx, tx, producer, row)
		if err != nil {
			return err
		}
		if key != (UserKey{}) {
			affectedUsers[key] = struct{}{}
		}
	}
	for key := range affectedUsers {
		if err := r.trimMessagesForUserTx(ctx, tx, key); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit apply message snapshot rows: %w", err)
	}
	return nil
}

// applyMessageCreatedTx 在事务中执行消息投影插入。
// 验证消息身份后写入 messages 表（忽略唯一约束冲突），然后触发用户消息裁剪。
func (r *sqliteMessageProjectionRepository) applyMessageCreatedTx(ctx context.Context, tx *sql.Tx, message Message) error {
	key := message.UserKey()
	if err := validateMessageIdentity(key, message.NodeID, message.Seq); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
`, message.Recipient.NodeID, message.Recipient.UserID, message.NodeID, message.Seq, message.Sender.NodeID, message.Sender.UserID, message.Body, message.CreatedAt.String(), MessageSession(message.Sender, message.Recipient)); err != nil {
		if !isUniqueConstraint(err) {
			return fmt.Errorf("insert message projection: %w", err)
		}
	}
	if err := r.trimMessagesForUserTx(ctx, tx, key); err != nil {
		return err
	}
	return nil
}

// applyMessageSnapshotRowTx 在事务中应用单条快照行。
// 返回值：
//   - UserKey: 受影响的用户键（用于后续裁剪）
//   - error: 处理错误
//
// 验证消息属于指定 producer、接收者存在，忽略已存在的记录。
func (r *sqliteMessageProjectionRepository) applyMessageSnapshotRowTx(ctx context.Context, tx *sql.Tx, producer int64, row *clusterproto.SnapshotRow) (UserKey, error) {
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
	if _, err := parseRequiredTimestamp(messageRow.CreatedAtHlc, "snapshot message created_at"); err != nil {
		return UserKey{}, err
	}
	if _, err := r.userRepository.GetUserTx(ctx, tx, key, false); err != nil {
		if errors.Is(err, ErrNotFound) {
			return UserKey{}, nil
		}
		return UserKey{}, err
	}

	if messageRow.Sender == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot message sender cannot be empty", ErrInvalidInput)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
`, messageRow.Recipient.NodeId, messageRow.Recipient.UserId, messageRow.NodeId, messageRow.Seq, messageRow.Sender.NodeId, messageRow.Sender.UserId, messageRow.Body, messageRow.CreatedAtHlc,
		MessageSession(UserKey{NodeID: messageRow.Sender.NodeId, UserID: messageRow.Sender.UserId}, UserKey{NodeID: messageRow.Recipient.NodeId, UserID: messageRow.Recipient.UserId}),
	); err != nil {
		if isUniqueConstraint(err) {
			return key, nil
		}
		return UserKey{}, fmt.Errorf("insert snapshot message: %w", err)
	}
	return key, nil
}

// listRawMessagesByUser 直接查询用户的消息列表（无黑名单过滤、无去重）。
// 委托给 listRawMessagesByUserSince，since 为 nil 表示不限时间。
func (r *sqliteMessageProjectionRepository) listRawMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	return r.listRawMessagesByUserSince(ctx, key, limit, nil)
}

// listRawMessagesByUserSince 按用户列出指定时间之后的消息。
// 支持可选的时间戳过滤（since），用于频道消息的增量查询。
func (r *sqliteMessageProjectionRepository) listRawMessagesByUserSince(ctx context.Context, key UserKey, limit int, since *clock.Timestamp) ([]Message, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	query := `
SELECT user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session
FROM messages
WHERE user_node_id = ? AND user_id = ?`
	args := []any{key.NodeID, key.UserID}
	if since != nil {
		query += ` AND created_at_hlc >= ?`
		args = append(args, since.String())
	}
	query += `
ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
LIMIT ?`
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list raw messages: %w", err)
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate raw messages: %w", err)
	}
	return messages, nil
}

// ListMessagesBySession 列出指定会话的消息。
// session 必须是 32 字节的会话标识。结果经过黑名单过滤。
func (r *sqliteMessageProjectionRepository) ListMessagesBySession(ctx context.Context, session []byte, requester UserKey, limit int) ([]Message, error) {
	if len(session) != 32 {
		return nil, fmt.Errorf("%w: session must be exactly 32 bytes", ErrInvalidInput)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, `
SELECT user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session
FROM messages
WHERE session = ?
ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
LIMIT ?
`, session, limit)
	if err != nil {
		return nil, fmt.Errorf("list messages by session: %w", err)
	}
	defer rows.Close()

	messages := make([]Message, 0, limit)
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session messages: %w", err)
	}

	if len(messages) == 0 {
		return messages, nil
	}
	return filterDirectMessagesByBlacklist(ctx, r.userRepository, r.blacklists, requester, messages)
}

// sqliteBlacklistRepository 是用户黑名单的 SQLite 实现。
// 黑名单是 AttachmentTypeUserBlacklist 类型附件的语义化视图。
type sqliteBlacklistRepository struct {
	attachments *sqliteUserAttachmentRepository
}

// ListActiveBlockedUsers 列出指定用户的所有活跃黑名单条目。
func (r *sqliteBlacklistRepository) ListActiveBlockedUsers(ctx context.Context, owner UserKey) ([]BlacklistEntry, error) {
	if r == nil || r.attachments == nil {
		return nil, nil
	}
	attachments, err := r.attachments.ListActiveByOwner(ctx, owner, AttachmentTypeUserBlacklist)
	if err != nil {
		return nil, err
	}
	entries := make([]BlacklistEntry, 0, len(attachments))
	for _, attachment := range attachments {
		entries = append(entries, blacklistEntryFromAttachment(attachment))
	}
	return entries, nil
}

// HasActiveBlock 检查 owner 是否在指定时间前已屏蔽了 blocked。
func (r *sqliteBlacklistRepository) HasActiveBlock(ctx context.Context, owner, blocked UserKey, createdAt *clock.Timestamp) (bool, error) {
	if r == nil || r.attachments == nil {
		return false, nil
	}
	return r.attachments.HasActive(ctx, owner, blocked, AttachmentTypeUserBlacklist, createdAt)
}

// filterDirectMessagesByBlacklist 从消息列表中过滤掉被黑名单屏蔽的消息。
// 被屏蔽用户发送的消息在屏蔽时间之后的不显示，
// 但非普通角色用户（如广播员）的消息不会被屏蔽。
func filterDirectMessagesByBlacklist(ctx context.Context, userRepo UserRepository, blacklistRepo BlacklistRepository, owner UserKey, messages []Message) ([]Message, error) {
	if blacklistRepo == nil || len(messages) == 0 {
		return messages, nil
	}
	entries, err := blacklistRepo.ListActiveBlockedUsers(ctx, owner)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return messages, nil
	}

	blockedAtBySender := make(map[UserKey]clock.Timestamp, len(entries))
	for _, entry := range entries {
		blockedAtBySender[entry.Blocked] = entry.BlockedAt
	}
	senderRoleCache := make(map[UserKey]string, len(entries))
	filtered := make([]Message, 0, len(messages))
	for _, message := range messages {
		blockedAt, blocked := blockedAtBySender[message.Sender]
		if !blocked || message.CreatedAt.Compare(blockedAt) < 0 {
			filtered = append(filtered, message)
			continue
		}
		role, ok := senderRoleCache[message.Sender]
		if !ok {
			sender, err := userRepo.GetUser(ctx, message.Sender, false)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					role = ""
				} else {
					return nil, err
				}
			} else {
				role = sender.Role
			}
			senderRoleCache[message.Sender] = role
		}
		if role != RoleUser {
			filtered = append(filtered, message)
		}
	}
	return filtered, nil
}

// sqliteUserRepository 是用户的 SQLite 实现。
type sqliteUserRepository struct {
	db *sql.DB
}

// sqliteSubscriptionRepository 是频道订阅的 SQLite 实现。
// 订阅是 AttachmentTypeChannelSubscription 类型附件的语义化视图。
type sqliteSubscriptionRepository struct {
	attachments *sqliteUserAttachmentRepository
}

// sqliteMessageTrimRepository 是消息裁剪记录的 SQLite 实现。
// 记录每条裁剪操作的裁剪数量和最后裁剪时间。
type sqliteMessageTrimRepository struct {
	db    *sql.DB
	clock *clock.Clock
}

// GetUser 查询用户信息。
// includeDeleted 为 true 时返回已删除的用户。
func (r *sqliteUserRepository) GetUser(ctx context.Context, key UserKey, includeDeleted bool) (User, error) {
	if err := key.Validate(); err != nil {
		return User{}, err
	}
	query := `
SELECT node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
FROM users
WHERE node_id = ? AND user_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at_hlc IS NULL`
	}

	row := r.db.QueryRowContext(ctx, query, key.NodeID, key.UserID)
	user, err := scanUser(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	return user, nil
}

// GetUserTx 在事务中查询用户信息。
// 功能同 GetUser，但使用事务上下文。
func (r *sqliteUserRepository) GetUserTx(ctx context.Context, tx *sql.Tx, key UserKey, includeDeleted bool) (User, error) {
	if err := key.Validate(); err != nil {
		return User{}, err
	}
	query := `
SELECT node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
FROM users
WHERE node_id = ? AND user_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at_hlc IS NULL`
	}

	row := tx.QueryRowContext(ctx, query, key.NodeID, key.UserID)
	user, err := scanUser(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	return user, nil
}

// ListBroadcastUserKeys 列出所有广播用户的 UserKey。
// 广播用户（RoleBroadcast）的消息会发送给所有普通用户。
func (r *sqliteUserRepository) ListBroadcastUserKeys(ctx context.Context) ([]UserKey, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT node_id, user_id
FROM users
WHERE role = ? AND deleted_at_hlc IS NULL
ORDER BY node_id ASC, user_id ASC
`, RoleBroadcast)
	if err != nil {
		return nil, fmt.Errorf("list broadcast users: %w", err)
	}
	defer rows.Close()

	keys := make([]UserKey, 0)
	for rows.Next() {
		var key UserKey
		if err := rows.Scan(&key.NodeID, &key.UserID); err != nil {
			return nil, fmt.Errorf("scan broadcast user: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate broadcast users: %w", err)
	}
	return keys, nil
}

// ListActiveSubscriptions 列出订阅者的所有活跃频道订阅。
func (r *sqliteSubscriptionRepository) ListActiveSubscriptions(ctx context.Context, subscriber UserKey) ([]Subscription, error) {
	if r == nil || r.attachments == nil {
		return nil, nil
	}
	attachments, err := r.attachments.ListActiveByOwner(ctx, subscriber, AttachmentTypeChannelSubscription)
	if err != nil {
		return nil, err
	}
	subscriptions := make([]Subscription, 0, len(attachments))
	for _, attachment := range attachments {
		subscriptions = append(subscriptions, subscriptionFromAttachment(attachment))
	}
	return subscriptions, nil
}

// ListChannelSubscribers 列出指定频道的所有订阅者。
func (r *sqliteSubscriptionRepository) ListChannelSubscribers(ctx context.Context, channel UserKey) ([]Subscription, error) {
	if r == nil || r.attachments == nil {
		return nil, nil
	}
	attachments, err := r.attachments.ListActiveBySubject(ctx, channel, AttachmentTypeChannelSubscription)
	if err != nil {
		return nil, err
	}
	subscriptions := make([]Subscription, 0, len(attachments))
	for _, attachment := range attachments {
		subscriptions = append(subscriptions, subscriptionFromAttachment(attachment))
	}
	return subscriptions, nil
}

// RecordMessageTrim 记录消息裁剪操作的统计信息。
// 使用 UPSERT 语句更新 message_trim_stats 表中的累计裁剪数量。
func (r *sqliteMessageTrimRepository) RecordMessageTrim(ctx context.Context, trimmed int64) error {
	if trimmed <= 0 {
		return nil
	}
	now := r.clock.Now().String()
	if _, err := r.db.ExecContext(ctx, `
INSERT INTO message_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES('global', ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = message_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, trimmed, now); err != nil {
		return fmt.Errorf("record message trim stats: %w", err)
	}
	return nil
}

// trimMessagesForUserTx 在事务中裁剪用户的过期消息。
// 当用户消息数超过 windowSize 时删除多余消息，保留最新的 windowSize 条。
// 使用 OFFSET 子句定位需要保留的边界，删除边界之外的消息。
func (r *sqliteMessageProjectionRepository) trimMessagesForUserTx(ctx context.Context, tx *sql.Tx, key UserKey) error {
	if err := key.Validate(); err != nil {
		return err
	}
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	var overflowMarker int
	err := tx.QueryRowContext(ctx, `
SELECT 1
FROM messages
WHERE user_node_id = ? AND user_id = ?
ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
LIMIT 1 OFFSET ?
`, key.NodeID, key.UserID, windowSize).Scan(&overflowMarker)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("check message trim overflow for user %d:%d: %w", key.NodeID, key.UserID, err)
	}

	result, err := tx.ExecContext(ctx, `
DELETE FROM messages
WHERE user_node_id = ? AND user_id = ?
  AND (node_id, seq) IN (
    SELECT node_id, seq
    FROM messages
    WHERE user_node_id = ? AND user_id = ?
    ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
    LIMIT -1 OFFSET ?
  )
`, key.NodeID, key.UserID, key.NodeID, key.UserID, windowSize)
	if err != nil {
		return fmt.Errorf("trim messages for user %d:%d: %w", key.NodeID, key.UserID, err)
	}
	trimmed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count trimmed messages for user %d:%d: %w", key.NodeID, key.UserID, err)
	}
	if trimmed > 0 {
		if err := r.recordMessageTrimTx(ctx, tx, trimmed); err != nil {
			return err
		}
	}
	return nil
}

// recordMessageTrimTx 在事务中记录消息裁剪统计信息。
// 功能同 RecordMessageTrim，但使用事务上下文。
func (r *sqliteMessageProjectionRepository) recordMessageTrimTx(ctx context.Context, tx *sql.Tx, trimmed int64) error {
	if trimmed <= 0 {
		return nil
	}
	now := r.clock.Now().String()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO message_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES('global', ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = message_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, trimmed, now); err != nil {
		return fmt.Errorf("record message trim stats: %w", err)
	}
	return nil
}

// recordPendingProjection 记录待重试的投影事件。
// 根据后端类型选择存储方式：Pebble 后端使用 pendingProjections 协调器，
// SQLite 后端直接写入 pending_projections 表。
func (s *Store) recordPendingProjection(ctx context.Context, event Event, reason error) error {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.pendingProjections != nil {
		return pebbleBackend.pendingProjections.Record(ctx, event, reason)
	}
	return s.recordPendingProjectionAt(ctx, s.db, event, reason)
}

// recordPendingProjectionTx 在事务中记录待重试的投影事件。
// 委托给 recordPendingProjectionAt，使用事务作为执行上下文。
func (s *Store) recordPendingProjectionTx(ctx context.Context, tx *sql.Tx, event Event, reason error) error {
	return s.recordPendingProjectionAt(ctx, tx, event, reason)
}

// recordPendingProjectionAt 使用指定的执行上下文记录待重试的投影事件。
// 使用 UPSERT 语义：如果同一事件已存在，增加重试次数并更新错误信息。
func (s *Store) recordPendingProjectionAt(ctx context.Context, execer sqlExecContext, event Event, reason error) error {
	now := s.clock.Now().String()
	message := ""
	if reason != nil {
		message = strings.TrimSpace(reason.Error())
	}
	if message == "" {
		message = "projection failed"
	}
	_, err := execer.ExecContext(ctx, `
INSERT INTO pending_projections(
    origin_node_id, event_id, event_type, aggregate_type, aggregate_node_id, aggregate_id,
    attempt_count, last_error, first_failed_at_hlc, last_failed_at_hlc
)
VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
ON CONFLICT(origin_node_id, event_id) DO UPDATE SET
    event_type = excluded.event_type,
    aggregate_type = excluded.aggregate_type,
    aggregate_node_id = excluded.aggregate_node_id,
    aggregate_id = excluded.aggregate_id,
    attempt_count = pending_projections.attempt_count + 1,
    last_error = excluded.last_error,
    last_failed_at_hlc = excluded.last_failed_at_hlc
`, event.OriginNodeID, event.EventID, string(event.EventType), event.Aggregate, event.AggregateNodeID, event.AggregateID, message, now, now)
	if err != nil {
		return fmt.Errorf("record pending projection: %w", err)
	}
	return nil
}

// clearPendingProjection 清除指定事件的待重试投影记录。
// 表示事件已成功投影，无需再次重试。
func (s *Store) clearPendingProjection(ctx context.Context, originNodeID, eventID int64) error {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.pendingProjections != nil {
		return pebbleBackend.pendingProjections.Clear(ctx, originNodeID, eventID)
	}
	if originNodeID <= 0 || eventID <= 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
DELETE FROM pending_projections
WHERE origin_node_id = ? AND event_id = ?
`, originNodeID, eventID); err != nil {
		return fmt.Errorf("clear pending projection: %w", err)
	}
	return nil
}

// projectionStats 返回待处理投影的统计信息。
// 包括待重试事件总数和最近一次失败的时间戳。
func (s *Store) projectionStats(ctx context.Context) (ProjectionStats, error) {
	if pebbleBackend, ok := s.backend.(*pebbleStoreBackend); ok && pebbleBackend.pendingProjections != nil {
		return pebbleBackend.pendingProjections.Stats(ctx)
	}
	var (
		total   int64
		lastRaw sql.NullString
	)
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*), MAX(last_failed_at_hlc)
FROM pending_projections
`).Scan(&total, &lastRaw); err != nil {
		return ProjectionStats{}, fmt.Errorf("query projection stats: %w", err)
	}
	stats := ProjectionStats{PendingTotal: total}
	if lastRaw.Valid && lastRaw.String != "" {
		ts, err := clock.ParseTimestamp(lastRaw.String)
		if err != nil {
			return ProjectionStats{}, fmt.Errorf("parse projection last failed timestamp: %w", err)
		}
		stats.LastFailedAt = &ts
	}
	return stats, nil
}

// listPendingProjectionEvents 列出待重试投影的事件列表。
// 委托给后端的具体实现。
func (s *Store) listPendingProjectionEvents(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	return s.backend.ListPendingProjectionEvents(ctx, s.db, limit)
}

// ReplayPendingEvents 重放待重试的投影事件。
// 对每个待重试事件尝试重新投影，成功则清除待处理记录，
// 失败则更新待处理记录的重试次数，继续处理下一个事件。
func (s *Store) ReplayPendingEvents(ctx context.Context, limit int) error {
	events, err := s.listPendingProjectionEvents(ctx, limit)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := s.projectMessageEvent(ctx, event); err != nil {
			if recordErr := s.recordPendingProjection(ctx, event, err); recordErr != nil {
				return fmt.Errorf("replay event %d:%d failed and record pending projection failed: %w", event.OriginNodeID, event.EventID, recordErr)
			}
			continue
		}
		if err := s.clearPendingProjection(ctx, event.OriginNodeID, event.EventID); err != nil {
			return err
		}
	}
	return nil
}

// projectMessageEvent 将事件投影为消息。
// 从事件体中提取 MessageCreatedEvent，转换为 Message 后调用投影接口。
func (s *Store) projectMessageEvent(ctx context.Context, event Event) error {
	body, ok := event.Body.(*clusterproto.MessageCreatedEvent)
	if !ok {
		return fmt.Errorf("%w: unsupported event body %T", ErrInvalidInput, event.Body)
	}
	message, err := messageFromCreatedEvent(body)
	if err != nil {
		return err
	}
	return s.backend.MessageProjection().ApplyMessageCreated(ctx, message)
}

// messageFromCreatedEvent 从 Protobuf MessageCreatedEvent 转换为内部 Message 结构体。
// 验证所有必要字段（接收者、发送者、消息体），复制消息体防止外部修改。
func messageFromCreatedEvent(body *clusterproto.MessageCreatedEvent) (Message, error) {
	if body == nil {
		return Message{}, fmt.Errorf("%w: message created event cannot be nil", ErrInvalidInput)
	}
	if body.Recipient == nil {
		return Message{}, fmt.Errorf("%w: recipient cannot be empty", ErrInvalidInput)
	}
	key := UserKey{NodeID: body.Recipient.NodeId, UserID: body.Recipient.UserId}
	if err := validateMessageIdentity(key, body.NodeId, body.Seq); err != nil {
		return Message{}, err
	}
	createdAt, err := parseRequiredTimestamp(body.CreatedAtHlc, "message created_at")
	if err != nil {
		return Message{}, err
	}
	if body.Sender == nil {
		return Message{}, fmt.Errorf("%w: sender cannot be empty", ErrInvalidInput)
	}
	sender := UserKey{NodeID: body.Sender.NodeId, UserID: body.Sender.UserId}
	if err := sender.Validate(); err != nil {
		return Message{}, fmt.Errorf("%w: sender cannot be empty", ErrInvalidInput)
	}
	if len(body.Body) == 0 {
		return Message{}, fmt.Errorf("%w: body cannot be empty", ErrInvalidInput)
	}
	return Message{
		Recipient: key,
		NodeID:    body.NodeId,
		Seq:       body.Seq,
		Sender:    sender,
		Body:      append([]byte(nil), body.Body...),
		CreatedAt: createdAt,
	}, nil
}
