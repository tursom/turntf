package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"notifier/internal/clock"
	clusterproto "notifier/internal/proto"
)

type sqliteEventLogRepository struct {
	db     *sql.DB
	ids    *clock.IDGenerator
	nodeID int64
	clock  *clock.Clock
}

type sqliteMessageProjectionRepository struct {
	db                *sql.DB
	clock             *clock.Clock
	messageWindowSize int
	userRepository    UserRepository
}

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

func (r *sqliteEventLogRepository) LastEventSequence(ctx context.Context) (int64, error) {
	var sequence int64
	if err := r.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM event_log`).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("query last event sequence: %w", err)
	}
	return sequence, nil
}

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

	rows, err := r.db.QueryContext(ctx, `
SELECT user_node_id, user_id, node_id, seq, sender, body, COALESCE(metadata, ''), created_at_hlc
FROM messages
WHERE (user_node_id = ? AND user_id = ?)
   OR EXISTS (
        SELECT 1
        FROM users broadcast_users
        WHERE broadcast_users.node_id = messages.user_node_id
          AND broadcast_users.user_id = messages.user_id
          AND broadcast_users.role = ?
          AND broadcast_users.deleted_at_hlc IS NULL
   )
   OR EXISTS (
        SELECT 1
        FROM channel_subscriptions subscriptions
        WHERE subscriptions.subscriber_node_id = ?
          AND subscriptions.subscriber_user_id = ?
          AND subscriptions.channel_node_id = messages.user_node_id
          AND subscriptions.channel_user_id = messages.user_id
          AND subscriptions.deleted_at_hlc IS NULL
          AND messages.created_at_hlc >= subscriptions.subscribed_at_hlc
   )
ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
LIMIT ?
`, key.NodeID, key.UserID, RoleBroadcast, key.NodeID, key.UserID, limit)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
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
		return nil, fmt.Errorf("iterate messages: %w", err)
	}
	return messages, nil
}

func (r *sqliteMessageProjectionRepository) BuildMessageSnapshotRows(ctx context.Context, producer int64) ([]*clusterproto.SnapshotRow, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT user_node_id, user_id, node_id, seq, sender, body, COALESCE(metadata, ''), created_at_hlc
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

func (r *sqliteMessageProjectionRepository) applyMessageCreatedTx(ctx context.Context, tx *sql.Tx, message Message) error {
	key := message.UserKey()
	if err := validateMessageIdentity(key, message.NodeID, message.Seq); err != nil {
		return err
	}
	if _, err := r.userRepository.GetUserTx(ctx, tx, key, false); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender, body, metadata, created_at_hlc)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
`, message.UserNodeID, message.UserID, message.NodeID, message.Seq, message.Sender, message.Body, nullIfEmpty(message.Metadata), message.CreatedAt.String()); err != nil {
		if !isUniqueConstraint(err) {
			return fmt.Errorf("insert message projection: %w", err)
		}
	}
	if err := r.trimMessagesForUserTx(ctx, tx, key); err != nil {
		return err
	}
	return nil
}

func (r *sqliteMessageProjectionRepository) applyMessageSnapshotRowTx(ctx context.Context, tx *sql.Tx, producer int64, row *clusterproto.SnapshotRow) (UserKey, error) {
	if row == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	messageRow := row.GetMessage()
	if messageRow == nil {
		return UserKey{}, fmt.Errorf("%w: messages snapshot contains non-message row", ErrInvalidInput)
	}
	key := UserKey{NodeID: messageRow.UserNodeId, UserID: messageRow.UserId}
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

	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender, body, metadata, created_at_hlc)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
`, messageRow.UserNodeId, messageRow.UserId, messageRow.NodeId, messageRow.Seq, messageRow.Sender, messageRow.Body, nullIfEmpty(messageRow.Metadata),
		messageRow.CreatedAtHlc); err != nil {
		if isUniqueConstraint(err) {
			return key, nil
		}
		return UserKey{}, fmt.Errorf("insert snapshot message: %w", err)
	}
	return key, nil
}

func (r *sqliteMessageProjectionRepository) listRawMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT user_node_id, user_id, node_id, seq, sender, body, COALESCE(metadata, ''), created_at_hlc
FROM messages
WHERE user_node_id = ? AND user_id = ?
ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
LIMIT ?
`, key.NodeID, key.UserID, limit)
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

type sqliteUserRepository struct {
	db *sql.DB
}

type sqliteSubscriptionRepository struct {
	db *sql.DB
}

type sqliteMessageTrimRepository struct {
	db    *sql.DB
	clock *clock.Clock
}

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

func (r *sqliteSubscriptionRepository) ListActiveSubscriptions(ctx context.Context, subscriber UserKey) ([]Subscription, error) {
	if err := subscriber.Validate(); err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id, subscribed_at_hlc, deleted_at_hlc, origin_node_id
FROM channel_subscriptions
WHERE subscriber_node_id = ? AND subscriber_user_id = ? AND deleted_at_hlc IS NULL
ORDER BY channel_node_id ASC, channel_user_id ASC
`, subscriber.NodeID, subscriber.UserID)
	if err != nil {
		return nil, fmt.Errorf("list active subscriptions: %w", err)
	}
	defer rows.Close()

	subscriptions := make([]Subscription, 0)
	for rows.Next() {
		subscription, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active subscriptions: %w", err)
	}
	return subscriptions, nil
}

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

func (r *sqliteMessageProjectionRepository) trimMessagesForUserTx(ctx context.Context, tx *sql.Tx, key UserKey) error {
	if err := key.Validate(); err != nil {
		return err
	}
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
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

func (s *Store) recordPendingProjection(ctx context.Context, event Event, reason error) error {
	now := s.clock.Now().String()
	message := ""
	if reason != nil {
		message = strings.TrimSpace(reason.Error())
	}
	if message == "" {
		message = "projection failed"
	}
	_, err := s.db.ExecContext(ctx, `
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

func (s *Store) clearPendingProjection(ctx context.Context, originNodeID, eventID int64) error {
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

func (s *Store) projectionStats(ctx context.Context) (ProjectionStats, error) {
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

func (s *Store) listPendingProjectionEvents(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if s.engine != EngineSQLite {
		rows, err := s.db.QueryContext(ctx, `
SELECT origin_node_id, event_id
FROM pending_projections
ORDER BY last_failed_at_hlc ASC, origin_node_id ASC, event_id ASC
LIMIT ?
`, limit)
		if err != nil {
			return nil, fmt.Errorf("list pending projection keys: %w", err)
		}
		defer rows.Close()

		var events []Event
		for rows.Next() {
			var originNodeID, eventID int64
			if err := rows.Scan(&originNodeID, &eventID); err != nil {
				return nil, fmt.Errorf("scan pending projection key: %w", err)
			}
			found, err := s.eventLog.ListEventsByOrigin(ctx, originNodeID, eventID-1, 1)
			if err != nil {
				return nil, err
			}
			if len(found) == 1 && found[0].OriginNodeID == originNodeID && found[0].EventID == eventID {
				events = append(events, found[0])
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate pending projection keys: %w", err)
		}
		return events, nil
	}

	rows, err := s.db.QueryContext(ctx, `
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

func (s *Store) projectMessageEvent(ctx context.Context, event Event) error {
	body, ok := event.Body.(*clusterproto.MessageCreatedEvent)
	if !ok {
		return fmt.Errorf("%w: unsupported event body %T", ErrInvalidInput, event.Body)
	}
	message, err := messageFromCreatedEvent(body)
	if err != nil {
		return err
	}
	return s.messageProjection.ApplyMessageCreated(ctx, message)
}

func messageFromCreatedEvent(body *clusterproto.MessageCreatedEvent) (Message, error) {
	if body == nil {
		return Message{}, fmt.Errorf("%w: message created event cannot be nil", ErrInvalidInput)
	}
	key := UserKey{NodeID: body.UserNodeId, UserID: body.UserId}
	if err := validateMessageIdentity(key, body.NodeId, body.Seq); err != nil {
		return Message{}, err
	}
	createdAt, err := parseRequiredTimestamp(body.CreatedAtHlc, "message created_at")
	if err != nil {
		return Message{}, err
	}
	if strings.TrimSpace(body.Sender) == "" {
		return Message{}, fmt.Errorf("%w: sender cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(body.Body) == "" {
		return Message{}, fmt.Errorf("%w: body cannot be empty", ErrInvalidInput)
	}
	return Message{
		UserNodeID: body.UserNodeId,
		UserID:     body.UserId,
		NodeID:     body.NodeId,
		Seq:        body.Seq,
		Sender:     strings.TrimSpace(body.Sender),
		Body:       strings.TrimSpace(body.Body),
		Metadata:   strings.TrimSpace(body.Metadata),
		CreatedAt:  createdAt,
	}, nil
}
