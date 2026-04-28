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

type storeBackend interface {
	Name() string
	Bind(storeBackendBindings) error
	CreateMessage(context.Context, *Store, CreateMessageParams) (Message, Event, error)
	EventLog() EventLogRepository
	MessageProjection() MessageProjectionRepository
	NextMessageSeqTx(context.Context, *sql.Tx, UserKey, int64) (int64, error)
	InsertLocalEventTx(context.Context, *sql.Tx, Event) (Event, error)
	StoreReplicatedEventTx(context.Context, *sql.Tx, *internalproto.ReplicatedEvent, Event) (Event, bool, error)
	ListPendingProjectionEvents(context.Context, *sql.DB, int) ([]Event, error)
	ListLocalOriginEventStats(context.Context, *sql.DB) (map[int64]localOriginEventStats, error)
	CountUnconfirmedOriginEvents(context.Context, *sql.DB, int64, int64, int64) (int64, error)
	PruneEventLogOrigin(context.Context, *sql.DB, *clock.Clock, int64, int) (int64, error)
	Close() error
}

type txMessageProjectionRepository interface {
	applyMessageCreatedTx(context.Context, *sql.Tx, Message) error
}

type storeBackendBindings struct {
	NodeID            int64
	Clock             *clock.Clock
	IDs               *clock.IDGenerator
	MessageWindowSize int
	UserRepository    UserRepository
	Subscriptions     SubscriptionRepository
	Blacklists        BlacklistRepository
	MessageTrim       MessageTrimRepository
}

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

type sqliteStoreBackend struct {
	db                *sql.DB
	nodeID            int64
	clock             *clock.Clock
	ids               *clock.IDGenerator
	messageWindowSize int
	eventLog          *sqliteEventLogRepository
	messageProjection MessageProjectionRepository
}

func (b *sqliteStoreBackend) Name() string {
	return EngineSQLite
}

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

func (b *sqliteStoreBackend) EventLog() EventLogRepository {
	return b.eventLog
}

func (b *sqliteStoreBackend) MessageProjection() MessageProjectionRepository {
	return b.messageProjection
}

func (b *sqliteStoreBackend) CreateMessage(ctx context.Context, s *Store, params CreateMessageParams) (Message, Event, error) {
	projection, ok := b.messageProjection.(txMessageProjectionRepository)
	if !ok {
		return b.createMessageLegacy(ctx, s, params)
	}
	return b.createMessageFast(ctx, s, params, projection)
}

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

func (b *sqliteStoreBackend) NextMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	return nextSQLiteMessageSeq(ctx, tx, key, nodeID)
}

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

func (b *sqliteStoreBackend) Close() error {
	return nil
}

type pebbleStoreBackend struct {
	db                    *pebble.DB
	sqlDB                 *sql.DB
	profile               PebbleProfile
	writes                *pebbleWriteCoordinator
	eventLog              *pebbleEventLogRepository
	messageProjection     MessageProjectionRepository
	messageProjectionRepo *pebbleMessageProjectionRepository
	messageSequences      *pebbleMessageSequenceRepository
	peerAckCursors        *pebblePeerAckCursorRepository
	originCursors         *pebbleOriginCursorRepository
	pendingProjections    *pebblePendingProjectionRepository
	localMessageRequests  chan pebbleLocalMessageWriteRequest
	localMessageCloseCh   chan chan error
	localMessageDone      chan struct{}
	localMessageStats     pebbleLocalMessageBatchStats
	localMessageMu        sync.Mutex
	localMessageClosed    bool
}

func (b *pebbleStoreBackend) Name() string {
	return EnginePebble
}

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

func (b *pebbleStoreBackend) EventLog() EventLogRepository {
	return b.eventLog
}

func (b *pebbleStoreBackend) MessageProjection() MessageProjectionRepository {
	return b.messageProjection
}

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

func (b *pebbleStoreBackend) NextMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	if b.messageSequences == nil {
		return 0, fmt.Errorf("pebble message sequence repository is not initialized")
	}
	return b.messageSequences.NextSequenceTx(ctx, tx, key, nodeID)
}

func (b *pebbleStoreBackend) InsertLocalEventTx(ctx context.Context, tx *sql.Tx, event Event) (Event, error) {
	return b.eventLog.Append(ctx, event)
}

func (b *pebbleStoreBackend) StoreReplicatedEventTx(ctx context.Context, tx *sql.Tx, rawEvent *internalproto.ReplicatedEvent, decoded Event) (Event, bool, error) {
	return b.eventLog.AppendReplicated(ctx, decoded)
}

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

func (b *pebbleStoreBackend) CountUnconfirmedOriginEvents(ctx context.Context, db *sql.DB, originNodeID, ackedEventID, fallbackCount int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, nil
	}
	if ackedEventID <= 0 {
		return fallbackCount, nil
	}
	return b.eventLog.CountEventsByOrigin(ctx, originNodeID, ackedEventID)
}

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
