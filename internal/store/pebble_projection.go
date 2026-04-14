package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	clusterproto "github.com/tursom/turntf/internal/proto"
)

var pebbleEventSequenceKey = []byte("meta/event_sequence")

type pebbleEventLogRepository struct {
	db     *pebble.DB
	ids    *clock.IDGenerator
	nodeID int64
	clock  *clock.Clock
	mu     sync.Mutex
}

type pebbleMessageProjectionRepository struct {
	db                *pebble.DB
	messageWindowSize int
	userRepository    UserRepository
	subscriptions     SubscriptionRepository
	blacklists        BlacklistRepository
	messageTrim       MessageTrimRepository
	mu                sync.Mutex
}

func (r *pebbleEventLogRepository) Append(ctx context.Context, event Event) (Event, error) {
	if r.ids == nil {
		return Event{}, fmt.Errorf("append event before id generator initialization")
	}
	if r.clock == nil {
		return Event{}, fmt.Errorf("append event before clock initialization")
	}
	event.EventID = r.ids.Next()
	event.OriginNodeID = r.nodeID
	stored, _, err := r.appendStored(ctx, event)
	return stored, err
}

func (r *pebbleEventLogRepository) AppendReplicated(ctx context.Context, event Event) (Event, bool, error) {
	if event.EventID <= 0 || event.OriginNodeID <= 0 {
		return Event{}, false, fmt.Errorf("%w: replicated event id and origin node id are required", ErrInvalidInput)
	}
	return r.appendStored(ctx, event)
}

func (r *pebbleEventLogRepository) appendStored(ctx context.Context, event Event) (Event, bool, error) {
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

	sequence, err := r.nextSequence()
	if err != nil {
		return Event{}, false, err
	}
	event.Sequence = sequence

	batch := r.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(pebbleEventSeqKey(sequence), value, pebble.Sync); err != nil {
		return Event{}, false, fmt.Errorf("write event sequence index: %w", err)
	}
	if err := batch.Set(originKey, encodeInt64(sequence), pebble.Sync); err != nil {
		return Event{}, false, fmt.Errorf("write event origin index: %w", err)
	}
	if err := batch.Set(pebbleEventSequenceKey, encodeInt64(sequence), pebble.Sync); err != nil {
		return Event{}, false, fmt.Errorf("write event sequence meta: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return Event{}, false, fmt.Errorf("commit event append: %w", err)
	}
	return event, true, nil
}

func (r *pebbleEventLogRepository) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	lower := pebbleEventSeqKey(afterSequence + 1)
	upper := prefixUpperBound([]byte("event/seq/"))
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

func (r *pebbleEventLogRepository) ListEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64, limit int) ([]Event, error) {
	if originNodeID <= 0 {
		return nil, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	prefix := []byte(fmt.Sprintf("event/origin/%020d/", originNodeID))
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

func (r *pebbleEventLogRepository) CountEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64) (int64, error) {
	if originNodeID <= 0 {
		return 0, nil
	}
	prefix := []byte(fmt.Sprintf("event/origin/%020d/", originNodeID))
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

func (r *pebbleEventLogRepository) LastEventSequence(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sequence, ok, err := r.readSequence(pebbleEventSequenceKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return sequence, nil
}

func (r *pebbleEventLogRepository) ListOriginProgress(ctx context.Context) ([]OriginProgress, error) {
	prefix := []byte("event/origin/")
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

func (r *pebbleEventLogRepository) nextSequence() (int64, error) {
	current, ok, err := r.readSequence(pebbleEventSequenceKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 1, nil
	}
	return current + 1, nil
}

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

func (r *pebbleEventLogRepository) eventBySequence(sequence int64) (Event, error) {
	value, closer, err := r.db.Get(pebbleEventSeqKey(sequence))
	if err != nil {
		return Event{}, fmt.Errorf("read event sequence %d: %w", sequence, err)
	}
	defer closer.Close()
	return eventFromPebbleValue(pebbleEventSeqKey(sequence), value)
}

func (r *pebbleMessageProjectionRepository) ApplyMessageCreated(ctx context.Context, message Message) error {
	key := message.UserKey()
	if err := validateMessageIdentity(key, message.NodeID, message.Seq); err != nil {
		return err
	}
	if _, err := r.userRepository.GetUser(ctx, key, false); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if ok, err := r.messageExists(message); err != nil {
		return err
	} else if ok {
		return nil
	}
	if err := r.putMessage(message); err != nil {
		return err
	}
	return r.trimMessagesForUser(ctx, key)
}

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

func (r *pebbleMessageProjectionRepository) BuildMessageSnapshotRows(ctx context.Context, producer int64) ([]*clusterproto.SnapshotRow, error) {
	if producer <= 0 {
		return nil, fmt.Errorf("%w: producer cannot be empty", ErrInvalidInput)
	}
	prefix := []byte(fmt.Sprintf("message/producer/%020d/", producer))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open snapshot message iterator: %w", err)
	}
	defer iter.Close()

	rows := make([]*clusterproto.SnapshotRow, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		message, err := messageFromPebbleValue(iter.Value())
		if err != nil {
			return nil, err
		}
		rows = append(rows, snapshotRowFromMessage(message))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate snapshot messages: %w", err)
	}
	return rows, nil
}

func (r *pebbleMessageProjectionRepository) ApplyMessageSnapshotRows(ctx context.Context, producer int64, rows []*clusterproto.SnapshotRow) error {
	r.mu.Lock()
	defer r.mu.Unlock()

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
		if err := r.trimMessagesForUser(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

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
	if ok, err := r.messageExists(message); err != nil {
		return UserKey{}, err
	} else if ok {
		return key, nil
	}
	if err := r.putMessage(message); err != nil {
		return UserKey{}, err
	}
	return key, nil
}

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

func (r *pebbleMessageProjectionRepository) putMessage(message Message) error {
	value, err := pebbleMessageValue(message)
	if err != nil {
		return err
	}
	batch := r.db.NewBatch()
	defer batch.Close()
	for _, key := range pebbleMessageKeys(message) {
		if err := batch.Set(key, value, pebble.Sync); err != nil {
			return fmt.Errorf("write message projection: %w", err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit message projection: %w", err)
	}
	return nil
}

func (r *pebbleMessageProjectionRepository) listRawMessagesByUser(ctx context.Context, key UserKey, limit int, since *clock.Timestamp) ([]Message, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	prefix := []byte(fmt.Sprintf("message/user/%020d/%020d/", key.NodeID, key.UserID))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open user message iterator: %w", err)
	}
	defer iter.Close()

	messages := make([]Message, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		message, err := messageFromPebbleValue(iter.Value())
		if err != nil {
			return nil, err
		}
		if since != nil && message.CreatedAt.Compare(*since) < 0 {
			continue
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

func (r *pebbleMessageProjectionRepository) trimMessagesForUser(ctx context.Context, key UserKey) error {
	messages, err := r.listRawMessagesByUser(ctx, key, 0, nil)
	if err != nil {
		return err
	}
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	if len(messages) <= windowSize {
		return nil
	}

	batch := r.db.NewBatch()
	defer batch.Close()
	for _, message := range messages[windowSize:] {
		for _, key := range pebbleMessageKeys(message) {
			if err := batch.Delete(key, pebble.Sync); err != nil {
				return fmt.Errorf("delete trimmed message: %w", err)
			}
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit message trim: %w", err)
	}
	return r.messageTrim.RecordMessageTrim(ctx, int64(len(messages)-windowSize))
}

func pebbleMessageValue(message Message) ([]byte, error) {
	value, err := gproto.Marshal(snapshotRowFromMessage(message))
	if err != nil {
		return nil, fmt.Errorf("marshal pebble message: %w", err)
	}
	return value, nil
}

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

func pebbleEventSeqKey(sequence int64) []byte {
	return fmt.Appendf(nil, "event/seq/%020d", sequence)
}

func pebbleEventOriginKey(originNodeID, eventID int64) []byte {
	return fmt.Appendf(nil, "event/origin/%020d/%020d", originNodeID, eventID)
}

func pebbleMessageIDKey(message Message) []byte {
	return fmt.Appendf(nil, "message/id/%020d/%020d/%020d/%020d", message.Recipient.NodeID, message.Recipient.UserID, message.NodeID, message.Seq)
}

func pebbleMessageUserKey(message Message) []byte {
	return fmt.Appendf(nil, "message/user/%020d/%020d/%s/%020d/%020d",
		message.Recipient.NodeID, message.Recipient.UserID, descendingString(message.CreatedAt.String()), message.NodeID, math.MaxInt64-message.Seq)
}

func pebbleMessageProducerKey(message Message) []byte {
	return fmt.Appendf(nil, "message/producer/%020d/%020d/%020d/%s/%020d",
		message.NodeID, message.Recipient.NodeID, message.Recipient.UserID, descendingString(message.CreatedAt.String()), math.MaxInt64-message.Seq)
}

func pebbleMessageKeys(message Message) [][]byte {
	return [][]byte{
		pebbleMessageIDKey(message),
		pebbleMessageUserKey(message),
		pebbleMessageProducerKey(message),
	}
}

func parsePebbleSequenceKey(key []byte) int64 {
	var sequence int64
	_, _ = fmt.Sscanf(string(key), "event/seq/%020d", &sequence)
	return sequence
}

func parsePebbleOriginKey(key []byte) (int64, int64, error) {
	var originNodeID, eventID int64
	if _, err := fmt.Sscanf(string(key), "event/origin/%020d/%020d", &originNodeID, &eventID); err != nil {
		return 0, 0, fmt.Errorf("parse event origin key %q: %w", key, err)
	}
	return originNodeID, eventID, nil
}

func encodeInt64(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

func decodeInt64(value []byte) int64 {
	return int64(binary.BigEndian.Uint64(value))
}

func prefixUpperBound(prefix []byte) []byte {
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

func descendingString(value string) string {
	buf := make([]byte, len(value))
	for i := range value {
		buf[i] = 0xff - value[i]
	}
	return string(buf)
}

func messageIdentity(message Message) string {
	return fmt.Sprintf("%d/%d/%d/%d", message.Recipient.NodeID, message.Recipient.UserID, message.NodeID, message.Seq)
}

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
