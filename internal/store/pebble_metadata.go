package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/tursom/turntf/internal/clock"
)

const pebbleCursorValueVersion = byte(1)

type pebblePeerAckCursorRepository struct {
	db     *pebble.DB
	writes *pebbleWriteCoordinator
	clock  *clock.Clock
	mu     sync.Mutex
}

type pebbleOriginCursorRepository struct {
	db     *pebble.DB
	writes *pebbleWriteCoordinator
	clock  *clock.Clock
	mu     sync.Mutex
}

type pebblePendingProjectionRepository struct {
	db     *pebble.DB
	writes *pebbleWriteCoordinator
	clock  *clock.Clock
	mu     sync.Mutex
}

type pebblePendingProjectionRecord struct {
	EventType        string `json:"event_type"`
	AggregateType    string `json:"aggregate_type"`
	AggregateNodeID  int64  `json:"aggregate_node_id"`
	AggregateID      int64  `json:"aggregate_id"`
	AttemptCount     int64  `json:"attempt_count"`
	LastError        string `json:"last_error"`
	FirstFailedAtHLC string `json:"first_failed_at_hlc"`
	LastFailedAtHLC  string `json:"last_failed_at_hlc"`
}

func (r *pebblePeerAckCursorRepository) Get(ctx context.Context, peerNodeID, originNodeID int64) (PeerAckCursor, error) {
	if peerNodeID <= 0 {
		return PeerAckCursor{}, fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}
	if originNodeID <= 0 {
		return PeerAckCursor{}, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return PeerAckCursor{}, err
	}
	value, closer, err := r.db.Get(pebblePeerAckCursorKey(peerNodeID, originNodeID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return PeerAckCursor{PeerNodeID: peerNodeID, OriginNodeID: originNodeID}, nil
		}
		return PeerAckCursor{}, fmt.Errorf("get pebble peer ack cursor: %w", err)
	}
	defer closer.Close()

	ackedEventID, updatedAt, err := decodePebbleCursorValue(value)
	if err != nil {
		return PeerAckCursor{}, err
	}
	return PeerAckCursor{
		PeerNodeID:   peerNodeID,
		OriginNodeID: originNodeID,
		AckedEventID: ackedEventID,
		UpdatedAt:    updatedAt,
	}, nil
}

func (r *pebblePeerAckCursorRepository) List(ctx context.Context) ([]PeerAckCursor, error) {
	prefix := []byte("meta/peer_ack_cursor/")
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open pebble peer ack cursor iterator: %w", err)
	}
	defer iter.Close()

	cursors := make([]PeerAckCursor, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		peerNodeID, originNodeID, err := parsePebblePeerAckCursorKey(iter.Key())
		if err != nil {
			return nil, err
		}
		ackedEventID, updatedAt, err := decodePebbleCursorValue(iter.Value())
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, PeerAckCursor{
			PeerNodeID:   peerNodeID,
			OriginNodeID: originNodeID,
			AckedEventID: ackedEventID,
			UpdatedAt:    updatedAt,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate pebble peer ack cursors: %w", err)
	}
	return cursors, nil
}

func (r *pebblePeerAckCursorRepository) Upsert(ctx context.Context, peerNodeID, originNodeID, ackedEventID int64) error {
	if peerNodeID <= 0 {
		return fmt.Errorf("%w: peer node id cannot be empty", ErrInvalidInput)
	}
	if originNodeID <= 0 {
		return fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if ackedEventID < 0 {
		return fmt.Errorf("%w: acked event id cannot be negative", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := pebblePeerAckCursorKey(peerNodeID, originNodeID)
	current, ok, err := readPebbleCursorValue(r.db, key)
	if err != nil {
		return err
	}
	if ok && ackedEventID < current {
		ackedEventID = current
	}
	return setPebbleCursorValue(r.db, r.writes, key, ackedEventID, r.clock.Now())
}

func (r *pebbleOriginCursorRepository) Get(ctx context.Context, originNodeID int64) (OriginCursor, error) {
	if originNodeID <= 0 {
		return OriginCursor{}, fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return OriginCursor{}, err
	}
	value, closer, err := r.db.Get(pebbleOriginCursorKey(originNodeID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return OriginCursor{OriginNodeID: originNodeID}, nil
		}
		return OriginCursor{}, fmt.Errorf("get pebble origin cursor: %w", err)
	}
	defer closer.Close()

	appliedEventID, updatedAt, err := decodePebbleCursorValue(value)
	if err != nil {
		return OriginCursor{}, err
	}
	return OriginCursor{
		OriginNodeID:   originNodeID,
		AppliedEventID: appliedEventID,
		UpdatedAt:      updatedAt,
	}, nil
}

func (r *pebbleOriginCursorRepository) List(ctx context.Context) ([]OriginCursor, error) {
	prefix := []byte("meta/origin_cursor/")
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open pebble origin cursor iterator: %w", err)
	}
	defer iter.Close()

	cursors := make([]OriginCursor, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		originNodeID, err := parsePebbleOriginCursorKey(iter.Key())
		if err != nil {
			return nil, err
		}
		appliedEventID, updatedAt, err := decodePebbleCursorValue(iter.Value())
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, OriginCursor{
			OriginNodeID:   originNodeID,
			AppliedEventID: appliedEventID,
			UpdatedAt:      updatedAt,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate pebble origin cursors: %w", err)
	}
	return cursors, nil
}

func (r *pebbleOriginCursorRepository) Upsert(ctx context.Context, originNodeID, appliedEventID int64) error {
	if originNodeID <= 0 {
		return fmt.Errorf("%w: origin node id cannot be empty", ErrInvalidInput)
	}
	if appliedEventID < 0 {
		return fmt.Errorf("%w: applied event id cannot be negative", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := pebbleOriginCursorKey(originNodeID)
	current, ok, err := readPebbleCursorValue(r.db, key)
	if err != nil {
		return err
	}
	if ok && appliedEventID < current {
		appliedEventID = current
	}
	return setPebbleCursorValue(r.db, r.writes, key, appliedEventID, r.clock.Now())
}

func (r *pebblePendingProjectionRepository) Record(ctx context.Context, event Event, reason error) error {
	if event.OriginNodeID <= 0 || event.EventID <= 0 {
		return fmt.Errorf("%w: pending projection event identity is required", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	message := "projection failed"
	if reason != nil && reason.Error() != "" {
		message = reason.Error()
	}
	now := r.clock.Now().String()
	key := pebblePendingProjectionKey(event.OriginNodeID, event.EventID)

	r.mu.Lock()
	defer r.mu.Unlock()

	record := pebblePendingProjectionRecord{
		EventType:        string(event.EventType),
		AggregateType:    event.Aggregate,
		AggregateNodeID:  event.AggregateNodeID,
		AggregateID:      event.AggregateID,
		AttemptCount:     1,
		LastError:        message,
		FirstFailedAtHLC: now,
		LastFailedAtHLC:  now,
	}
	if value, closer, err := r.db.Get(key); err == nil {
		defer closer.Close()
		current, err := decodePebblePendingProjectionRecord(value)
		if err != nil {
			return err
		}
		record.AttemptCount = current.AttemptCount + 1
		record.FirstFailedAtHLC = current.FirstFailedAtHLC
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("read pebble pending projection: %w", err)
	}

	value, err := encodePebblePendingProjectionRecord(record)
	if err != nil {
		return err
	}
	return applyPebbleValueSet(r.db, r.writes, key, value, false)
}

func (r *pebblePendingProjectionRepository) Clear(ctx context.Context, originNodeID, eventID int64) error {
	if originNodeID <= 0 || eventID <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := applyPebbleValueDelete(r.db, r.writes, pebblePendingProjectionKey(originNodeID, eventID), false); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("clear pebble pending projection: %w", err)
	}
	return nil
}

func (r *pebblePendingProjectionRepository) List(ctx context.Context, limit int) ([]pendingProjectionEnvelope, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	prefix := []byte("meta/pending_projection/")
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("open pebble pending projection iterator: %w", err)
	}
	defer iter.Close()

	items := make([]pendingProjectionEnvelope, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		originNodeID, eventID, err := parsePebblePendingProjectionKey(iter.Key())
		if err != nil {
			return nil, err
		}
		record, err := decodePebblePendingProjectionRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		lastFailedAt, err := clock.ParseTimestamp(record.LastFailedAtHLC)
		if err != nil {
			return nil, fmt.Errorf("parse pebble pending projection last_failed_at: %w", err)
		}
		items = append(items, pendingProjectionEnvelope{
			OriginNodeID: originNodeID,
			EventID:      eventID,
			Record:       record,
			LastFailedAt: lastFailedAt,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate pebble pending projections: %w", err)
	}

	sort.Slice(items, func(i, j int) bool {
		if cmp := items[i].LastFailedAt.Compare(items[j].LastFailedAt); cmp != 0 {
			return cmp < 0
		}
		if items[i].OriginNodeID != items[j].OriginNodeID {
			return items[i].OriginNodeID < items[j].OriginNodeID
		}
		return items[i].EventID < items[j].EventID
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (r *pebblePendingProjectionRepository) Stats(ctx context.Context) (ProjectionStats, error) {
	prefix := []byte("meta/pending_projection/")
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return ProjectionStats{}, fmt.Errorf("open pebble pending projection stats iterator: %w", err)
	}
	defer iter.Close()

	var (
		total      int64
		lastFailed *clock.Timestamp
	)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return ProjectionStats{}, err
		}
		record, err := decodePebblePendingProjectionRecord(iter.Value())
		if err != nil {
			return ProjectionStats{}, err
		}
		last, err := clock.ParseTimestamp(record.LastFailedAtHLC)
		if err != nil {
			return ProjectionStats{}, fmt.Errorf("parse pebble pending projection stats timestamp: %w", err)
		}
		total++
		if lastFailed == nil || last.Compare(*lastFailed) > 0 {
			lastCopy := last
			lastFailed = &lastCopy
		}
	}
	if err := iter.Error(); err != nil {
		return ProjectionStats{}, fmt.Errorf("iterate pebble pending projection stats: %w", err)
	}
	return ProjectionStats{
		PendingTotal: total,
		LastFailedAt: lastFailed,
	}, nil
}

type pendingProjectionEnvelope struct {
	OriginNodeID int64
	EventID      int64
	Record       pebblePendingProjectionRecord
	LastFailedAt clock.Timestamp
}

func pebblePeerAckCursorKey(peerNodeID, originNodeID int64) []byte {
	return fmt.Appendf(nil, "meta/peer_ack_cursor/%020d/%020d", peerNodeID, originNodeID)
}

func pebbleOriginCursorKey(originNodeID int64) []byte {
	return fmt.Appendf(nil, "meta/origin_cursor/%020d", originNodeID)
}

func pebblePendingProjectionKey(originNodeID, eventID int64) []byte {
	return fmt.Appendf(nil, "meta/pending_projection/%020d/%020d", originNodeID, eventID)
}

func parsePebblePeerAckCursorKey(key []byte) (int64, int64, error) {
	var peerNodeID, originNodeID int64
	if _, err := fmt.Sscanf(string(key), "meta/peer_ack_cursor/%020d/%020d", &peerNodeID, &originNodeID); err != nil {
		return 0, 0, fmt.Errorf("parse pebble peer ack cursor key %q: %w", key, err)
	}
	return peerNodeID, originNodeID, nil
}

func parsePebbleOriginCursorKey(key []byte) (int64, error) {
	var originNodeID int64
	if _, err := fmt.Sscanf(string(key), "meta/origin_cursor/%020d", &originNodeID); err != nil {
		return 0, fmt.Errorf("parse pebble origin cursor key %q: %w", key, err)
	}
	return originNodeID, nil
}

func parsePebblePendingProjectionKey(key []byte) (int64, int64, error) {
	var originNodeID, eventID int64
	if _, err := fmt.Sscanf(string(key), "meta/pending_projection/%020d/%020d", &originNodeID, &eventID); err != nil {
		return 0, 0, fmt.Errorf("parse pebble pending projection key %q: %w", key, err)
	}
	return originNodeID, eventID, nil
}

func readPebbleCursorValue(db *pebble.DB, key []byte) (int64, bool, error) {
	value, closer, err := db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read pebble cursor value: %w", err)
	}
	defer closer.Close()

	id, _, err := decodePebbleCursorValue(value)
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func setPebbleCursorValue(db *pebble.DB, writes *pebbleWriteCoordinator, key []byte, id int64, updatedAt clock.Timestamp) error {
	return applyPebbleValueSet(db, writes, key, encodePebbleCursorValue(id, updatedAt.String()), false)
}

func applyPebbleValueSet(db *pebble.DB, writes *pebbleWriteCoordinator, key, value []byte, forceSync bool) error {
	if db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}
	batch := db.NewBatch()
	if err := batch.Set(key, value, nil); err != nil {
		_ = batch.Close()
		return err
	}
	return applyPebbleBatch(batch, writes, forceSync)
}

func applyPebbleValueDelete(db *pebble.DB, writes *pebbleWriteCoordinator, key []byte, forceSync bool) error {
	if db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}
	batch := db.NewBatch()
	if err := batch.Delete(key, nil); err != nil {
		_ = batch.Close()
		return err
	}
	return applyPebbleBatch(batch, writes, forceSync)
}

func encodePebbleCursorValue(id int64, updatedAt string) []byte {
	raw := []byte(updatedAt)
	value := make([]byte, 1+8+4+len(raw))
	value[0] = pebbleCursorValueVersion
	copy(value[1:9], encodeInt64(id))
	copy(value[9:13], encodeInt64(int64(len(raw)))[4:])
	copy(value[13:], raw)
	return value
}

func decodePebbleCursorValue(value []byte) (int64, clock.Timestamp, error) {
	if len(value) < 13 {
		return 0, clock.Timestamp{}, fmt.Errorf("%w: invalid pebble cursor value length %d", ErrInvalidInput, len(value))
	}
	if value[0] != pebbleCursorValueVersion {
		return 0, clock.Timestamp{}, fmt.Errorf("%w: unsupported pebble cursor value version %d", ErrInvalidInput, value[0])
	}
	id := decodeInt64(value[1:9])
	rawLen := int(decodeInt64(append(make([]byte, 4), value[9:13]...)))
	if rawLen < 0 || len(value) != 13+rawLen {
		return 0, clock.Timestamp{}, fmt.Errorf("%w: invalid pebble cursor timestamp length %d", ErrInvalidInput, rawLen)
	}
	updatedAt, err := clock.ParseTimestamp(string(value[13:]))
	if err != nil {
		return 0, clock.Timestamp{}, fmt.Errorf("parse pebble cursor timestamp: %w", err)
	}
	return id, updatedAt, nil
}

func encodePebblePendingProjectionRecord(record pebblePendingProjectionRecord) ([]byte, error) {
	value, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal pebble pending projection: %w", err)
	}
	return value, nil
}

func decodePebblePendingProjectionRecord(value []byte) (pebblePendingProjectionRecord, error) {
	var record pebblePendingProjectionRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return pebblePendingProjectionRecord{}, fmt.Errorf("unmarshal pebble pending projection: %w", err)
	}
	return record, nil
}
