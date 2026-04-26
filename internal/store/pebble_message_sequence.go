package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

type pebbleMessageSequenceRepository struct {
	db     *pebble.DB
	sqlDB  *sql.DB
	writes *pebbleWriteCoordinator
	mu     sync.Mutex
	next   map[string]int64
}

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

func (r *pebbleMessageSequenceRepository) storeCommittedNextLocked(cacheKey string, next int64) {
	if r.next == nil {
		r.next = make(map[string]int64)
	}
	r.next[cacheKey] = next
}

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
