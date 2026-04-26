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

const (
	pebbleProjectionLockShards      = 256
	pebbleMessageTrimWorkerMaxUsers = 64
	pebbleMessageTrimWorkerDelay    = 25 * time.Millisecond
	pebbleMessageTrimHardSlack      = 128
	pebbleMessageUserStateVersion   = byte(1)
)

type pebbleMessageUserState struct {
	StoredCount int64
	MaxSeq      int64
	TrimNeeded  bool
}

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

func pebbleProjectionShardIndex(key UserKey) int {
	hash := uint64(key.NodeID)*1_146_959_810_393_466_559 ^ uint64(key.UserID)*1_099_511_628_211
	return int(hash % pebbleProjectionLockShards)
}

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

func (r *pebbleMessageProjectionRepository) seedMessageUserStateLocked(ctx context.Context, key UserKey) (pebbleMessageUserState, error) {
	if err := key.Validate(); err != nil {
		return pebbleMessageUserState{}, err
	}
	windowSize := normalizeMessageWindowSize(r.messageWindowSize)
	hardThreshold := pebbleMessageTrimHardThreshold(windowSize)
	prefix := []byte(fmt.Sprintf("message/user/%020d/%020d/", key.NodeID, key.UserID))
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

func (r *pebbleMessageProjectionRepository) prepareMessageWrite(batch *pebble.Batch, message Message, state pebbleMessageUserState) (pebbleMessageUserState, error) {
	if batch == nil {
		return pebbleMessageUserState{}, fmt.Errorf("%w: pebble batch cannot be nil", ErrInvalidInput)
	}
	value, err := pebbleMessageValue(message)
	if err != nil {
		return pebbleMessageUserState{}, err
	}
	refValue := pebbleMessageIndexValue(pebbleMessageRefFromMessage(message))
	if err := batch.Set(pebbleMessageIDKey(message), value, nil); err != nil {
		return pebbleMessageUserState{}, fmt.Errorf("write message primary projection: %w", err)
	}
	if err := batch.Set(pebbleMessageUserKey(message), refValue, nil); err != nil {
		return pebbleMessageUserState{}, fmt.Errorf("write message user index: %w", err)
	}
	if err := batch.Set(pebbleMessageProducerKey(message), refValue, nil); err != nil {
		return pebbleMessageUserState{}, fmt.Errorf("write message producer index: %w", err)
	}

	state.StoredCount++
	if message.Seq > state.MaxSeq {
		state.MaxSeq = message.Seq
	}
	if state.StoredCount > int64(normalizeMessageWindowSize(r.messageWindowSize)) {
		state.TrimNeeded = true
	}
	if err := batch.Set(pebbleMessageUserStateKey(message.UserKey()), encodePebbleMessageUserState(state), nil); err != nil {
		return pebbleMessageUserState{}, fmt.Errorf("write message user state: %w", err)
	}
	return state, nil
}

func (r *pebbleMessageProjectionRepository) trimMessagesForUser(ctx context.Context, key UserKey, forceSync bool) error {
	unlock := r.lockUsers([]UserKey{key})
	defer unlock()
	return r.trimMessagesForUserLocked(ctx, key, forceSync)
}

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

	batch := r.db.NewBatch()
	for _, message := range messages[windowSize:] {
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

func pebbleMessageTrimHardThreshold(windowSize int) int {
	if windowSize <= pebbleMessageTrimHardSlack {
		return windowSize + pebbleMessageTrimHardSlack
	}
	return windowSize + pebbleMessageTrimHardSlack
}

func pebbleMessageUserStateKey(key UserKey) []byte {
	return fmt.Appendf(nil, "meta/message_user_state/%020d/%020d", key.NodeID, key.UserID)
}

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

func maxMessageSequence(messages []Message) int64 {
	var maxSeq int64
	for _, message := range messages {
		if message.Seq > maxSeq {
			maxSeq = message.Seq
		}
	}
	return maxSeq
}
