package store

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

const pebbleLocalMessageBatchMaxOps = 128

type pebbleLocalMessageWriteRequest struct {
	params   CreateMessageParams
	response chan pebbleLocalMessageWriteResult
}

type pebbleLocalMessageWriteResult struct {
	message Message
	event   Event
	err     error
}

type pebbleSequenceReservation struct {
	cacheKey string
	key      []byte
	next     int64
}

type pebbleLocalMessageBatchStats struct {
	noSyncBatches    atomic.Uint64
	forceSyncBatches atomic.Uint64
}

type pebbleLocalMessageBatchStatsSnapshot struct {
	NoSyncBatches    uint64
	ForceSyncBatches uint64
}

func (s *pebbleLocalMessageBatchStats) record(mode PebbleMessageSyncMode) {
	switch mode {
	case PebbleMessageSyncModeForceSync:
		s.forceSyncBatches.Add(1)
	default:
		s.noSyncBatches.Add(1)
	}
}

func (s *pebbleLocalMessageBatchStats) snapshot() pebbleLocalMessageBatchStatsSnapshot {
	return pebbleLocalMessageBatchStatsSnapshot{
		NoSyncBatches:    s.noSyncBatches.Load(),
		ForceSyncBatches: s.forceSyncBatches.Load(),
	}
}

func (b *pebbleStoreBackend) startLocalMessageLoop() {
	if b == nil {
		return
	}
	b.localMessageMu.Lock()
	defer b.localMessageMu.Unlock()

	if b.localMessageRequests != nil {
		return
	}
	b.localMessageRequests = make(chan pebbleLocalMessageWriteRequest, pebbleLocalMessageBatchMaxOps*8)
	b.localMessageCloseCh = make(chan chan error, 1)
	b.localMessageDone = make(chan struct{})
	go b.runLocalMessageLoop()
}

func (b *pebbleStoreBackend) submitLocalMessage(ctx context.Context, params CreateMessageParams) (Message, Event, error) {
	if b == nil {
		return Message{}, Event{}, fmt.Errorf("pebble local message loop is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return Message{}, Event{}, err
	}

	response := make(chan pebbleLocalMessageWriteResult, 1)

	b.localMessageMu.Lock()
	if b.localMessageClosed || b.localMessageRequests == nil {
		b.localMessageMu.Unlock()
		return Message{}, Event{}, fmt.Errorf("pebble local message loop is closed")
	}
	b.localMessageMu.Unlock()

	b.localMessageRequests <- pebbleLocalMessageWriteRequest{
		params:   params,
		response: response,
	}
	result := <-response
	return result.message, result.event, result.err
}

func (b *pebbleStoreBackend) closeLocalMessageLoop() error {
	if b == nil {
		return nil
	}

	b.localMessageMu.Lock()
	if b.localMessageRequests == nil {
		b.localMessageMu.Unlock()
		return nil
	}
	if b.localMessageClosed {
		b.localMessageMu.Unlock()
		<-b.localMessageDone
		return nil
	}
	b.localMessageClosed = true
	b.localMessageMu.Unlock()

	response := make(chan error, 1)
	b.localMessageCloseCh <- response
	err := <-response
	<-b.localMessageDone
	return err
}

func (b *pebbleStoreBackend) runLocalMessageLoop() {
	defer close(b.localMessageDone)

	drainQueued := func() []pebbleLocalMessageWriteRequest {
		pending := make([]pebbleLocalMessageWriteRequest, 0, pebbleLocalMessageBatchMaxOps)
		for len(pending) < pebbleLocalMessageBatchMaxOps {
			select {
			case req := <-b.localMessageRequests:
				pending = append(pending, req)
			default:
				return pending
			}
		}
		return pending
	}

	for {
		select {
		case req := <-b.localMessageRequests:
			pending := []pebbleLocalMessageWriteRequest{req}
			runtime.Gosched()
			pending = append(pending, drainQueued()...)
			for len(pending) > 0 {
				limit := len(pending)
				if limit > pebbleLocalMessageBatchMaxOps {
					limit = pebbleLocalMessageBatchMaxOps
				}
				chunk := pending[:limit]
				for len(chunk) > 0 {
					segmentEnd := contiguousLocalMessageSyncModePrefix(chunk)
					b.processLocalMessageBatch(chunk[:segmentEnd])
					chunk = chunk[segmentEnd:]
				}
				pending = pending[limit:]
				if len(pending) == 0 {
					pending = append(pending, drainQueued()...)
				}
			}
		case response := <-b.localMessageCloseCh:
			response <- nil
			return
		}
	}
}

func (b *pebbleStoreBackend) processLocalMessageBatch(requests []pebbleLocalMessageWriteRequest) {
	if len(requests) == 0 {
		return
	}
	if b == nil || b.db == nil || b.eventLog == nil || b.messageSequences == nil || b.messageProjectionRepo == nil {
		respondLocalMessageBatchError(requests, fmt.Errorf("pebble local message loop is not initialized"))
		return
	}

	projection := b.messageProjectionRepo
	unlockUsers := projection.lockUsers(uniqueMessageRecipients(requests))
	defer unlockUsers()

	ctx := context.Background()
	userStates := make(map[UserKey]pebbleMessageUserState)
	sequenceReservations := make(map[UserKey]pebbleSequenceReservation)
	committedNextByCacheKey := make(map[string]int64)
	dirtyUsers := make(map[UserKey]struct{})
	hardTrimUsers := make(map[UserKey]struct{})
	results := make([]pebbleLocalMessageWriteResult, len(requests))

	batch := b.db.NewBatch()
	defer batch.Close()

	b.eventLog.mu.Lock()
	defer b.eventLog.mu.Unlock()

	currentEventSequence, err := b.eventLog.lastEventSequenceLocked()
	if err != nil {
		respondLocalMessageBatchError(requests, err)
		return
	}

	nextEventSequence := currentEventSequence + 1
	windowSize := normalizeMessageWindowSize(projection.messageWindowSize)
	trimThreshold := int64(pebbleMessageTrimThreshold(windowSize))
	hardTrimThreshold := int64(pebbleMessageTrimHardThreshold(windowSize))
	syncMode := requests[0].params.PebbleMessageSyncMode
	forceSync := syncMode == PebbleMessageSyncModeForceSync

	for i, request := range requests {
		if request.params.PebbleMessageSyncMode != syncMode {
			respondLocalMessageBatchError(requests, fmt.Errorf("%w: local pebble message batch contains mixed sync modes", ErrInvalidInput))
			return
		}
		key := request.params.UserKey

		state, ok := userStates[key]
		if !ok {
			state, err = projection.messageUserStateLocked(ctx, key)
			if err != nil {
				respondLocalMessageBatchError(requests, err)
				return
			}
		}

		reservation, ok := sequenceReservations[key]
		if !ok {
			cacheKey, sequenceKey, next, err := b.messageSequences.LoadNextSequence(ctx, key, b.eventLog.nodeID)
			if err != nil {
				respondLocalMessageBatchError(requests, err)
				return
			}
			reservation = pebbleSequenceReservation{
				cacheKey: cacheKey,
				key:      sequenceKey,
				next:     next,
			}
		}

		now := b.eventLog.clock.Now()
		message := Message{
			Recipient: key,
			NodeID:    b.eventLog.nodeID,
			Seq:       reservation.next,
			Sender:    request.params.Sender,
			Body:      append([]byte(nil), request.params.Body...),
			CreatedAt: now,
		}
		reservation.next++

		event := Event{
			Sequence:        nextEventSequence,
			EventID:         b.eventLog.ids.Next(),
			EventType:       EventTypeMessageCreated,
			Aggregate:       "message",
			AggregateNodeID: message.NodeID,
			AggregateID:     message.Seq,
			HLC:             now,
			OriginNodeID:    b.eventLog.nodeID,
			Body:            messageCreatedProtoFromMessage(message),
		}
		value, err := eventLogValue(event)
		if err != nil {
			respondLocalMessageBatchError(requests, err)
			return
		}
		if err := b.eventLog.writeStoredEventToBatch(batch, event, value); err != nil {
			respondLocalMessageBatchError(requests, err)
			return
		}

		state, err = projection.prepareMessageWrite(batch, message, state)
		if err != nil {
			respondLocalMessageBatchError(requests, err)
			return
		}

		userStates[key] = state
		sequenceReservations[key] = reservation
		committedNextByCacheKey[reservation.cacheKey] = reservation.next

		switch {
		case state.StoredCount > hardTrimThreshold:
			hardTrimUsers[key] = struct{}{}
		case state.StoredCount > trimThreshold:
			dirtyUsers[key] = struct{}{}
		}

		results[i] = pebbleLocalMessageWriteResult{
			message: message,
			event:   event,
		}
		nextEventSequence++
	}

	if len(results) > 0 {
		if err := batch.Set(pebbleEventSequenceKey, encodeInt64(results[len(results)-1].event.Sequence), nil); err != nil {
			respondLocalMessageBatchError(requests, fmt.Errorf("write event sequence meta: %w", err))
			return
		}
	}
	for _, reservation := range sequenceReservations {
		if err := batch.Set(reservation.key, encodeInt64(reservation.next), nil); err != nil {
			respondLocalMessageBatchError(requests, fmt.Errorf("write pebble message sequence: %w", err))
			return
		}
	}

	commitOptions := pebble.NoSync
	if forceSync {
		commitOptions = pebble.Sync
	}
	if err := batch.Commit(commitOptions); err != nil {
		respondLocalMessageBatchError(requests, fmt.Errorf("commit local pebble message batch: %w", err))
		return
	}
	b.localMessageStats.record(syncMode)

	b.eventLog.lastSequence = results[len(results)-1].event.Sequence
	b.eventLog.sequenceLoaded = true
	b.messageSequences.StoreCommittedNextByCacheKey(committedNextByCacheKey)

	for _, key := range sortedUserKeys(hardTrimUsers) {
		if err := projection.trimMessagesForUserLocked(ctx, key, forceSync); err != nil {
			dirtyUsers[key] = struct{}{}
			continue
		}
		delete(dirtyUsers, key)
	}

	for key := range dirtyUsers {
		projection.scheduleTrim(key)
	}

	for i, request := range requests {
		request.response <- results[i]
	}
}

func respondLocalMessageBatchError(requests []pebbleLocalMessageWriteRequest, err error) {
	for _, request := range requests {
		request.response <- pebbleLocalMessageWriteResult{err: err}
	}
}

func contiguousLocalMessageSyncModePrefix(requests []pebbleLocalMessageWriteRequest) int {
	if len(requests) == 0 {
		return 0
	}
	mode := requests[0].params.PebbleMessageSyncMode
	for i := 1; i < len(requests); i++ {
		if requests[i].params.PebbleMessageSyncMode != mode {
			return i
		}
	}
	return len(requests)
}

func uniqueMessageRecipients(requests []pebbleLocalMessageWriteRequest) []UserKey {
	seen := make(map[UserKey]struct{}, len(requests))
	keys := make([]UserKey, 0, len(requests))
	for _, request := range requests {
		if _, ok := seen[request.params.UserKey]; ok {
			continue
		}
		seen[request.params.UserKey] = struct{}{}
		keys = append(keys, request.params.UserKey)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].NodeID != keys[j].NodeID {
			return keys[i].NodeID < keys[j].NodeID
		}
		return keys[i].UserID < keys[j].UserID
	})
	return keys
}
