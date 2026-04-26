package store

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/tursom/turntf/internal/testutil/benchroot"
)

const storeBenchmarkWarmupPasses = 1

type storeBenchmarkEngineScenario struct {
	name                  string
	engine                string
	pebbleMessageSyncMode PebbleMessageSyncMode
}

func storeBenchmarkEngineScenarios() []storeBenchmarkEngineScenario {
	return []storeBenchmarkEngineScenario{
		{name: EngineSQLite, engine: EngineSQLite},
		{name: EnginePebble + "/" + string(PebbleMessageSyncModeNoSync), engine: EnginePebble, pebbleMessageSyncMode: PebbleMessageSyncModeNoSync},
		{name: EnginePebble + "/" + string(PebbleMessageSyncModeForceSync), engine: EnginePebble, pebbleMessageSyncMode: PebbleMessageSyncModeForceSync},
	}
}

func BenchmarkStoreCreateMessage(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range storeBenchmarkEngineScenarios() {
				for _, payloadSize := range []int{256, 4 << 10} {
					b.Run(fmt.Sprintf("%s/%dB", scenario.name, payloadSize), func(b *testing.B) {
						benchmarkStoreCreateMessage(b, mode, scenario, payloadSize)
					})
				}
			}
		})
	}
}

func BenchmarkStoreCreateMessageSteadyState(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range storeBenchmarkEngineScenarios() {
				for _, payloadSize := range []int{256, 4 << 10} {
					b.Run(fmt.Sprintf("%s/%dB", scenario.name, payloadSize), func(b *testing.B) {
						benchmarkStoreCreateMessageSteadyState(b, mode, scenario, payloadSize)
					})
				}
			}
		})
	}
}

func BenchmarkStoreCreateMessageParallel(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range storeBenchmarkEngineScenarios() {
				for _, tc := range []struct {
					name      string
					userCount int
				}{
					{name: "hotspot", userCount: 1},
					{name: "uniform-1000", userCount: 1000},
				} {
					b.Run(fmt.Sprintf("%s/%s", scenario.name, tc.name), func(b *testing.B) {
						benchmarkStoreCreateMessageParallel(b, mode, scenario, tc.userCount)
					})
				}
			}
		})
	}
}

func BenchmarkStoreListMessagesByUser(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range storeBenchmarkEngineScenarios() {
				for _, history := range []int{100, 1000} {
					b.Run(fmt.Sprintf("%s/history-%d", scenario.name, history), func(b *testing.B) {
						benchmarkStoreListMessagesByUser(b, mode, scenario, history)
					})
				}
			}
		})
	}
}

func BenchmarkStorePruneEventLogOnce(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, scenario := range storeBenchmarkEngineScenarios() {
				for _, tc := range []struct {
					name       string
					retain     int
					eventCount int
				}{
					{name: "retain-128/events-256", retain: 128, eventCount: 256},
					{name: "retain-128/events-4096", retain: 128, eventCount: 4096},
				} {
					b.Run(fmt.Sprintf("%s/%s", scenario.name, tc.name), func(b *testing.B) {
						benchmarkStorePruneEventLogOnce(b, mode, scenario, tc.retain, tc.eventCount)
					})
				}
			}
		})
	}
}

func benchmarkStoreCreateMessage(b *testing.B, mode benchroot.Mode, scenario storeBenchmarkEngineScenario, payloadSize int) {
	ctx := context.Background()
	st, closeStore := openBenchmarkStore(b, mode, scenario, "create-message", 1, DefaultMessageWindowSize, DefaultEventLogMaxEventsPerOrigin)
	b.Cleanup(closeStore)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bench-create-message",
		PasswordHash: "bench-hash",
		Role:         RoleUser,
	})
	if err != nil {
		b.Fatalf("create benchmark user: %v", err)
	}

	payload := bytes.Repeat([]byte("m"), payloadSize)
	runStoreBenchmarkWarmup(b, func() {
		mustCreateBenchmarkMessage(b, st, ctx, user.Key(), payload)
	})
	assertBenchmarkMessageSequenceCounter(b, st, ctx, scenario.engine, user.Key(), 2)
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mustCreateBenchmarkMessage(b, st, ctx, user.Key(), payload)
	}
}

func benchmarkStoreCreateMessageSteadyState(b *testing.B, mode benchroot.Mode, scenario storeBenchmarkEngineScenario, payloadSize int) {
	ctx := context.Background()
	st, closeStore := openBenchmarkStore(b, mode, scenario, "create-message-steady-state", 1, DefaultMessageWindowSize, DefaultEventLogMaxEventsPerOrigin)
	b.Cleanup(closeStore)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bench-create-message-steady-state",
		PasswordHash: "bench-hash",
		Role:         RoleUser,
	})
	if err != nil {
		b.Fatalf("create benchmark user: %v", err)
	}

	payload := bytes.Repeat([]byte("s"), payloadSize)
	for i := 0; i < 2*DefaultMessageWindowSize; i++ {
		mustCreateBenchmarkMessage(b, st, ctx, user.Key(), payload)
	}

	runStoreBenchmarkWarmup(b, func() {
		mustCreateBenchmarkMessage(b, st, ctx, user.Key(), payload)
	})
	assertBenchmarkMessageSequenceCounter(b, st, ctx, scenario.engine, user.Key(), 2)
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustCreateBenchmarkMessage(b, st, ctx, user.Key(), payload)
	}
}

func benchmarkStoreCreateMessageParallel(b *testing.B, mode benchroot.Mode, scenario storeBenchmarkEngineScenario, userCount int) {
	ctx := context.Background()
	st, closeStore := openBenchmarkStore(b, mode, scenario, fmt.Sprintf("create-message-parallel-%d", userCount), 1, DefaultMessageWindowSize, DefaultEventLogMaxEventsPerOrigin)
	b.Cleanup(closeStore)

	users := make([]User, 0, userCount)
	for i := 0; i < userCount; i++ {
		user, _, err := st.CreateUser(ctx, CreateUserParams{
			Username:     fmt.Sprintf("bench-create-message-parallel-%04d", i),
			PasswordHash: "bench-hash",
			Role:         RoleUser,
		})
		if err != nil {
			b.Fatalf("create benchmark user %d: %v", i, err)
		}
		users = append(users, user)
	}

	payload := bytes.Repeat([]byte("p"), 256)
	var counter atomic.Uint64
	runStoreBenchmarkWarmup(b, func() {
		mustCreateBenchmarkMessage(b, st, ctx, users[0].Key(), payload)
	})
	assertBenchmarkMessageSequenceCounter(b, st, ctx, scenario.engine, users[0].Key(), 2)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			index := 0
			if len(users) > 1 {
				index = int(counter.Add(1)-1) % len(users)
			}
			mustCreateBenchmarkMessage(b, st, ctx, users[index].Key(), payload)
		}
	})
}

func benchmarkStoreListMessagesByUser(b *testing.B, mode benchroot.Mode, scenario storeBenchmarkEngineScenario, history int) {
	const limit = 50

	ctx := context.Background()
	st, closeStore := openBenchmarkStore(b, mode, scenario, "list-messages", 1, DefaultMessageWindowSize, DefaultEventLogMaxEventsPerOrigin)
	b.Cleanup(closeStore)

	user, _, err := st.CreateUser(ctx, CreateUserParams{
		Username:     "bench-list-messages",
		PasswordHash: "bench-hash",
		Role:         RoleUser,
	})
	if err != nil {
		b.Fatalf("create benchmark user: %v", err)
	}

	expectedBodies := make([][]byte, 0, history)
	for i := 0; i < history; i++ {
		body := []byte(fmt.Sprintf("list-body-%04d", i))
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  user.Key(),
			Body:    body,
		}); err != nil {
			b.Fatalf("seed message %d: %v", i, err)
		}
		expectedBodies = append(expectedBodies, body)
	}
	wantFirst := expectedBodies[len(expectedBodies)-1]
	wantLast := expectedBodies[len(expectedBodies)-limit]

	runStoreBenchmarkWarmup(b, func() {
		mustListBenchmarkMessages(b, st, ctx, user.Key(), limit, wantFirst, wantLast)
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustListBenchmarkMessages(b, st, ctx, user.Key(), limit, wantFirst, wantLast)
	}
}

func benchmarkStorePruneEventLogOnce(b *testing.B, mode benchroot.Mode, scenario storeBenchmarkEngineScenario, retain, eventCount int) {
	ctx := context.Background()

	b.StopTimer()
	runStoreBenchmarkWarmup(b, func() {
		st, closeStore := openBenchmarkStore(b, mode, scenario, "prune-event-log-warmup", 1, DefaultMessageWindowSize, retain)
		appendBenchmarkUserEvents(b, st, eventCount)

		result, err := st.PruneEventLogOnce(ctx)
		if err != nil {
			closeStore()
			b.Fatalf("warmup prune event log: %v", err)
		}
		mustVerifyBenchmarkPruneResult(b, st, ctx, result, retain, eventCount)
		closeStore()
	})
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		st, closeStore := openBenchmarkStore(b, mode, scenario, "prune-event-log", 1, DefaultMessageWindowSize, retain)
		appendBenchmarkUserEvents(b, st, eventCount)

		b.StartTimer()
		result, err := st.PruneEventLogOnce(ctx)
		b.StopTimer()
		if err != nil {
			closeStore()
			b.Fatalf("prune event log: %v", err)
		}
		mustVerifyBenchmarkPruneResult(b, st, ctx, result, retain, eventCount)
		closeStore()
	}
}

func openBenchmarkStore(tb testing.TB, mode benchroot.Mode, scenario storeBenchmarkEngineScenario, name string, nodeSlot uint16, messageWindowSize int, maxEventsPerOrigin int) (*Store, func()) {
	tb.Helper()

	dir, cleanupDir := mode.MkdirTemp(tb, "turntf-store-bench-*")
	opts := Options{
		NodeID:                     testNodeID(nodeSlot),
		Engine:                     scenario.engine,
		PebbleMessageSyncMode:      scenario.pebbleMessageSyncMode,
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	}
	if scenario.engine == EnginePebble {
		opts.PebblePath = filepath.Join(dir, name+".pebble")
	}
	st, err := Open(filepath.Join(dir, name+".db"), opts)
	if err != nil {
		cleanupDir()
		tb.Fatalf("open benchmark store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("init benchmark store: %v", err)
	}
	return st, func() {
		_ = st.Close()
		cleanupDir()
	}
}

func appendBenchmarkUserEvents(tb testing.TB, st *Store, count int) {
	tb.Helper()

	ctx := context.Background()
	for i := 0; i < count; i++ {
		now := st.clock.Now()
		user := User{
			NodeID:              st.NodeID(),
			ID:                  int64(10_000 + i),
			Username:            fmt.Sprintf("bench-event-user-%05d", i),
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleChannel,
			CreatedAt:           now,
			UpdatedAt:           now,
			VersionUsername:     now,
			VersionPasswordHash: now,
			VersionProfile:      now,
			VersionRole:         now,
			OriginNodeID:        st.NodeID(),
		}
		if _, err := st.backend.EventLog().Append(ctx, Event{
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: user.NodeID,
			AggregateID:     user.ID,
			HLC:             now,
			Body:            userCreatedProtoFromUser(user),
		}); err != nil {
			tb.Fatalf("append benchmark event %d: %v", i, err)
		}
	}
}

func runStoreBenchmarkWarmup(tb testing.TB, fn func()) {
	tb.Helper()
	for i := 0; i < storeBenchmarkWarmupPasses; i++ {
		fn()
	}
}

func mustCreateBenchmarkMessage(tb testing.TB, st *Store, ctx context.Context, key UserKey, payload []byte) (Message, Event) {
	tb.Helper()

	message, event, err := st.CreateMessage(ctx, CreateMessageParams{
		UserKey: key,
		Sender:  key,
		Body:    payload,
	})
	if err != nil {
		tb.Fatalf("create message: %v", err)
	}
	if message.Recipient != key || message.NodeID != st.NodeID() || message.Seq <= 0 {
		tb.Fatalf("unexpected created message: %+v", message)
	}
	if !bytes.Equal(message.Body, payload) || event.EventID <= 0 {
		tb.Fatalf("unexpected create message payload/event: message=%+v event=%+v", message, event)
	}
	return message, event
}

func mustListBenchmarkMessages(tb testing.TB, st *Store, ctx context.Context, key UserKey, limit int, wantFirst, wantLast []byte) {
	tb.Helper()

	messages, err := st.ListMessagesByUser(ctx, key, limit)
	if err != nil {
		tb.Fatalf("list messages: %v", err)
	}
	if len(messages) != limit {
		tb.Fatalf("unexpected message count: got=%d want=%d", len(messages), limit)
	}
	if !bytes.Equal(messages[0].Body, wantFirst) || !bytes.Equal(messages[len(messages)-1].Body, wantLast) {
		tb.Fatalf("unexpected message order: first=%q last=%q", messages[0].Body, messages[len(messages)-1].Body)
	}
}

func assertBenchmarkMessageSequenceCounter(tb testing.TB, st *Store, ctx context.Context, engine string, key UserKey, wantAtLeast int64) {
	tb.Helper()
	if engine != EngineSQLite {
		return
	}

	var nextSeq int64
	if err := st.db.QueryRowContext(ctx, `
SELECT next_seq
FROM message_sequence_counters
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`, key.NodeID, key.UserID, st.NodeID()).Scan(&nextSeq); err != nil {
		tb.Fatalf("read benchmark message sequence counter: %v", err)
	}
	if nextSeq < wantAtLeast {
		tb.Fatalf("unexpected benchmark message sequence counter: got=%d want-at-least=%d", nextSeq, wantAtLeast)
	}
}

func mustVerifyBenchmarkPruneResult(tb testing.TB, st *Store, ctx context.Context, result EventLogPruneResult, retain, eventCount int) {
	tb.Helper()

	wantTrimmed := int64(eventCount - retain)
	if result.TrimmedEvents != wantTrimmed || result.OriginsAffected != 1 || result.MaxEventsPerOrigin != retain {
		tb.Fatalf("unexpected prune result: %+v", result)
	}
	retained, err := st.ListEventsByOrigin(ctx, st.NodeID(), 0, retain+1)
	if err != nil {
		tb.Fatalf("list retained events: %v", err)
	}
	if len(retained) != retain {
		tb.Fatalf("unexpected retained event count: got=%d want=%d", len(retained), retain)
	}
	truncatedBefore, err := st.EventLogTruncatedBefore(ctx, st.NodeID())
	if err != nil {
		tb.Fatalf("read truncated boundary: %v", err)
	}
	if truncatedBefore <= 0 {
		tb.Fatalf("expected non-zero truncation boundary")
	}
}
