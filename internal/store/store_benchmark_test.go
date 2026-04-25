package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkStoreCreateMessage(b *testing.B) {
	for _, engine := range []string{EngineSQLite, EnginePebble} {
		for _, payloadSize := range []int{256, 4 << 10} {
			b.Run(fmt.Sprintf("%s/%dB", engine, payloadSize), func(b *testing.B) {
				benchmarkStoreCreateMessage(b, engine, payloadSize)
			})
		}
	}
}

func BenchmarkStoreListMessagesByUser(b *testing.B) {
	for _, engine := range []string{EngineSQLite, EnginePebble} {
		for _, history := range []int{100, 1000} {
			b.Run(fmt.Sprintf("%s/history-%d", engine, history), func(b *testing.B) {
				benchmarkStoreListMessagesByUser(b, engine, history)
			})
		}
	}
}

func BenchmarkStorePruneEventLogOnce(b *testing.B) {
	for _, engine := range []string{EngineSQLite, EnginePebble} {
		for _, tc := range []struct {
			name       string
			retain     int
			eventCount int
		}{
			{name: "retain-128/events-256", retain: 128, eventCount: 256},
			{name: "retain-128/events-4096", retain: 128, eventCount: 4096},
		} {
			b.Run(fmt.Sprintf("%s/%s", engine, tc.name), func(b *testing.B) {
				benchmarkStorePruneEventLogOnce(b, engine, tc.retain, tc.eventCount)
			})
		}
	}
}

func benchmarkStoreCreateMessage(b *testing.B, engine string, payloadSize int) {
	ctx := context.Background()
	st, closeStore := openBenchmarkStore(b, engine, "create-message", 1, DefaultMessageWindowSize, DefaultEventLogMaxEventsPerOrigin)
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
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		message, event, err := st.CreateMessage(ctx, CreateMessageParams{
			UserKey: user.Key(),
			Sender:  user.Key(),
			Body:    payload,
		})
		if err != nil {
			b.Fatalf("create message: %v", err)
		}
		if message.Recipient != user.Key() || message.NodeID != st.NodeID() || message.Seq <= 0 {
			b.Fatalf("unexpected created message: %+v", message)
		}
		if !bytes.Equal(message.Body, payload) || event.EventID <= 0 {
			b.Fatalf("unexpected create message payload/event: message=%+v event=%+v", message, event)
		}
	}
}

func benchmarkStoreListMessagesByUser(b *testing.B, engine string, history int) {
	const limit = 50

	ctx := context.Background()
	st, closeStore := openBenchmarkStore(b, engine, "list-messages", 1, DefaultMessageWindowSize, DefaultEventLogMaxEventsPerOrigin)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messages, err := st.ListMessagesByUser(ctx, user.Key(), limit)
		if err != nil {
			b.Fatalf("list messages: %v", err)
		}
		if len(messages) != limit {
			b.Fatalf("unexpected message count: got=%d want=%d", len(messages), limit)
		}
		if !bytes.Equal(messages[0].Body, wantFirst) || !bytes.Equal(messages[len(messages)-1].Body, wantLast) {
			b.Fatalf("unexpected message order: first=%q last=%q", messages[0].Body, messages[len(messages)-1].Body)
		}
	}
}

func benchmarkStorePruneEventLogOnce(b *testing.B, engine string, retain, eventCount int) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		st, closeStore := openBenchmarkStore(b, engine, "prune-event-log", 1, DefaultMessageWindowSize, retain)
		appendBenchmarkUserEvents(b, st, eventCount)

		b.StartTimer()
		result, err := st.PruneEventLogOnce(ctx)
		b.StopTimer()
		if err != nil {
			closeStore()
			b.Fatalf("prune event log: %v", err)
		}

		wantTrimmed := int64(eventCount - retain)
		if result.TrimmedEvents != wantTrimmed || result.OriginsAffected != 1 || result.MaxEventsPerOrigin != retain {
			closeStore()
			b.Fatalf("unexpected prune result: %+v", result)
		}
		retained, err := st.ListEventsByOrigin(ctx, st.NodeID(), 0, retain+1)
		if err != nil {
			closeStore()
			b.Fatalf("list retained events: %v", err)
		}
		if len(retained) != retain {
			closeStore()
			b.Fatalf("unexpected retained event count: got=%d want=%d", len(retained), retain)
		}
		truncatedBefore, err := st.EventLogTruncatedBefore(ctx, st.NodeID())
		if err != nil {
			closeStore()
			b.Fatalf("read truncated boundary: %v", err)
		}
		if truncatedBefore <= 0 {
			closeStore()
			b.Fatalf("expected non-zero truncation boundary")
		}
		closeStore()
	}
}

func openBenchmarkStore(tb testing.TB, engine, name string, nodeSlot uint16, messageWindowSize int, maxEventsPerOrigin int) (*Store, func()) {
	tb.Helper()

	dir, err := os.MkdirTemp("", "turntf-store-bench-*")
	if err != nil {
		tb.Fatalf("create benchmark temp dir: %v", err)
	}
	opts := Options{
		NodeID:                     testNodeID(nodeSlot),
		Engine:                     engine,
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	}
	if engine == EnginePebble {
		opts.PebblePath = filepath.Join(dir, name+".pebble")
	}
	st, err := Open(filepath.Join(dir, name+".db"), opts)
	if err != nil {
		tb.Fatalf("open benchmark store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		tb.Fatalf("init benchmark store: %v", err)
	}
	return st, func() {
		_ = st.Close()
		_ = os.RemoveAll(dir)
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
		if _, err := st.eventLog.Append(ctx, Event{
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
