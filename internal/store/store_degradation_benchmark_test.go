package store

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkDegradationStoreListMessagesByUser(b *testing.B) {
	for _, engine := range []string{EngineSQLite, EnginePebble} {
		b.Run(engine, func(b *testing.B) {
			benchmarkStoreListMessagesByUserDegradation(b, engine, []int{64, 256, 1000})
		})
	}
}

func BenchmarkDegradationStorePruneEventLogOnce(b *testing.B) {
	for _, engine := range []string{EngineSQLite, EnginePebble} {
		b.Run(engine, func(b *testing.B) {
			benchmarkStorePruneEventLogOnceDegradation(b, engine, 128, []int{256, 1024, 4096})
		})
	}
}

type listMessagesDegradationProfile struct {
	history   int
	st        *Store
	close     func()
	userKey   UserKey
	wantFirst []byte
	wantLast  []byte
}

func benchmarkStoreListMessagesByUserDegradation(b *testing.B, engine string, histories []int) {
	b.Helper()

	ctx := context.Background()
	profiles := make([]listMessagesDegradationProfile, 0, len(histories))
	for _, history := range histories {
		st, closeStore := openBenchmarkStore(b, engine, fmt.Sprintf("list-messages-degradation-%d", history), 1, history, DefaultEventLogMaxEventsPerOrigin)
		user, _, err := st.CreateUser(ctx, CreateUserParams{
			Username:     fmt.Sprintf("bench-list-messages-degradation-%d", history),
			PasswordHash: "bench-hash",
			Role:         RoleChannel,
		})
		if err != nil {
			closeStore()
			b.Fatalf("create benchmark user for history %d: %v", history, err)
		}

		var wantFirst []byte
		var wantLast []byte
		for i := 0; i < history; i++ {
			body := []byte(fmt.Sprintf("degradation-body-%04d", i))
			if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
				UserKey: user.Key(),
				Sender:  user.Key(),
				Body:    body,
			}); err != nil {
				closeStore()
				b.Fatalf("seed message %d for history %d: %v", i, history, err)
			}
			if i == 0 {
				wantLast = append([]byte(nil), body...)
			}
			wantFirst = append([]byte(nil), body...)
		}

		profiles = append(profiles, listMessagesDegradationProfile{
			history:   history,
			st:        st,
			close:     closeStore,
			userKey:   user.Key(),
			wantFirst: wantFirst,
			wantLast:  wantLast,
		})
	}
	for _, profile := range profiles {
		b.Cleanup(profile.close)
	}

	totals := make([]time.Duration, len(profiles))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for idx, profile := range profiles {
			start := time.Now()
			messages, err := profile.st.ListMessagesByUser(ctx, profile.userKey, profile.history)
			totals[idx] += time.Since(start)
			if err != nil {
				b.Fatalf("list messages for history %d: %v", profile.history, err)
			}
			if len(messages) != profile.history {
				b.Fatalf("unexpected message count for history %d: got=%d want=%d", profile.history, len(messages), profile.history)
			}
			if !bytes.Equal(messages[0].Body, profile.wantFirst) || !bytes.Equal(messages[len(messages)-1].Body, profile.wantLast) {
				b.Fatalf("unexpected message order for history %d: first=%q last=%q", profile.history, messages[0].Body, messages[len(messages)-1].Body)
			}
		}
	}
	b.StopTimer()

	reportDegradationMetrics(b, "history", histories, totals)
}

func benchmarkStorePruneEventLogOnceDegradation(b *testing.B, engine string, retain int, eventCounts []int) {
	b.Helper()

	ctx := context.Background()
	totals := make([]time.Duration, len(eventCounts))

	for i := 0; i < b.N; i++ {
		for idx, eventCount := range eventCounts {
			b.StopTimer()
			st, closeStore := openBenchmarkStore(b, engine, fmt.Sprintf("prune-event-log-degradation-%d", eventCount), 1, DefaultMessageWindowSize, retain)
			appendBenchmarkUserEvents(b, st, eventCount)

			b.StartTimer()
			start := time.Now()
			result, err := st.PruneEventLogOnce(ctx)
			totals[idx] += time.Since(start)
			b.StopTimer()

			if err != nil {
				closeStore()
				b.Fatalf("prune event log for event count %d: %v", eventCount, err)
			}

			wantTrimmed := int64(eventCount - retain)
			if result.TrimmedEvents != wantTrimmed || result.OriginsAffected != 1 || result.MaxEventsPerOrigin != retain {
				closeStore()
				b.Fatalf("unexpected prune result for event count %d: %+v", eventCount, result)
			}
			retained, err := st.ListEventsByOrigin(ctx, st.NodeID(), 0, retain+1)
			if err != nil {
				closeStore()
				b.Fatalf("list retained events for event count %d: %v", eventCount, err)
			}
			if len(retained) != retain {
				closeStore()
				b.Fatalf("unexpected retained event count for event count %d: got=%d want=%d", eventCount, len(retained), retain)
			}

			closeStore()
		}
	}

	reportDegradationMetrics(b, "events", eventCounts, totals)
}

func reportDegradationMetrics(b *testing.B, dimension string, sizes []int, totals []time.Duration) {
	b.Helper()

	if len(sizes) == 0 || len(sizes) != len(totals) || b.N <= 0 {
		return
	}

	baseSize := sizes[0]
	baseNsPerOp := float64(totals[0].Nanoseconds()) / float64(b.N)
	baseLabel := degradationMetricLabel(baseSize)
	b.ReportMetric(baseNsPerOp, fmt.Sprintf("%s_%s_ns/op", dimension, baseLabel))

	for idx := 1; idx < len(sizes); idx++ {
		size := sizes[idx]
		label := degradationMetricLabel(size)
		nsPerOp := float64(totals[idx].Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, fmt.Sprintf("%s_%s_ns/op", dimension, label))
		b.ReportMetric(nsPerOp/baseNsPerOp, fmt.Sprintf("%s_%s_vs_%s_x", dimension, label, baseLabel))

		delta := size - baseSize
		if delta > 0 {
			deltaNsPer1K := (nsPerOp - baseNsPerOp) * 1000 / float64(delta)
			b.ReportMetric(deltaNsPer1K, fmt.Sprintf("%s_%s_vs_%s_delta_ns_per_1k", dimension, label, baseLabel))
		}
	}
}

func degradationMetricLabel(size int) string {
	return fmt.Sprintf("%05d", size)
}
