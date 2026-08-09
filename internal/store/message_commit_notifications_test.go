package store

import (
	"context"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func TestMessageCommitNotifications(t *testing.T) {
	for _, testCase := range []struct {
		name string
		open func(*testing.T) *Store
	}{
		{
			name: EngineSQLite,
			open: func(t *testing.T) *Store {
				st := openNamedTestStore(t, "message-notify-sqlite", 1)
				t.Cleanup(func() {
					if err := st.Close(); err != nil {
						t.Fatalf("close sqlite store: %v", err)
					}
				})
				return st
			},
		},
		{
			name: EnginePebble,
			open: func(t *testing.T) *Store {
				return openPebbleTestStore(t, "message-notify-pebble", 1, DefaultMessageWindowSize)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			st := testCase.open(t)
			ctx := context.Background()
			user, _, err := st.CreateUser(ctx, CreateUserParams{
				Username:     "message-notify-user",
				PasswordHash: "!",
				Role:         RoleUser,
			})
			if err != nil {
				t.Fatalf("create notification user: %v", err)
			}

			notifications, unsubscribe := st.SubscribeMessageCommits()

			if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
				UserKey: UserKey{},
				Sender:  user.Key(),
				Body:    []byte("invalid"),
			}); err == nil {
				t.Fatal("expected invalid message write to fail")
			}
			assertNoMessageCommitNotification(t, notifications)

			for _, body := range []string{"local-1", "local-2", "local-3"} {
				if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
					UserKey: user.Key(),
					Sender:  user.Key(),
					Body:    []byte(body),
				}); err != nil {
					t.Fatalf("create local message %q: %v", body, err)
				}
			}
			assertMessageCommitNotification(t, notifications)
			assertNoMessageCommitNotification(t, notifications)

			now := st.Clock().Now()
			replicated := ToReplicatedEvent(Event{
				EventID:         1,
				Aggregate:       "message",
				AggregateNodeID: testNodeID(2),
				AggregateID:     1,
				HLC:             now,
				OriginNodeID:    testNodeID(2),
				Body: &internalproto.MessageCreatedEvent{
					Recipient:    &internalproto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
					NodeId:       testNodeID(2),
					Seq:          1,
					Sender:       &internalproto.ClusterUserRef{NodeId: user.NodeID, UserId: user.ID},
					Body:         []byte("replicated"),
					CreatedAtHlc: now.String(),
				},
			})
			if err := st.ApplyReplicatedEvent(ctx, replicated); err != nil {
				t.Fatalf("apply replicated message: %v", err)
			}
			assertMessageCommitNotification(t, notifications)

			if err := st.ApplyReplicatedEvent(ctx, replicated); err != nil {
				t.Fatalf("apply duplicate replicated message: %v", err)
			}
			assertNoMessageCommitNotification(t, notifications)

			unsubscribe()
			if _, _, err := st.CreateMessage(ctx, CreateMessageParams{
				UserKey: user.Key(),
				Sender:  user.Key(),
				Body:    []byte("after-unsubscribe"),
			}); err != nil {
				t.Fatalf("create message after unsubscribe: %v", err)
			}
			assertNoMessageCommitNotification(t, notifications)
		})
	}
}

func assertMessageCommitNotification(t *testing.T, notifications <-chan struct{}) {
	t.Helper()
	select {
	case <-notifications:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message commit notification")
	}
}

func assertNoMessageCommitNotification(t *testing.T, notifications <-chan struct{}) {
	t.Helper()
	select {
	case <-notifications:
		t.Fatal("received unexpected message commit notification")
	case <-time.After(25 * time.Millisecond):
	}
}
