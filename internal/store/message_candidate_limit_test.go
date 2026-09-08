package store

import (
	"context"
	"fmt"
	"testing"
)

// 对比完整窗口查询，确保候选下推不改变跨来源合并或黑名单过滤结果。
func TestSQLiteMessageCandidateLimit(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureBootstrapAdmin(ctx, BootstrapAdminConfig{Username: "root", PasswordHash: "hash-root"}); err != nil {
		t.Fatal(err)
	}
	createUser := func(name, role string) User {
		t.Helper()
		user, _, err := st.CreateUser(ctx, CreateUserParams{Username: name, PasswordHash: "hash", Role: role})
		if err != nil {
			t.Fatal(err)
		}
		return user
	}
	owner := createUser("owner", RoleUser)
	sender := createUser("sender", RoleUser)
	broadcast, err := st.GetUser(ctx, UserKey{NodeID: st.NodeID(), UserID: BroadcastUserID})
	if err != nil {
		t.Fatal(err)
	}
	channel := createUser("channel", RoleChannel)
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{Subscriber: owner.Key(), Channel: channel.Key()}); err != nil {
		t.Fatal(err)
	}
	seed := func(recipient UserKey, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, _, err := st.CreateMessage(ctx, CreateMessageParams{UserKey: recipient, Sender: sender.Key(), Body: []byte(fmt.Sprintf("message-%d", i))}); err != nil {
				t.Fatal(err)
			}
		}
	}
	seed(owner.Key(), 20)
	seed(broadcast.Key(), 20)
	seed(channel.Key(), 20)
	check := func(t *testing.T) {
		t.Helper()
		all, err := st.ListMessagesByUser(ctx, owner.Key(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{1, 7, 50, 100} {
			got, err := st.ListMessagesByUser(ctx, owner.Key(), limit)
			if err != nil {
				t.Fatal(err)
			}
			want := min(limit, len(all))
			if len(got) != want {
				t.Fatalf("limit %d: got %d messages, want %d", limit, len(got), want)
			}
			for i := range got {
				if messageIdentity(got[i]) != messageIdentity(all[i]) {
					t.Fatalf("limit %d: message %d differs", limit, i)
				}
			}
		}
	}
	t.Run("merged", check)
	entry, _, err := st.BlockUser(ctx, BlacklistParams{Owner: owner.Key(), Blocked: sender.Key()})
	if err != nil {
		t.Fatal(err)
	}
	// 复制可能带来拉黑之后的消息；直接投影以覆盖读取时过滤及补足旧消息的路径。
	projection := st.backend.MessageProjection()
	for i := int64(21); i <= 40; i++ {
		err := projection.ApplyMessageCreated(ctx, Message{Recipient: owner.Key(), Sender: sender.Key(), NodeID: st.NodeID(), Seq: i, Body: []byte("blocked"), CreatedAt: entry.BlockedAt})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Run("blacklist-refill", check)
}
