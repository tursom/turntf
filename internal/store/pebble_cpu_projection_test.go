package store

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/tursom/turntf/internal/clock"
)

func TestPebbleCPUReadScenariosMatchLegacy(t *testing.T) {
	for _, profile := range []PebbleProfile{PebbleProfileBalanced, PebbleProfileThroughput} {
		t.Run(string(profile), func(t *testing.T) {
			for _, scenario := range []string{"inbox-full", "strong-filter", "incomplete-fallback", "broadcast-channel", "local-messages"} {
				t.Run(scenario, func(t *testing.T) {
					st := openPebbleTestStoreWithSettings(t, "cpu-read", 1, 256, DefaultEventLogMaxEventsPerOrigin, PebbleMessageSyncModeNoSync, profile)
					owner := pebbleCPUSeedReadScenario(t, st, scenario, 256)
					for _, limit := range []int{1, 20, 1000} {
						want := pebbleCPULegacyMessages(t, st, owner, limit)
						got, err := st.ListMessagesByUser(context.Background(), owner, limit)
						pebbleCPURequireMessages(t, got, err, want)
					}
				})
			}
		})
	}
}

func TestPebbleCPUInboxStopsMaterializingAfterLimit(t *testing.T) {
	for _, profile := range []PebbleProfile{PebbleProfileBalanced, PebbleProfileThroughput} {
		t.Run(string(profile), func(t *testing.T) {
			st := openPebbleTestStoreWithSettings(t, "cpu-stop", 1, 64, DefaultEventLogMaxEventsPerOrigin, PebbleMessageSyncModeNoSync, profile)
			owner := pebbleCPUSeedReadScenario(t, st, "inbox-full", 64)
			r := requirePebbleBackend(t, st).messageProjectionRepo
			want := pebbleCPULegacyMessages(t, st, owner, 2)
			inbox, err := r.listInboxMessagesByUser(context.Background(), owner, 64)
			if err != nil {
				t.Fatal(err)
			}
			// 第三条是不能解码的引用。只需前两条时不应物化第三条。
			if err := r.db.Set(pebbleInboxUserKey(owner, inbox[2]), []byte{pebbleMessageIndexRefMarker}, pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			got, err := st.ListMessagesByUser(context.Background(), owner, 2)
			pebbleCPURequireMessages(t, got, err, want)
			if _, err := st.ListMessagesByUser(context.Background(), owner, 3); err == nil {
				t.Fatal("expected corrupt inbox entry to fail when it is needed")
			}
		})
	}
}

func TestPebbleCPUInboxScanBoundCountsFilteredEntries(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStore(t, "cpu-scan-bound", 1, 4)
	r := requirePebbleBackend(t, st).messageProjectionRepo
	if err := r.close(); err != nil {
		t.Fatal(err)
	}
	owner := pebbleCPUCreateUser(t, st, "owner", RoleUser).Key()
	sender := pebbleCPUCreateUser(t, st, "sender", RoleUser).Key()
	if _, _, err := st.BlockUser(ctx, BlacklistParams{Owner: owner, Blocked: sender}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		from := sender
		if i == 0 {
			from = owner
		}
		if err := r.ApplyMessageCreated(ctx, Message{Recipient: owner, Sender: from, NodeID: testNodeID(9), Seq: int64(i + 1), CreatedAt: st.Clock().Now()}); err != nil {
			t.Fatal(err)
		}
	}
	// 最新 window 条全部被过滤，不能扫描到第五条来凑数；legacy 的直接消息窗口也只有四条。
	got, err := st.ListMessagesByUser(ctx, owner, 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("scanned beyond original window: messages=%+v err=%v", got, err)
	}
	// limit 大于 window 时仍须使用 max(limit, window)，而不是硬限为 window。
	if _, _, err := st.UnblockUser(ctx, BlacklistParams{Owner: owner, Blocked: sender}); err != nil {
		t.Fatal(err)
	}
	got, err = st.ListMessagesByUser(ctx, owner, 5)
	if err != nil || len(got) != 5 {
		t.Fatalf("limit above window: count=%d err=%v", len(got), err)
	}
}

type pebbleCPUCountBlacklists struct {
	BlacklistRepository
	calls          int
	extra          []BlacklistEntry
	afterFirstRead func()
}

func (r *pebbleCPUCountBlacklists) ListActiveBlockedUsers(ctx context.Context, key UserKey) ([]BlacklistEntry, error) {
	r.calls++
	entries, err := r.BlacklistRepository.ListActiveBlockedUsers(ctx, key)
	if r.calls == 1 && r.afterFirstRead != nil {
		r.afterFirstRead()
	}
	return append(entries, r.extra...), err
}

type pebbleCPUCountSubscriptions struct {
	SubscriptionRepository
	calls          int
	afterFirstRead func()
}

func (r *pebbleCPUCountSubscriptions) ListActiveSubscriptions(ctx context.Context, key UserKey) ([]Subscription, error) {
	r.calls++
	entries, err := r.SubscriptionRepository.ListActiveSubscriptions(ctx, key)
	if r.calls == 1 && r.afterFirstRead != nil {
		r.afterFirstRead()
	}
	return entries, err
}

type pebbleCPUCountUsers struct {
	UserRepository
	calls map[UserKey]int
}

func (r *pebbleCPUCountUsers) GetUser(ctx context.Context, key UserKey, deleted bool) (User, error) {
	r.calls[key]++
	return r.UserRepository.GetUser(ctx, key, deleted)
}

func TestPebbleCPUFallbackRefreshesVisibility(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStore(t, "cpu-visibility", 1, 64)
	r := requirePebbleBackend(t, st).messageProjectionRepo
	owner := pebbleCPUCreateUser(t, st, "owner", RoleUser).Key()
	sender := pebbleCPUCreateUser(t, st, "sender", RoleUser).Key()
	admin := pebbleCPUCreateUser(t, st, "admin", RoleAdmin).Key()
	unknown := testSenderKey(9, 999)
	entry, _, err := st.BlockUser(ctx, BlacklistParams{Owner: owner, Blocked: sender})
	if err != nil {
		t.Fatal(err)
	}
	blacklists := &pebbleCPUCountBlacklists{BlacklistRepository: r.blacklists, extra: []BlacklistEntry{
		{Owner: owner, Blocked: admin, BlockedAt: entry.BlockedAt},
		{Owner: owner, Blocked: unknown, BlockedAt: entry.BlockedAt},
	}}
	subscriptions := &pebbleCPUCountSubscriptions{SubscriptionRepository: r.subscriptions}
	users := &pebbleCPUCountUsers{UserRepository: r.userRepository, calls: make(map[UserKey]int)}
	r.blacklists, r.subscriptions, r.userRepository = blacklists, subscriptions, users
	before := entry.BlockedAt
	before.WallTimeMs--
	after := entry.BlockedAt
	after.WallTimeMs++
	seeds := []Message{
		{Sender: sender, CreatedAt: before},          // 拉黑前可见。
		{Sender: sender, CreatedAt: entry.BlockedAt}, // 等于拉黑时间也隐藏。
		{Sender: sender, CreatedAt: after},
		{Sender: admin, CreatedAt: after}, // 非普通用户和未知发送者保持原豁免。
		{Sender: unknown, CreatedAt: after},
	}
	for i := range seeds {
		seeds[i].Recipient, seeds[i].NodeID, seeds[i].Seq = owner, testNodeID(9), int64(i+1)
		if err := r.ApplyMessageCreated(ctx, seeds[i]); err != nil {
			t.Fatal(err)
		}
	}
	// 五条 inbox 不足 limit，且被过滤发送者同时出现在 inbox 与 legacy。
	blacklists.calls, subscriptions.calls = 0, 0
	users.calls = make(map[UserKey]int)
	got, err := st.ListMessagesByUser(ctx, owner, 20)
	want := []Message{seeds[4], seeds[3], seeds[0]}
	sortMessages(want)
	pebbleCPURequireMessages(t, got, err, want)
	if blacklists.calls != 2 || subscriptions.calls != 2 || users.calls[sender] != 2 || users.calls[admin] != 2 || users.calls[unknown] != 2 {
		t.Fatalf("fallback did not refresh metadata: blacklist=%d subscriptions=%d users=%v", blacklists.calls, subscriptions.calls, users.calls)
	}
	if _, _, err := st.UnblockUser(ctx, BlacklistParams{Owner: owner, Blocked: sender}); err != nil {
		t.Fatal(err)
	}
	got, err = st.ListMessagesByUser(ctx, owner, 20)
	want = append([]Message(nil), seeds...)
	sortMessages(want)
	pebbleCPURequireMessages(t, got, err, want)
	if blacklists.calls != 4 || subscriptions.calls != 4 {
		t.Fatalf("visibility was cached across queries: blacklist=%d subscriptions=%d", blacklists.calls, subscriptions.calls)
	}
}

func TestPebbleCPUInboxViewPrecedesVisibilityRead(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStore(t, "cpu-view-order", 1, 64)
	r := requirePebbleBackend(t, st).messageProjectionRepo
	owner := pebbleCPUCreateUser(t, st, "owner", RoleUser).Key()
	sender := pebbleCPUCreateUser(t, st, "sender", RoleUser).Key()
	old := Message{Recipient: owner, Sender: owner, NodeID: testNodeID(9), Seq: 1, Body: []byte("visible"), CreatedAt: st.Clock().Now()}
	if err := r.ApplyMessageCreated(ctx, old); err != nil {
		t.Fatal(err)
	}
	// 精确模拟读取旧黑名单后，另一请求完成拉黑并投影新消息。
	// iterator 必须已固定消息视图，不能把新消息和旧权限组合返回。
	r.blacklists = &pebbleCPUCountBlacklists{BlacklistRepository: r.blacklists, afterFirstRead: func() {
		if _, _, err := st.BlockUser(ctx, BlacklistParams{Owner: owner, Blocked: sender}); err != nil {
			t.Fatal(err)
		}
		message := Message{Recipient: owner, Sender: sender, NodeID: testNodeID(9), Seq: 2, Body: []byte("hidden"), CreatedAt: st.Clock().Now()}
		if err := r.ApplyMessageCreated(ctx, message); err != nil {
			t.Fatal(err)
		}
	}}
	got, err := st.ListMessagesByUser(ctx, owner, 1)
	pebbleCPURequireMessages(t, got, err, []Message{old})
}

func TestPebbleCPUFallbackRefreshesSubscriptionAfterUnsubscribe(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStore(t, "cpu-fallback-order", 1, 64)
	r := requirePebbleBackend(t, st).messageProjectionRepo
	owner := pebbleCPUCreateUser(t, st, "owner", RoleUser).Key()
	channel := pebbleCPUCreateUser(t, st, "channel", RoleChannel).Key()
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{Subscriber: owner, Channel: channel}); err != nil {
		t.Fatal(err)
	}
	r.subscriptions = &pebbleCPUCountSubscriptions{SubscriptionRepository: r.subscriptions, afterFirstRead: func() {
		if _, _, err := st.UnsubscribeChannel(ctx, ChannelSubscriptionParams{Subscriber: owner, Channel: channel}); err != nil {
			t.Fatal(err)
		}
		if err := r.ApplyMessageCreated(ctx, Message{Recipient: channel, Sender: channel, NodeID: testNodeID(9), Seq: 1, Body: []byte("after unsubscribe"), CreatedAt: st.Clock().Now()}); err != nil {
			t.Fatal(err)
		}
	}}
	got, err := st.ListMessagesByUser(ctx, owner, 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("fallback returned a message after unsubscribe: %+v err=%v", got, err)
	}
}

func TestPebbleCPUSubscriptionBoundaryAndUnsubscribe(t *testing.T) {
	ctx := context.Background()
	st := openPebbleTestStore(t, "cpu-subscription", 1, 64)
	r := requirePebbleBackend(t, st).messageProjectionRepo
	owner := pebbleCPUCreateUser(t, st, "owner", RoleUser).Key()
	channel := pebbleCPUCreateUser(t, st, "channel", RoleChannel).Key()
	subscription, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{Subscriber: owner, Channel: channel})
	if err != nil {
		t.Fatal(err)
	}
	before := subscription.SubscribedAt
	before.WallTimeMs--
	after := subscription.SubscribedAt
	after.WallTimeMs++
	for i, timestamp := range []clock.Timestamp{before, subscription.SubscribedAt, after} {
		message := Message{Recipient: channel, Sender: owner, NodeID: testNodeID(9), Seq: int64(i + 1), CreatedAt: timestamp}
		if err := r.ApplyMessageCreated(ctx, message); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			// 模拟旧订阅留下的 inbox 项；当前订阅时间仍须过滤它。
			batch := r.db.NewBatch()
			if err := r.writeInboxEntry(batch, owner, message, true, nil); err != nil {
				t.Fatal(err)
			}
			if err := applyPebbleBatch(batch, nil, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, limit := range []int{1, 2, 20} {
		want := pebbleCPULegacyMessages(t, st, owner, limit)
		got, err := st.ListMessagesByUser(ctx, owner, limit)
		pebbleCPURequireMessages(t, got, err, want)
	}
	if _, _, err := st.UnsubscribeChannel(ctx, ChannelSubscriptionParams{Subscriber: owner, Channel: channel}); err != nil {
		t.Fatal(err)
	}
	got, err := st.ListMessagesByUser(ctx, owner, 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("unsubscribed channel remains visible: %+v err=%v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.ListMessagesByUser(canceled, owner, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query: %v", err)
	}
}
