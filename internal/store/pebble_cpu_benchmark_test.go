package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/tursom/turntf/internal/testutil/benchroot"
)

// 本文件只使用 6b9a81e 已有的接口及测试夹具，可单独 overlay 到该基线。
// 读取候选只需 overlay pebble_projection.go；批写候选需同时 overlay
// pebble_message_state.go 和 pebble_local_message_writer.go。禁止混跑两组测量。
// 所有建库、播种及批写清理均在计时外；完整本地批次包含实际 Commit 和响应读取。
func BenchmarkPebbleCPUListMessages(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		b.Run(mode.Name(), func(b *testing.B) {
			for _, profile := range []PebbleProfile{PebbleProfileBalanced, PebbleProfileThroughput} {
				b.Run(string(profile), func(b *testing.B) {
					for _, scenario := range []string{"inbox-full", "strong-filter", "incomplete-fallback", "broadcast-channel", "local-messages"} {
						b.Run(scenario, func(b *testing.B) {
							st, cleanup := openBenchmarkStore(b, mode, storeBenchmarkEngineScenario{
								engine: EnginePebble, pebbleProfile: profile, pebbleMessageSyncMode: PebbleMessageSyncModeNoSync,
							}, "cpu-list", 1, 256, DefaultEventLogMaxEventsPerOrigin)
							defer cleanup()
							owner := pebbleCPUSeedReadScenario(b, st, scenario, 256)
							ctx := context.Background()
							want := pebbleCPULegacyMessages(b, st, owner, 20)
							messages, err := st.ListMessagesByUser(ctx, owner, 20)
							pebbleCPURequireMessages(b, messages, err, want)
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								messages, err := st.ListMessagesByUser(ctx, owner, 20)
								if err != nil || len(messages) != len(want) {
									b.Fatalf("list messages: count=%d err=%v", len(messages), err)
								}
							}
							b.StopTimer()
						})
					}
				})
			}
		})
	}
}

// 每 op 是固定 128 条消息的完整原子批次。每轮计时外删除本轮全部消息、
// 索引和事件，恢复 state/sequence 元数据及内存游标，避免历史增长或 trim 干扰。
// 保留真实 WAL/Commit 成本；这不是纯编码微基准，亦不代表后台 trim 的稳态吞吐。
func BenchmarkPebbleCPULocalMessageBatch(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		b.Run(mode.Name(), func(b *testing.B) {
			for _, profile := range []PebbleProfile{PebbleProfileBalanced, PebbleProfileThroughput} {
				b.Run(string(profile), func(b *testing.B) {
					for _, userCount := range []int{1, pebbleLocalMessageBatchMaxOps} {
						name := "hotspot"
						if userCount > 1 {
							name = "uniform"
						}
						b.Run(name, func(b *testing.B) {
							st, cleanup := openBenchmarkStore(b, mode, storeBenchmarkEngineScenario{
								engine: EnginePebble, pebbleProfile: profile, pebbleMessageSyncMode: PebbleMessageSyncModeNoSync,
							}, "cpu-batch", 1, 256, DefaultEventLogMaxEventsPerOrigin)
							defer cleanup()
							backend := requirePebbleBackend(b, st)
							if err := backend.messageProjectionRepo.close(); err != nil {
								b.Fatal(err)
							}
							users := make([]UserKey, userCount)
							for i := range users {
								users[i] = pebbleCPUCreateUser(b, st, fmt.Sprintf("batch-%d", i), RoleUser).Key()
							}
							requests := pebbleCPURequests(users, pebbleLocalMessageBatchMaxOps, PebbleMessageSyncModeNoSync)
							baseSequence, err := st.LastEventSequence(context.Background())
							if err != nil {
								b.Fatal(err)
							}
							results := make([]pebbleLocalMessageWriteResult, len(requests))
							reset := func() {
								batch := backend.db.NewBatch()
								defer batch.Close()
								for _, result := range results {
									if result.message.Seq == 0 {
										continue
									}
									for _, key := range pebbleMessageKeys(result.message) {
										if err := batch.Delete(key, nil); err != nil {
											b.Fatal(err)
										}
									}
									for _, key := range [][]byte{pebbleEventSeqKey(result.event.Sequence), pebbleEventOriginKey(result.event.OriginNodeID, result.event.EventID)} {
										if err := batch.Delete(key, nil); err != nil {
											b.Fatal(err)
										}
									}
								}
								next := make(map[string]int64, len(users))
								for _, key := range users {
									if err := batch.Set(pebbleMessageUserStateKey(key), encodePebbleMessageUserState(pebbleMessageUserState{}), nil); err != nil {
										b.Fatal(err)
									}
									seqKey := pebbleMessageSequenceKey(key, st.NodeID())
									if err := batch.Set(seqKey, encodeInt64(1), nil); err != nil {
										b.Fatal(err)
									}
									next[string(seqKey)] = 1
								}
								if err := batch.Set(pebbleEventSequenceKey, encodeInt64(baseSequence), nil); err != nil {
									b.Fatal(err)
								}
								if err := batch.Commit(pebble.NoSync); err != nil {
									b.Fatal(err)
								}
								backend.eventLog.lastSequence = baseSequence
								backend.messageSequences.StoreCommittedNextByCacheKey(next)
							}
							reset()
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								backend.processLocalMessageBatch(requests)
								for j := range requests {
									results[j] = <-requests[j].response
									if results[j].err != nil {
										b.Fatal(results[j].err)
									}
								}
								b.StopTimer()
								reset()
								b.StartTimer()
							}
							b.StopTimer()
							b.ReportMetric(pebbleLocalMessageBatchMaxOps, "messages/op")
						})
					}
				})
			}
		})
	}
}

func pebbleCPUCreateUser(tb testing.TB, st *Store, name, role string) User {
	tb.Helper()
	user, _, err := st.CreateUser(context.Background(), CreateUserParams{Username: name, PasswordHash: "hash", Role: role})
	if err != nil {
		tb.Fatal(err)
	}
	return user
}

func pebbleCPURequests(keys []UserKey, count int, mode PebbleMessageSyncMode) []pebbleLocalMessageWriteRequest {
	requests := make([]pebbleLocalMessageWriteRequest, count)
	for i := range requests {
		key := keys[i%len(keys)]
		requests[i] = pebbleLocalMessageWriteRequest{
			params:   CreateMessageParams{UserKey: key, Sender: key, Body: make([]byte, 256), PebbleMessageSyncMode: mode},
			response: make(chan pebbleLocalMessageWriteResult, 1),
		}
	}
	return requests
}

func pebbleCPUSeedReadScenario(tb testing.TB, st *Store, scenario string, history int) UserKey {
	tb.Helper()
	ctx := context.Background()
	owner := pebbleCPUCreateUser(tb, st, "owner", RoleUser).Key()
	sender := pebbleCPUCreateUser(tb, st, "sender", RoleUser).Key()
	channel := pebbleCPUCreateUser(tb, st, "channel", RoleChannel).Key()
	projection := requirePebbleBackend(tb, st).messageProjectionRepo
	if _, _, err := st.SubscribeChannel(ctx, ChannelSubscriptionParams{Subscriber: owner, Channel: channel}); err != nil {
		tb.Fatal(err)
	}
	if scenario == "strong-filter" {
		if _, _, err := st.BlockUser(ctx, BlacklistParams{Owner: owner, Blocked: sender}); err != nil {
			tb.Fatal(err)
		}
	}
	for i := 0; i < history; i++ {
		if scenario == "local-messages" {
			if _, _, err := st.CreateMessage(ctx, CreateMessageParams{UserKey: owner, Sender: owner, Body: make([]byte, 256)}); err != nil {
				tb.Fatal(err)
			}
			continue
		}
		message := Message{Recipient: owner, Sender: owner, NodeID: testNodeID(9), Seq: int64(i + 1), Body: make([]byte, 256), CreatedAt: st.Clock().Now()}
		if scenario == "strong-filter" && i >= history/8 {
			message.Sender = sender
		}
		if scenario == "broadcast-channel" && i%2 == 0 {
			message.Recipient = channel
		}
		// 使用复制/快照共有的投影入口填 inbox，本地 CreateMessage 不填 inbox。
		if err := projection.ApplyMessageCreated(ctx, message); err != nil {
			tb.Fatal(err)
		}
		if scenario == "incomplete-fallback" && i < history-7 {
			if err := projection.db.Delete(pebbleInboxUserKey(owner, message), pebble.NoSync); err != nil {
				tb.Fatal(err)
			}
		}
	}
	if scenario == "broadcast-channel" {
		broadcast := pebbleCPUEnsureBroadcast(tb, st)
		for i := 0; i < 16; i++ {
			if err := projection.ApplyMessageCreated(ctx, Message{Recipient: broadcast, Sender: owner, NodeID: testNodeID(9), Seq: int64(i + 1), Body: []byte("broadcast"), CreatedAt: st.Clock().Now()}); err != nil {
				tb.Fatal(err)
			}
		}
	}
	inbox, err := projection.listInboxMessagesByUser(ctx, owner, history+1)
	if err != nil {
		tb.Fatal(err)
	}
	wantInbox := history
	if scenario == "local-messages" {
		wantInbox = 0
	} else if scenario == "incomplete-fallback" {
		wantInbox = 7
	}
	if len(inbox) != wantInbox {
		tb.Fatalf("%s fixture inbox=%d want=%d", scenario, len(inbox), wantInbox)
	}
	return owner
}

func pebbleCPUEnsureBroadcast(tb testing.TB, st *Store) UserKey {
	tb.Helper()
	key := UserKey{NodeID: st.NodeID(), UserID: BroadcastUserID}
	if _, err := st.GetUser(context.Background(), key); err == nil {
		return key
	}
	now := st.Clock().Now()
	user := User{NodeID: key.NodeID, ID: key.UserID, Username: "broadcast", PasswordHash: disabledPasswordHash, Profile: "{}", Role: RoleBroadcast, SystemReserved: true,
		CreatedAt: now, UpdatedAt: now, VersionUsername: now, VersionPasswordHash: now, VersionProfile: now, VersionRole: now, OriginNodeID: st.NodeID()}
	if err := st.ApplyReplicatedEvent(context.Background(), ToReplicatedEvent(Event{EventID: 9001, EventType: EventTypeUserCreated, Aggregate: "user", AggregateNodeID: user.NodeID, AggregateID: user.ID,
		HLC: now, OriginNodeID: testNodeID(9), Body: userCreatedProtoFromUser(user)})); err != nil {
		tb.Fatal(err)
	}
	return key
}

// 独立按旧路径构造期望值，避免用候选的过滤或流式 helper 自证正确。
func pebbleCPULegacyMessages(tb testing.TB, st *Store, owner UserKey, limit int) []Message {
	tb.Helper()
	ctx := context.Background()
	r := requirePebbleBackend(tb, st).messageProjectionRepo
	direct, err := r.listRawMessagesByUser(ctx, owner, 0, nil)
	if err != nil {
		tb.Fatal(err)
	}
	messages, err := filterDirectMessagesByBlacklist(ctx, r.userRepository, r.blacklists, owner, direct)
	if err != nil {
		tb.Fatal(err)
	}
	broadcasts, err := r.listBroadcastMessages(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	messages = append(messages, broadcasts...)
	subscriptions, err := r.subscriptions.ListActiveSubscriptions(ctx, owner)
	if err != nil {
		tb.Fatal(err)
	}
	for _, subscription := range subscriptions {
		items, err := r.listRawMessagesByUser(ctx, subscription.Channel, 0, &subscription.SubscribedAt)
		if err != nil {
			tb.Fatal(err)
		}
		messages = append(messages, items...)
	}
	seen := make(map[messageIdentityKey]bool)
	unique := make([]Message, 0, len(messages))
	for _, message := range messages {
		id := messageIdentity(message)
		if !seen[id] {
			seen[id] = true
			unique = append(unique, message)
		}
	}
	sortMessages(unique)
	if len(unique) > limit {
		unique = unique[:limit]
	}
	return unique
}

func pebbleCPURequireMessages(tb testing.TB, got []Message, err error, want []Message) {
	tb.Helper()
	if err != nil || len(got) != len(want) {
		tb.Fatalf("messages count=%d want=%d err=%v", len(got), len(want), err)
	}
	for i := range want {
		if messageIdentity(got[i]) != messageIdentity(want[i]) || got[i].CreatedAt != want[i].CreatedAt || got[i].Sender != want[i].Sender || string(got[i].Body) != string(want[i].Body) {
			tb.Fatalf("message %d: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}
