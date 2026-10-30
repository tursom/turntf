package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/tursom/turntf/internal/clock"
	clusterproto "github.com/tursom/turntf/internal/proto"
)

func TestSQLiteCPUMessageAuthorizationTracksTransactionState(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	ctx := context.Background()
	newUser := func(name string) User {
		t.Helper()
		user, _, err := st.CreateUser(ctx, CreateUserParams{Username: name, PasswordHash: "hash"})
		if err != nil {
			t.Fatal(err)
		}
		return user
	}
	owner, sender := newUser("owner"), newUser("sender")
	check := func(recipient, from UserKey, want error) {
		t.Helper()
		_, _, err := st.CreateMessage(ctx, CreateMessageParams{UserKey: recipient, Sender: from, Body: []byte("authorization")})
		if !errors.Is(err, want) {
			t.Fatalf("recipient=%v sender=%v: want=%v got=%v", recipient, from, want, err)
		}
	}
	check(owner.Key(), sender.Key(), nil)
	if _, _, err := st.BlockUser(ctx, BlacklistParams{Owner: owner.Key(), Blocked: sender.Key()}); err != nil {
		t.Fatal(err)
	}
	check(owner.Key(), sender.Key(), ErrBlockedByBlacklist)
	check(owner.Key(), owner.Key(), nil)
	check(owner.Key(), testSenderKey(9, 99), nil)
	// 暖过普通用户读取后，直接在 SQL 中改变角色，确保没有用缓存事实做权限判断。
	for _, role := range []string{RoleChannel, RoleUser} {
		if _, err := st.db.ExecContext(ctx, `UPDATE users SET role = ? WHERE node_id = ? AND user_id = ?`, role, sender.NodeID, sender.ID); err != nil {
			t.Fatal(err)
		}
		var want error
		if role == RoleUser {
			want = ErrBlockedByBlacklist
		}
		check(owner.Key(), sender.Key(), want)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE users SET role = ? WHERE node_id = ? AND user_id = ?`, RoleChannel, owner.NodeID, owner.ID); err != nil {
		t.Fatal(err)
	}
	check(owner.Key(), sender.Key(), nil)
	if _, err := st.db.ExecContext(ctx, `UPDATE users SET role = ? WHERE node_id = ? AND user_id = ?`, RoleUser, owner.NodeID, owner.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DeleteUser(ctx, sender.Key()); err != nil {
		t.Fatal(err)
	}
	check(owner.Key(), sender.Key(), nil)
	if _, err := st.DeleteUser(ctx, owner.Key()); err != nil {
		t.Fatal(err)
	}
	check(owner.Key(), sender.Key(), ErrNotFound)
	check(testSenderKey(9, 98), sender.Key(), ErrNotFound)
}

func TestSQLiteCPUTrimMatchesOffsetDeletionAndRollback(t *testing.T) {
	st := openNamedTestStoreWithWindow(t, "trim-oracle", 1, 7)
	defer st.Close()
	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "trim-oracle", PasswordHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := st.CreateUser(ctx, CreateUserParams{Username: "other", PasswordHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	projection := st.backend.MessageProjection().(*sqliteMessageProjectionRepository)
	rng := rand.New(rand.NewSource(7))
	for _, count := range []int{0, 6, 7, 8, 70} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			tx, err := st.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for i := 0; i < count; i++ {
				created := clock.Timestamp{WallTimeMs: 1740000000000 + int64(rng.Intn(3)), Logical: 1, NodeID: 1}
				for _, key := range []UserKey{user.Key(), other.Key()} {
					if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
`, key.NodeID, key.UserID, 1+i%3, i+1, key.NodeID, key.UserID, []byte("trim"), created.String(), MessageSession(key, key)); err != nil {
						t.Fatal(err)
					}
				}
			}
			readIDs := func(limit bool) [][2]int64 {
				t.Helper()
				query := `SELECT node_id, seq FROM messages WHERE user_node_id = ? AND user_id = ? ORDER BY created_at_hlc DESC, node_id ASC, seq DESC`
				if limit {
					query += ` LIMIT 7`
				}
				rows, err := tx.QueryContext(ctx, query, user.NodeID, user.ID)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var ids [][2]int64
				for rows.Next() {
					var id [2]int64
					if err := rows.Scan(&id[0], &id[1]); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, id)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return ids
			}
			want := readIDs(true)
			if err := projection.trimMessagesForUserTx(ctx, tx, user.Key()); err != nil {
				t.Fatal(err)
			}
			if got := readIDs(false); !reflect.DeepEqual(got, want) {
				t.Fatalf("wrong survivor set: want=%v got=%v", want, got)
			}
			var otherCount, trimmed int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE user_node_id = ? AND user_id = ?`, other.NodeID, other.ID).Scan(&otherCount); err != nil || otherCount != count {
				t.Fatalf("other user changed: count=%d err=%v", otherCount, err)
			}
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT trimmed_total FROM message_trim_stats WHERE scope = 'global'), 0)`).Scan(&trimmed); err != nil || trimmed != max(count-7, 0) {
				t.Fatalf("trim statistics: total=%d err=%v", trimmed, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			stats, err := st.OperationsStats(ctx, nil)
			if err != nil || stats.MessageTrim.TrimmedTotal != 0 || stats.MessageTrim.LastTrimmedAt != nil {
				t.Fatalf("trim statistics survived rollback: stats=%+v err=%v", stats.MessageTrim, err)
			}
		})
	}
}

func TestSQLiteCPUTrimSnapshotBatchAndDuplicate(t *testing.T) {
	st := openNamedTestStoreWithWindow(t, "trim-snapshot", 1, 3)
	defer st.Close()
	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "snapshot", PasswordHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	var rows []*clusterproto.SnapshotRow
	for i := 1; i <= 12; i++ {
		rows = append(rows, snapshotRowFromMessage(Message{Recipient: user.Key(), Sender: user.Key(), NodeID: 2, Seq: int64(i), Body: []byte("snapshot"), CreatedAt: clock.Timestamp{WallTimeMs: 1740000000000, NodeID: 2}}))
	}
	projection := st.backend.MessageProjection()
	for i := 0; i < 2; i++ {
		if err := projection.ApplyMessageSnapshotRows(ctx, 2, rows); err != nil {
			t.Fatal(err)
		}
		messages, err := projection.ListMessagesByUser(ctx, user.Key(), 100)
		if err != nil || len(messages) != 3 || messages[0].Seq != 12 || messages[2].Seq != 10 {
			t.Fatalf("snapshot window: messages=%v err=%v", messages, err)
		}
		stats, err := st.OperationsStats(ctx, nil)
		// 重放已裁剪的快照会再次插入并裁剪历史行，与基线统计相同。
		if err != nil || stats.MessageTrim.TrimmedTotal != int64(9*(i+1)) {
			t.Fatalf("snapshot trim count: stats=%+v err=%v", stats.MessageTrim, err)
		}
		if err := projection.ApplyMessageCreated(ctx, messages[0]); err != nil {
			t.Fatalf("duplicate projection: %v", err)
		}
		stats, err = st.OperationsStats(ctx, nil)
		if err != nil || stats.MessageTrim.TrimmedTotal != int64(9*(i+1)) {
			t.Fatalf("duplicate changed trim count: stats=%+v err=%v", stats.MessageTrim, err)
		}
	}
}

func TestSQLiteCPUProjectionSavepointRollsBackTrimAndReplays(t *testing.T) {
	st := openNamedTestStoreWithWindow(t, "trim-savepoint", 1, 1)
	defer st.Close()
	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "savepoint", PasswordHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	write := func(body string) (Message, Event, error) {
		return st.CreateMessage(ctx, CreateMessageParams{UserKey: user.Key(), Sender: user.Key(), Body: []byte(body)})
	}
	if _, _, err := write("first"); err != nil {
		t.Fatal(err)
	}
	// 在消息已插入且旧消息已删除后使统计写入失败，验证整个投影保存点回滚。
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER fail_trim BEFORE INSERT ON message_trim_stats BEGIN SELECT RAISE(ABORT, 'trim unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	message, event, err := write("deferred")
	if !errors.Is(err, ErrProjectionDeferred) || message.Seq != 2 || event.Sequence == 0 {
		t.Fatalf("expected durable event and deferred projection: message=%+v event=%+v err=%v", message, event, err)
	}
	messages, err := st.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil || len(messages) != 1 || string(messages[0].Body) != "first" {
		t.Fatalf("savepoint did not restore original window: messages=%v err=%v", messages, err)
	}
	stats, err := st.OperationsStats(ctx, nil)
	if err != nil || stats.MessageTrim.TrimmedTotal != 0 || stats.MessageTrim.LastTrimmedAt != nil {
		t.Fatalf("failed projection changed trim statistics: stats=%+v err=%v", stats.MessageTrim, err)
	}
	pending, err := st.projectionStats(ctx)
	if err != nil || pending.PendingTotal != 1 {
		t.Fatalf("missing deferred projection: pending=%+v err=%v", pending, err)
	}
	if _, err := st.db.ExecContext(ctx, `DROP TRIGGER fail_trim`); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplayPendingEvents(ctx, 10); err != nil {
		t.Fatal(err)
	}
	messages, err = st.ListMessagesByUser(ctx, user.Key(), 10)
	if err != nil || len(messages) != 1 || string(messages[0].Body) != "deferred" {
		t.Fatalf("replayed window: messages=%v err=%v", messages, err)
	}
	stats, err = st.OperationsStats(ctx, nil)
	if err != nil || stats.MessageTrim.TrimmedTotal != 1 || stats.MessageTrim.LastTrimmedAt == nil {
		t.Fatalf("replayed trim statistics: stats=%+v err=%v", stats.MessageTrim, err)
	}
	pending, err = st.projectionStats(ctx)
	if err != nil || pending.PendingTotal != 0 {
		t.Fatalf("replay left pending projection: pending=%+v err=%v", pending, err)
	}
}
