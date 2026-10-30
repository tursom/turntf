package store

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/clock"
)

func TestSQLiteCPUStatementsSingleConnectionLifecycle(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "single.db"), Options{NodeID: testNodeID(1), MessageWindowSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}
	b := st.backend.(*sqliteStoreBackend)
	oldStatements := b.statements
	if len(oldStatements) == 0 {
		t.Fatal("Bind did not prepare statements")
	}
	// 重复初始化应释放前一批语句，不能泄漏或使新 Repository 持有关闭的语句。
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range oldStatements {
		if _, err := stmt.ExecContext(ctx); err == nil || !strings.Contains(err.Error(), "statement is closed") {
			t.Fatalf("old statement remains usable: %v", err)
		}
	}
	user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "single", PasswordHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, _, err := st.CreateMessage(ctx, CreateMessageParams{UserKey: user.Key(), Sender: user.Key(), Body: []byte("single")}); err != nil {
			t.Fatalf("single-connection transaction %d: %v", i, err)
		}
	}
	statements := b.statements
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range statements {
		if _, err := stmt.ExecContext(ctx); err == nil || !strings.Contains(err.Error(), "statement is closed") {
			t.Fatalf("Close did not release statement: %v", err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatalf("repeated backend Close: %v", err)
	}
}

func TestSQLiteCPUPrepareFailureDoesNotPublishStatements(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "partial.db"), Options{NodeID: testNodeID(1)})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	st.db.SetMaxOpenConns(1)
	// 第一条能 prepare，第二条因缺表失败；失败不能发布半初始化的语句集。
	if _, err := st.db.Exec(`CREATE TABLE event_log(event_id INTEGER, origin_node_id INTEGER, value BLOB)`); err != nil {
		t.Fatal(err)
	}
	statements, err := prepareSQLiteStatements(st.db)
	if err == nil || statements != nil {
		t.Fatalf("partial preparation: statements=%v err=%v", statements, err)
	}
	if err := st.db.Ping(); err != nil {
		t.Fatalf("connection not released after prepare failure: %v", err)
	}
}

func TestSQLiteCPUMessageUserPreservesReadErrors(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	ctx := context.Background()
	user, _, err := st.CreateUser(ctx, CreateUserParams{Username: "read-errors", PasswordHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	b := st.backend.(*sqliteStoreBackend)
	fields := []string{"created_at_hlc", "updated_at_hlc", "version_username", "version_password_hash", "version_profile", "version_role", "version_deleted", "system_reserved", "origin_node_id"}
	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			tx, err := st.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE users SET %s = ? WHERE node_id = ? AND user_id = ?", field), "invalid", user.NodeID, user.ID); err != nil {
				t.Fatal(err)
			}
			_, want := st.getUserTx(ctx, tx, user.Key(), false)
			_, got := b.messageUserRoleTx(ctx, tx, user.Key())
			if want == nil || got == nil {
				t.Fatalf("invalid %s: want=%v got=%v", field, want, got)
			}
			if strings.HasPrefix(field, "version_") || strings.HasSuffix(field, "_hlc") {
				if got.Error() != want.Error() {
					t.Fatalf("changed timestamp error: want=%v got=%v", want, got)
				}
			}
		})
	}
}

func TestSQLiteCPUUserTimestampValidationMatchesParser(t *testing.T) {
	valid := "1740000000000-65535-9223372036854775807"
	cases := []string{
		valid, "0000000000000-00000-0000000000000000001",
		"1740000000000-65536-0000000000000000001",
		"1740000000000-00001-9223372036854775808",
		"1740000000000-00001-0000000000000000000",
		"+740000000000-00001-+000000000000000001",
		"1740000000000-+0001-0000000000000000001",
		"invalid", "", valid + "0", valid[:len(valid)-1],
	}
	// 每一位的数字、分隔符或非 ASCII 变化都与现有解析器核对。
	for i := range valid {
		for _, replacement := range []byte{'0', '9', '+', '-', 'x', 0xff} {
			value := []byte(valid)
			value[i] = replacement
			cases = append(cases, string(value))
		}
	}
	for _, value := range cases {
		_, want := clock.ParseTimestamp(value)
		got := validateSQLiteUserTimestamp(value, "created_at")
		if (want == nil) != (got == nil) {
			t.Fatalf("%q: parser=%v validator=%v", value, want, got)
		}
		if want != nil && got.Error() != "parse user created_at: "+want.Error() {
			t.Fatalf("%q: changed parse error: %v", value, got)
		}
	}
}
