package cluster

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tursom/turntf/internal/api"
	"github.com/tursom/turntf/internal/kv"
)

var _ api.KVService = (*Manager)(nil)

// 只省去网络，使用临时磁盘和真实 Raft 验证选主、日志提交和 FSM 应用。
func newKVAPIManager(t *testing.T) *Manager {
	t.Helper()
	address, transport := raft.NewInmemTransport("kv-api-test")
	node, err := kv.NewPersistentNode(kv.NodeOptions{ID: "kv-api-test", Address: address, Transport: transport}, t.TempDir())
	if err != nil {
		_ = transport.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
		_ = transport.Close()
	})
	if err := node.Bootstrap([]raft.Server{{ID: "kv-api-test", Address: address, Suffrage: raft.Voter}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for node.Raft().State() != raft.Leader {
		select {
		case <-deadline.C:
			t.Fatal("single-node Raft did not elect a leader")
		case <-ticker.C:
		}
	}
	return &Manager{kvNode: node}
}

func TestKVAPIAdminCrossOwnerAndACLIsolation(t *testing.T) {
	m := newKVAPIManager(t)
	ctx := context.Background()
	owner := kv.Principal{Subject: "owner"}
	admin := kv.Principal{Subject: "admin", SystemAdmin: true}
	reader := kv.Principal{Subject: "reader"}
	stranger := kv.Principal{Subject: "stranger"}
	mustSucceed := func(result kv.Result, err error) kv.Result {
		t.Helper()
		if err != nil || !result.Succeeded || result.Revision == 0 {
			t.Fatalf("mutation = %+v, %v", result, err)
		}
		return result
	}
	mustSucceed(m.KVCreateDatabase(ctx, owner, "owned"))
	mustSucceed(m.KVCreateDatabase(ctx, stranger, "other"))
	mustSucceed(m.KVCreateDatabase(ctx, admin, "admin-owned"))
	// 数据库名称属于全局空间，管理员不能用相同名称创建另一个用户的库。
	if _, err := m.KVCreateDatabase(ctx, admin, "owned"); err == nil {
		t.Fatal("duplicate global database name accepted")
	}
	first := mustSucceed(m.KVPut(ctx, admin, "owned", "cfg/a", []byte("first")))
	updated := mustSucceed(m.KVPut(ctx, admin, "owned", "cfg/a", []byte("updated")))
	entry, _, err := m.KVGet(ctx, admin, "owned", "cfg/a")
	if err != nil || string(entry.Value) != "updated" || entry.CreateRevision != first.Revision || entry.ModRevision != updated.Revision {
		t.Fatalf("admin get = %+v, %v", entry, err)
	}
	mustSucceed(m.KVPut(ctx, admin, "owned", "outside", []byte("excluded")))
	items, _, err := m.KVList(ctx, admin, "owned", "cfg/")
	if err != nil || len(items) != 1 || string(items["cfg/a"].Value) != "updated" {
		t.Fatalf("admin prefix list = %+v, %v", items, err)
	}
	mustSucceed(m.KVGrant(ctx, admin, "owned", reader.Subject, kv.PermissionRead))
	if _, _, err := m.KVGet(ctx, reader, "owned", "cfg/a"); err != nil {
		t.Fatalf("granted reader: %v", err)
	}
	watch, err := m.KVWatch(reader, "owned", "cfg/")
	if err != nil {
		t.Fatal(err)
	}
	watch.Close()
	denied := []struct {
		name string
		call func(kv.Principal) error
	}{
		{"put", func(p kv.Principal) error { _, err := m.KVPut(ctx, p, "owned", "cfg/a", []byte("bad")); return err }},
		{"delete", func(p kv.Principal) error { _, err := m.KVDelete(ctx, p, "owned", "cfg/a"); return err }},
		{"txn", func(p kv.Principal) error {
			_, err := m.KVTxn(ctx, p, "owned", nil, nil, []string{"cfg/a"})
			return err
		}},
		{"grant", func(p kv.Principal) error { _, err := m.KVGrant(ctx, p, "owned", "stranger", 7); return err }},
		{"revoke", func(p kv.Principal) error { _, err := m.KVRevoke(ctx, p, "owned", owner.Subject); return err }},
		{"acl", func(p kv.Principal) error { _, _, err := m.KVAccess(ctx, p, "owned"); return err }},
	}
	_, beforeDenied, _ := m.KVDatabases(ctx, admin, "")
	for _, tc := range denied {
		for _, p := range []kv.Principal{reader, stranger, {Subject: "admin"}, {SystemAdmin: true}} {
			if err := tc.call(p); !errors.Is(err, kv.ErrPermission) {
				t.Fatalf("%s by %+v = %v, want permission denied", tc.name, p, err)
			}
		}
	}
	if _, _, err := m.KVGet(ctx, stranger, "owned", "cfg/a"); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("stranger get = %v", err)
	}
	if _, _, err := m.KVList(ctx, stranger, "owned", ""); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("stranger list = %v", err)
	}
	if w, err := m.KVWatch(stranger, "owned", ""); !errors.Is(err, kv.ErrPermission) {
		w.Close()
		t.Fatalf("stranger watch = %v", err)
	}
	_, afterDenied, _ := m.KVDatabases(ctx, admin, "")
	if afterDenied != beforeDenied {
		t.Fatal("denied operations reached the FSM")
	}
	// 比较失败是已提交的业务结果；puts 和 deletes 都必须保持原状。
	absent := false
	conflict, err := m.KVTxn(ctx, admin, "owned", []kv.Compare{{Key: "cfg/a", Exists: &absent}}, []kv.Put{{Key: "new", Value: []byte("bad")}}, []string{"cfg/a"})
	if err != nil || conflict.Succeeded || conflict.Revision <= afterDenied {
		t.Fatalf("compare conflict = %+v, %v", conflict, err)
	}
	if _, _, err := m.KVGet(ctx, admin, "owned", "new"); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("failed txn inserted key: %v", err)
	}
	entry, _, err = m.KVGet(ctx, owner, "owned", "cfg/a")
	if err != nil || string(entry.Value) != "updated" || entry.ModRevision != updated.Revision {
		t.Fatalf("failed txn changed key: %+v, %v", entry, err)
	}
	stale, err := m.KVTxn(ctx, admin, "owned", []kv.Compare{{Key: "cfg/a", Revision: first.Revision}}, []kv.Put{{Key: "cfg/a", Value: []byte("stale")}}, nil)
	if err != nil || stale.Succeeded {
		t.Fatalf("stale revision accepted: %+v, %v", stale, err)
	}
	txn := mustSucceed(m.KVTxn(ctx, admin, "owned", []kv.Compare{{Key: "cfg/a", Revision: updated.Revision}}, []kv.Put{{Key: "cfg/b", Value: []byte("txn")}}, []string{"cfg/a"}))
	if _, _, err := m.KVGet(ctx, owner, "owned", "cfg/a"); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("txn did not delete: %v", err)
	}
	entry, _, err = m.KVGet(ctx, owner, "owned", "cfg/b")
	if err != nil || string(entry.Value) != "txn" || entry.ModRevision != txn.Revision {
		t.Fatalf("txn put = %+v, %v", entry, err)
	}
	mustSucceed(m.KVDelete(ctx, admin, "owned", "cfg/b"))
	if _, _, err := m.KVGet(ctx, admin, "owned", "cfg/b"); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("admin delete = %v", err)
	}
	mustSucceed(m.KVGrant(ctx, admin, "owned", reader.Subject, kv.PermissionWrite))
	mustSucceed(m.KVPut(ctx, reader, "owned", "write-only", []byte("ok")))
	if _, _, err := m.KVGet(ctx, reader, "owned", "write-only"); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("grant must replace permissions: %v", err)
	}
	mustSucceed(m.KVRevoke(ctx, admin, "owned", reader.Subject))
	if _, err := m.KVPut(ctx, reader, "owned", "write-only", nil); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("revoked writer = %v", err)
	}
	// 数据库管理员只能管理当前库，不能跨库，也不会隐式获得读权限。
	mustSucceed(m.KVGrant(ctx, admin, "owned", reader.Subject, kv.PermissionAdmin))
	mustSucceed(m.KVGrant(ctx, reader, "owned", stranger.Subject, kv.PermissionRead))
	if _, _, err := m.KVAccess(ctx, reader, "owned"); err != nil {
		t.Fatalf("database admin cannot inspect ACL: %v", err)
	}
	if _, _, err := m.KVGet(ctx, reader, "owned", "outside"); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("database admin gained implicit read: %v", err)
	}
	if _, err := m.KVGrant(ctx, reader, "other", reader.Subject, 7); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("database admin escaped database scope: %v", err)
	}
	mustSucceed(m.KVRevoke(ctx, reader, "owned", stranger.Subject))
	mustSucceed(m.KVRevoke(ctx, admin, "owned", reader.Subject))
	mustSucceed(m.KVRevoke(ctx, admin, "owned", owner.Subject))
	if _, _, err := m.KVGet(ctx, owner, "owned", "outside"); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("owner bypassed revoked ACL: %v", err)
	}
	for _, tc := range []struct {
		principal kv.Principal
		owner     string
		want      []kv.DatabaseInfo
	}{
		{admin, "owner", []kv.DatabaseInfo{{Name: "owned", Owner: "owner"}}},
		{admin, "admin", []kv.DatabaseInfo{{Name: "admin-owned", Owner: "admin"}}},
		{owner, "", []kv.DatabaseInfo{{Name: "owned", Owner: "owner"}}},
		{owner, "stranger", []kv.DatabaseInfo{}},
		{reader, "owner", []kv.DatabaseInfo{}},
	} {
		got, _, err := m.KVDatabases(ctx, tc.principal, tc.owner)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("databases(%+v, %q) = %+v, %v; want %+v", tc.principal, tc.owner, got, err, tc.want)
		}
	}
	access, _, err := m.KVAccess(ctx, admin, "owned")
	if err != nil || access.Owner != owner.Subject || access.Permission != 7 || len(access.ACL) != 0 {
		t.Fatalf("owner or ACL changed unexpectedly: %+v, %v", access, err)
	}
	access.ACL["stranger"] = 7
	if _, _, err := m.KVGet(ctx, stranger, "owned", "outside"); !errors.Is(err, kv.ErrPermission) {
		t.Fatalf("ACL response aliases FSM: %v", err)
	}
	// 无 ACL 的库仍可由系统管理员恢复授权，且不会转移所有权。
	mustSucceed(m.KVGrant(ctx, admin, "owned", owner.Subject, 7))
	access, _, err = m.KVAccess(ctx, owner, "owned")
	if err != nil || access.Owner != owner.Subject || access.ACL[owner.Subject] != 7 {
		t.Fatalf("restored owner = %+v, %v", access, err)
	}
}
