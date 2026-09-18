package kv

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
)

func apply(t *testing.T, f *FSM, cmd Command, index uint64) Result {
	t.Helper()
	b, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	v := f.Apply(&raft.Log{Index: index, Data: b})
	if err, ok := v.(error); ok {
		t.Fatal(err)
	}
	return v.(Result)
}

func TestTransactionCompareAndSetIsAtomic(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: "create_database", Database: "overlay", Owner: "owner"}, 1)
	apply(t, f, Command{Op: "put", Database: "overlay", Key: "leases/ip/64", Value: []byte("node-a")}, 2)
	exists := false
	b, err := json.Marshal(Command{Op: "txn", Database: "overlay", Compare: []Compare{{Key: "leases/ip/64", Exists: &exists}}, Puts: []Put{{Key: "leases/ip/64", Value: []byte("node-b")}}})
	if err != nil {
		t.Fatal(err)
	}
	value := f.Apply(&raft.Log{Index: 3, Data: b})
	result, ok := value.(Result)
	if !ok || result.Succeeded {
		t.Fatal("expected compare failure")
	}
	entry, _, err := f.Get("overlay", "leases/ip/64")
	if err != nil || string(entry.Value) != "node-a" {
		t.Fatalf("lease changed after failed txn: %q %v", entry.Value, err)
	}
}

func TestDatabaseACLAndWildcardRead(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: "create_database", Database: "overlay", Owner: "owner"}, 1)
	if !f.Can("overlay", "reader", PermissionRead) { /* owner-only database must remain private */
	} else {
		t.Fatal("unexpected implicit read permission")
	}
	apply(t, f, Command{Op: "grant", Database: "overlay", Principal: "reader", Permission: PermissionRead}, 2)
	if !f.Can("overlay", "reader", PermissionRead) || f.Can("overlay", "reader", PermissionWrite) {
		t.Fatal("unexpected ACL result")
	}
	apply(t, f, Command{Op: "grant", Database: "overlay", Principal: "*", Permission: PermissionRead}, 3)
	if !f.Can("overlay", "another", PermissionRead) {
		t.Fatal("wildcard read was not applied")
	}
}
