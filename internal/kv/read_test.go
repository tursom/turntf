package kv

import (
	"errors"
	"reflect"
	"testing"
)

func TestCanPrincipalIsolation(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: "create_database", Database: "private", Owner: "owner"}, 1)
	apply(t, f, Command{Op: "grant", Database: "private", Principal: "reader", Permission: PermissionRead}, 2)
	apply(t, f, Command{Op: "grant", Database: "private", Principal: "delegate", Permission: PermissionAdmin}, 3)
	for _, tc := range []struct {
		name       string
		database   string
		principal  Principal
		permission Permission
		want       bool
	}{
		{"owner", "private", Principal{Subject: "owner"}, PermissionRead | PermissionWrite | PermissionAdmin, true},
		{"reader", "private", Principal{Subject: "reader"}, PermissionRead, true},
		{"reader cannot write", "private", Principal{Subject: "reader"}, PermissionWrite, false},
		{"delegate cannot read", "private", Principal{Subject: "delegate"}, PermissionRead, false},
		{"stranger", "private", Principal{Subject: "stranger"}, PermissionRead, false},
		{"admin subject is not a role", "private", Principal{Subject: "admin"}, PermissionAdmin, false},
		{"system admin", "private", Principal{Subject: "admin", SystemAdmin: true}, PermissionRead | PermissionWrite | PermissionAdmin, true},
		{"empty admin", "private", Principal{SystemAdmin: true}, PermissionRead, false},
		{"missing database", "missing", Principal{Subject: "admin", SystemAdmin: true}, PermissionRead, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := f.CanPrincipal(tc.database, tc.principal, tc.permission); got != tc.want {
				t.Fatalf("CanPrincipal = %v, want %v", got, tc.want)
			}
		})
	}
	apply(t, f, Command{Op: "grant", Database: "private", Principal: "*", Permission: PermissionRead}, 4)
	if !f.CanPrincipal("private", Principal{Subject: "stranger"}, PermissionRead) || f.CanPrincipal("private", Principal{}, PermissionRead) {
		t.Fatal("wildcard must allow authenticated readers only")
	}
}

func TestDatabasesVisibilityAndOwnerFilter(t *testing.T) {
	f := NewFSM()
	for i, db := range []DatabaseInfo{{"z-private", "bob"}, {"a-owned", "alice"}, {"c-shared", "bob"}, {"b-public", "bob"}, {"d-zero", "bob"}} {
		apply(t, f, Command{Op: "create_database", Database: db.Name, Owner: db.Owner}, uint64(i+1))
	}
	apply(t, f, Command{Op: "revoke", Database: "a-owned", Principal: "alice"}, 6)
	apply(t, f, Command{Op: "grant", Database: "c-shared", Principal: "alice", Permission: PermissionWrite}, 7)
	apply(t, f, Command{Op: "grant", Database: "b-public", Principal: "*", Permission: PermissionRead}, 8)
	apply(t, f, Command{Op: "grant", Database: "d-zero", Principal: "alice", Permission: 0}, 9)
	for _, tc := range []struct {
		name      string
		principal Principal
		owner     string
		want      []DatabaseInfo
	}{
		{"visible sorted", Principal{Subject: "alice"}, "", []DatabaseInfo{{"a-owned", "alice"}, {"b-public", "bob"}, {"c-shared", "bob"}}},
		{"other owner still filtered by ACL", Principal{Subject: "alice"}, "bob", []DatabaseInfo{{"b-public", "bob"}, {"c-shared", "bob"}}},
		{"owner without ACL", Principal{Subject: "alice"}, "alice", []DatabaseInfo{{"a-owned", "alice"}}},
		{"public only", Principal{Subject: "stranger"}, "", []DatabaseInfo{{"b-public", "bob"}}},
		{"admin all", Principal{Subject: "admin", SystemAdmin: true}, "", []DatabaseInfo{{"a-owned", "alice"}, {"b-public", "bob"}, {"c-shared", "bob"}, {"d-zero", "bob"}, {"z-private", "bob"}}},
		{"admin filter", Principal{Subject: "admin", SystemAdmin: true}, "alice", []DatabaseInfo{{"a-owned", "alice"}}},
		{"unknown owner", Principal{Subject: "admin", SystemAdmin: true}, "nobody", []DatabaseInfo{}},
		{"anonymous admin", Principal{SystemAdmin: true}, "", []DatabaseInfo{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, rev := f.Databases(tc.principal, tc.owner)
			if !reflect.DeepEqual(got, tc.want) || rev != 9 {
				t.Fatalf("Databases = %#v, revision %d; want %#v, 9", got, rev, tc.want)
			}
		})
	}
	if f.CanPrincipal("a-owned", Principal{Subject: "alice"}, PermissionRead) {
		t.Fatal("ownership must not restore revoked ACL")
	}
	items, _ := f.Databases(Principal{Subject: "alice"}, "alice")
	items[0].Owner = "changed"
	again, _ := f.Databases(Principal{Subject: "alice"}, "alice")
	if again[0].Owner != "alice" {
		t.Fatal("database metadata aliases FSM state")
	}
}

func TestAccessRequiresAdminAndReturnsCopy(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: "create_database", Database: "db", Owner: "owner"}, 1)
	apply(t, f, Command{Op: "grant", Database: "db", Principal: "reader", Permission: PermissionRead | PermissionWrite}, 2)
	apply(t, f, Command{Op: "grant", Database: "db", Principal: "delegate", Permission: PermissionAdmin}, 3)
	apply(t, f, Command{Op: "grant", Database: "db", Principal: "*", Permission: PermissionRead}, 4)
	for _, p := range []Principal{{}, {SystemAdmin: true}, {Subject: "reader"}, {Subject: "stranger"}} {
		if _, _, err := f.Access("db", p); !errors.Is(err, ErrPermission) {
			t.Fatalf("Access(%+v) error = %v", p, err)
		}
	}
	admin := Principal{Subject: "admin", SystemAdmin: true}
	if _, _, err := f.Access("missing", admin); !errors.Is(err, ErrPermission) {
		t.Fatalf("missing database error = %v", err)
	}
	for _, tc := range []struct {
		principal  Principal
		permission Permission
	}{
		{Principal{Subject: "owner"}, PermissionRead | PermissionWrite | PermissionAdmin},
		{Principal{Subject: "delegate"}, PermissionRead | PermissionAdmin},
		{admin, PermissionRead | PermissionWrite | PermissionAdmin},
	} {
		access, rev, err := f.Access("db", tc.principal)
		if err != nil || rev != 4 || access.Name != "db" || access.Owner != "owner" || access.Permission != tc.permission {
			t.Fatalf("Access(%+v) = %+v, %d, %v", tc.principal, access, rev, err)
		}
		delete(access.ACL, "owner")
		access.ACL["intruder"] = PermissionAdmin
		again, _, err := f.Access("db", admin)
		if err != nil || again.ACL["owner"] != 7 || again.ACL["intruder"] != 0 {
			t.Fatalf("ACL copy changed FSM: %+v, %v", again, err)
		}
	}
	apply(t, f, Command{Op: "revoke", Database: "db", Principal: "owner"}, 5)
	if _, _, err := f.Access("db", Principal{Subject: "owner"}); !errors.Is(err, ErrPermission) {
		t.Fatalf("revoked owner can inspect ACL: %v", err)
	}
}
