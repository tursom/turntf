package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/tursom/turntf/internal/kv"
)

func (m *Manager) KVWatch(principal kv.Principal, database, prefix string) (kv.Watch, error) {
	if m == nil || m.kvNode == nil {
		return kv.Watch{}, errors.New("kv: consensus is disabled")
	}
	if !m.kvNode.FSM().CanPrincipal(database, principal, kv.PermissionRead) {
		return kv.Watch{}, kv.ErrPermission
	}
	return m.kvNode.FSM().Watch(database, prefix), nil
}
func (m *Manager) KVCreateDatabase(ctx context.Context, principal kv.Principal, database string) (kv.Result, error) {
	if principal.Subject == "" || database == "" {
		return kv.Result{}, errors.New("kv: principal and database are required")
	}
	return m.applyKV(ctx, principal, kv.Command{Op: "create_database", Database: database, Owner: principal.Subject}, kv.PermissionAdmin)
}
func (m *Manager) KVDatabases(_ context.Context, principal kv.Principal, owner string) ([]kv.DatabaseInfo, uint64, error) {
	if m == nil || m.kvNode == nil {
		return nil, 0, errors.New("kv: consensus is disabled")
	}
	items, rev := m.kvNode.FSM().Databases(principal, owner)
	return items, rev, nil
}
func (m *Manager) KVAccess(_ context.Context, principal kv.Principal, database string) (kv.DatabaseAccess, uint64, error) {
	if m == nil || m.kvNode == nil {
		return kv.DatabaseAccess{}, 0, errors.New("kv: consensus is disabled")
	}
	return m.kvNode.FSM().Access(database, principal)
}
func (m *Manager) KVGet(_ context.Context, principal kv.Principal, database, key string) (kv.Entry, uint64, error) {
	if m == nil || m.kvNode == nil {
		return kv.Entry{}, 0, errors.New("kv: consensus is disabled")
	}
	if !m.kvNode.FSM().CanPrincipal(database, principal, kv.PermissionRead) {
		return kv.Entry{}, 0, kv.ErrPermission
	}
	return m.kvNode.FSM().Get(database, key)
}
func (m *Manager) KVList(_ context.Context, principal kv.Principal, database, prefix string) (map[string]kv.Entry, uint64, error) {
	if m == nil || m.kvNode == nil {
		return nil, 0, errors.New("kv: consensus is disabled")
	}
	if !m.kvNode.FSM().CanPrincipal(database, principal, kv.PermissionRead) {
		return nil, 0, kv.ErrPermission
	}
	return m.kvNode.FSM().List(database, prefix)
}
func (m *Manager) KVPut(ctx context.Context, principal kv.Principal, database, key string, value []byte) (kv.Result, error) {
	return m.applyKV(ctx, principal, kv.Command{Op: "put", Database: database, Key: key, Value: value}, kv.PermissionWrite)
}
func (m *Manager) KVDelete(ctx context.Context, principal kv.Principal, database, key string) (kv.Result, error) {
	return m.applyKV(ctx, principal, kv.Command{Op: "delete", Database: database, Key: key}, kv.PermissionWrite)
}
func (m *Manager) KVTxn(ctx context.Context, principal kv.Principal, database string, compare []kv.Compare, puts []kv.Put, deletes []string) (kv.Result, error) {
	return m.applyKV(ctx, principal, kv.Command{Op: "txn", Database: database, Compare: compare, Puts: puts, Deletes: deletes}, kv.PermissionWrite)
}
func (m *Manager) KVGrant(ctx context.Context, principal kv.Principal, database, target string, permission kv.Permission) (kv.Result, error) {
	return m.applyKV(ctx, principal, kv.Command{Op: "grant", Database: database, Principal: target, Permission: permission}, kv.PermissionAdmin)
}
func (m *Manager) KVRevoke(ctx context.Context, principal kv.Principal, database, target string) (kv.Result, error) {
	return m.applyKV(ctx, principal, kv.Command{Op: "revoke", Database: database, Principal: target}, kv.PermissionAdmin)
}
func (m *Manager) applyKV(ctx context.Context, principal kv.Principal, cmd kv.Command, required kv.Permission) (kv.Result, error) {
	if m == nil || m.kvNode == nil {
		return kv.Result{}, errors.New("kv: consensus is disabled")
	}
	if principal.Subject == "" || (cmd.Op != "create_database" && !m.kvNode.FSM().CanPrincipal(cmd.Database, principal, required)) {
		return kv.Result{}, kv.ErrPermission
	}
	result, err := m.kvNode.Apply(ctx, cmd)
	if err != nil {
		return kv.Result{}, fmt.Errorf("kv: apply %s: %w", cmd.Op, err)
	}
	return result, nil
}
