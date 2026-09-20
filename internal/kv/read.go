package kv

import "sort"

// CanPrincipal 先检查数据库存在，再检查系统管理员身份或数据库 ACL。
func (f *FSM) CanPrincipal(database string, principal Principal, required Permission) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	db, ok := f.state.Databases[database]
	return ok && principal.Subject != "" && (principal.SystemAdmin || canAccess(db, principal.Subject, required))
}

func canAccess(db Database, subject string, required Permission) bool {
	return db.ACL[subject]&required == required || db.ACL["*"]&required == required
}

// Databases 按所有者返回元数据，不复制整个状态机中的键值数据。
// 普通用户只能列出自有或有任一数据库权限的库；所有者身份本身不授予读写权限。
func (f *FSM) Databases(principal Principal, owner string) ([]DatabaseInfo, uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	items := make([]DatabaseInfo, 0)
	for name, db := range f.state.Databases {
		if owner != "" && owner != db.Owner {
			continue
		}
		if principal.Subject == "" || (!principal.SystemAdmin && db.Owner != principal.Subject && (db.ACL[principal.Subject]|db.ACL["*"]) == 0) {
			continue
		}
		items = append(items, DatabaseInfo{Name: name, Owner: db.Owner})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, f.state.Revision
}

// Access 返回独立的 ACL 副本，避免调用方修改状态机；只有数据库管理权限可读取授权列表。
func (f *FSM) Access(database string, principal Principal) (DatabaseAccess, uint64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	db, ok := f.state.Databases[database]
	if !ok || principal.Subject == "" || (!principal.SystemAdmin && !canAccess(db, principal.Subject, PermissionAdmin)) {
		return DatabaseAccess{}, 0, ErrPermission
	}
	acl := make(map[string]Permission, len(db.ACL))
	for subject, p := range db.ACL {
		acl[subject] = p
	}
	p := db.ACL[principal.Subject] | db.ACL["*"]
	if principal.SystemAdmin {
		p = PermissionRead | PermissionWrite | PermissionAdmin
	}
	return DatabaseAccess{Name: database, Owner: db.Owner, ACL: acl, Permission: p}, f.state.Revision, nil
}

func (f *FSM) Can(database, principal string, required Permission) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	db, ok := f.state.Databases[database]
	if !ok {
		return false
	}
	if p := db.ACL[principal]; p&required == required {
		return true
	}
	if p := db.ACL["*"]; p&required == required {
		return true
	}
	return false
}

func (f *FSM) Get(database, key string) (Entry, uint64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	db, ok := f.state.Databases[database]
	if !ok {
		return Entry{}, f.state.Revision, ErrNotFound
	}
	e, ok := db.Data[key]
	if !ok {
		return Entry{}, f.state.Revision, ErrNotFound
	}
	e.Value = append([]byte(nil), e.Value...)
	return e, f.state.Revision, nil
}

func (f *FSM) List(database, prefix string) (map[string]Entry, uint64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	db, ok := f.state.Databases[database]
	if !ok {
		return nil, f.state.Revision, ErrNotFound
	}
	out := make(map[string]Entry)
	for k, e := range db.Data {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			e.Value = append([]byte(nil), e.Value...)
			out[k] = e
		}
	}
	return out, f.state.Revision, nil
}
