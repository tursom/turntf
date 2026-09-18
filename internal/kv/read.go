package kv

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
