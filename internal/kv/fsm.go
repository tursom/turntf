package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

var (
	ErrNotFound    = errors.New("kv: key not found")
	ErrCompareFail = errors.New("kv: compare failed")
	ErrPermission  = errors.New("kv: permission denied")
)

type Permission uint8

const (
	PermissionRead Permission = 1 << iota
	PermissionWrite
	PermissionAdmin
)

type Entry struct {
	Value          []byte `json:"value"`
	CreateRevision uint64 `json:"create_revision"`
	ModRevision    uint64 `json:"mod_revision"`
}

type Database struct {
	Owner string
	ACL   map[string]Permission
	Data  map[string]Entry
}

type State struct {
	Revision  uint64
	Databases map[string]Database
}

// Command is the deterministic state-machine input replicated by Raft.
type Command struct {
	Op         string     `json:"op"`
	Database   string     `json:"database,omitempty"`
	Key        string     `json:"key,omitempty"`
	Value      []byte     `json:"value,omitempty"`
	Owner      string     `json:"owner,omitempty"`
	Principal  string     `json:"principal,omitempty"`
	Permission Permission `json:"permission,omitempty"`
	Compare    []Compare  `json:"compare,omitempty"`
	Puts       []Put      `json:"puts,omitempty"`
	Deletes    []string   `json:"deletes,omitempty"`
}
type Compare struct {
	Key      string `json:"key"`
	Revision uint64 `json:"revision"`
	Exists   *bool  `json:"exists,omitempty"`
}
type Put struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}
type Result struct {
	Revision  uint64
	Entries   []Entry
	Succeeded bool
}

type FSM struct {
	mu    sync.RWMutex
	state State
}

func NewFSM() *FSM          { return &FSM{state: State{Databases: make(map[string]Database)}} }
func (f *FSM) State() State { f.mu.RLock(); defer f.mu.RUnlock(); return cloneState(f.state) }
func (f *FSM) Apply(log *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	result, err := f.applyLocked(cmd)
	if errors.Is(err, ErrCompareFail) {
		return result
	}
	if err != nil {
		return err
	}
	return result
}
func (f *FSM) applyLocked(c Command) (Result, error) {
	f.state.Revision++
	db, ok := f.state.Databases[c.Database]
	if c.Op == "create_database" {
		if ok {
			return Result{}, fmt.Errorf("kv: database exists")
		}
		f.state.Databases[c.Database] = Database{Owner: c.Owner, ACL: map[string]Permission{c.Owner: PermissionRead | PermissionWrite | PermissionAdmin}, Data: map[string]Entry{}}
		return Result{Revision: f.state.Revision, Succeeded: true}, nil
	}
	if !ok {
		return Result{}, ErrNotFound
	}
	switch c.Op {
	case "grant":
		if c.Principal == "" {
			return Result{}, ErrPermission
		}
		if db.ACL == nil {
			db.ACL = map[string]Permission{}
		}
		db.ACL[c.Principal] = c.Permission
		f.state.Databases[c.Database] = db
	case "revoke":
		delete(db.ACL, c.Principal)
		f.state.Databases[c.Database] = db
	case "put":
		db.Data[c.Key] = Entry{Value: append([]byte(nil), c.Value...), CreateRevision: entryCreate(db.Data[c.Key], f.state.Revision), ModRevision: f.state.Revision}
		f.state.Databases[c.Database] = db
	case "delete":
		delete(db.Data, c.Key)
		f.state.Databases[c.Database] = db
	case "txn":
		for _, cmp := range c.Compare {
			e, exists := db.Data[cmp.Key]
			if cmp.Exists != nil && *cmp.Exists != exists {
				return Result{Revision: f.state.Revision, Succeeded: false}, ErrCompareFail
			}
			if cmp.Revision != 0 && (!exists || e.ModRevision != cmp.Revision) {
				return Result{Revision: f.state.Revision, Succeeded: false}, ErrCompareFail
			}
		}
		for _, p := range c.Puts {
			db.Data[p.Key] = Entry{Value: append([]byte(nil), p.Value...), CreateRevision: entryCreate(db.Data[p.Key], f.state.Revision), ModRevision: f.state.Revision}
		}
		for _, k := range c.Deletes {
			delete(db.Data, k)
		}
		f.state.Databases[c.Database] = db
	default:
		return Result{}, fmt.Errorf("kv: unknown operation %q", c.Op)
	}
	return Result{Revision: f.state.Revision, Succeeded: true}, nil
}
func entryCreate(e Entry, rev uint64) uint64 {
	if e.CreateRevision != 0 {
		return e.CreateRevision
	}
	return rev
}
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return &snapshot{state: f.State()}, nil }
func (f *FSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var s State
	if err := json.NewDecoder(r).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = normalizeState(s)
	f.mu.Unlock()
	return nil
}

type snapshot struct{ state State }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(s.state)
	if err == nil {
		_, err = sink.Write(b)
	}
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}
func (*snapshot) Release()     {}
func cloneState(s State) State { return normalizeState(s) }
func normalizeState(s State) State {
	if s.Databases == nil {
		s.Databases = map[string]Database{}
	}
	for n, d := range s.Databases {
		if d.ACL == nil {
			d.ACL = map[string]Permission{}
		}
		if d.Data == nil {
			d.Data = map[string]Entry{}
		}
		for k, e := range d.Data {
			e.Value = append([]byte(nil), e.Value...)
			d.Data[k] = e
		}
		s.Databases[n] = d
	}
	return s
}
func Keys(d Database) []string {
	out := make([]string, 0, len(d.Data))
	for k := range d.Data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
