package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// Exercise real elections, durable replication and a lagging follower restart.
// The mixed case rejects binary at a JSON-only endpoint; the binary case trims
// logs while the follower is offline, forcing actual snapshot body restoration.
func TestConsensusRaftReplicationAndRestart(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixed=%v", mixed), func(t *testing.T) {
			var mu sync.RWMutex
			transports := map[raft.ServerAddress]*Transport{}
			nodes := map[raft.ServerAddress]*PersistentNode{}
			dir := t.TempDir()
			var snapshots atomic.Int64
			start := func(id raft.ServerAddress) {
				tr := NewTransport(id, nil, WithConsensusSender(func(ctx context.Context, target raft.ServerAddress, p []byte) error {
					if mixed && target == "3" && !json.Valid(p) {
						return fmt.Errorf("legacy endpoint received binary")
					}
					if err := ctx.Err(); err != nil {
						return err
					}
					w, err := decodeRPC(p)
					if err != nil {
						return err
					}
					if w.Type == "snapshot" && !w.response {
						snapshots.Add(1)
					}
					mu.RLock()
					peer := transports[target]
					mu.RUnlock()
					if peer == nil {
						return fmt.Errorf("peer offline")
					}
					return peer.DeliverFrom(id, p)
				}), WithBinaryConsensus(func(target raft.ServerAddress) bool { return !mixed || (id != "3" && target != "3") }))
				n, err := NewPersistentNode(NodeOptions{ID: raft.ServerID(id), Address: id, Transport: tr}, filepath.Join(dir, string(id)))
				if err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				transports[id] = tr
				mu.Unlock()
				nodes[id] = n
			}
			stop := func(id raft.ServerAddress) {
				mu.Lock()
				tr := transports[id]
				delete(transports, id)
				mu.Unlock()
				if n := nodes[id]; n != nil {
					_ = n.Close()
					delete(nodes, id)
				}
				if tr != nil {
					_ = tr.Close()
				}
			}
			t.Cleanup(func() {
				for _, id := range []raft.ServerAddress{"1", "2", "3"} {
					stop(id)
				}
			})
			for _, id := range []raft.ServerAddress{"1", "2", "3"} {
				start(id)
			}
			if err := nodes["1"].Bootstrap([]raft.Server{{ID: "1", Address: "1"}, {ID: "2", Address: "2"}, {ID: "3", Address: "3"}}); err != nil {
				t.Fatal(err)
			}
			wait := func(label string, f func() bool) {
				t.Helper()
				deadline := time.Now().Add(12 * time.Second)
				for time.Now().Before(deadline) {
					if f() {
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatal("timeout: " + label)
			}
			var leader raft.ServerAddress
			wait("election", func() bool {
				for id, n := range nodes {
					if n.Raft().State() == raft.Leader {
						leader = id
						return true
					}
				}
				return false
			})
			if _, err := nodes[leader].Apply(context.Background(), Command{Op: "create_database", Database: "test", Owner: "owner"}); err != nil {
				t.Fatal(err)
			}
			var follower raft.ServerAddress
			for _, id := range []raft.ServerAddress{"1", "2", "3"} {
				if id != leader {
					follower = id
					break
				}
			}
			wait("initial replication", func() bool { return nodes[follower].FSM().Can("test", "owner", PermissionRead) })
			stop(follower)
			for i := 0; i < 5; i++ {
				if _, err := nodes[leader].Apply(context.Background(), Command{Op: "put", Database: "test", Key: "key", Value: []byte(fmt.Sprint(i))}); err != nil {
					t.Fatal(err)
				}
			}
			if !mixed {
				cfg := nodes[leader].Raft().ReloadableConfig()
				cfg.TrailingLogs = 0
				if err := nodes[leader].Raft().ReloadConfig(cfg); err != nil {
					t.Fatal(err)
				}
				if err := nodes[leader].Raft().Snapshot().Error(); err != nil {
					t.Fatal(err)
				}
			}
			start(follower)
			wait("follower catches up", func() bool {
				entry, _, err := nodes[follower].FSM().Get("test", "key")
				return err == nil && string(entry.Value) == "4"
			})
			if !mixed && snapshots.Load() == 0 {
				t.Fatal("did not exercise snapshot transfer")
			}
		})
	}
}
