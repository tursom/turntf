package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiscoveredPeerStatementSingleConnectionLifecycle(t *testing.T) {
	for _, engine := range []string{EngineSQLite, EnginePebble} {
		t.Run(engine, func(t *testing.T) {
			dir := t.TempDir()
			st, err := Open(filepath.Join(dir, "metadata.db"), Options{Engine: engine, NodeID: testNodeID(1), PebblePath: filepath.Join(dir, "messages")})
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = st.Close()
				}
			}()
			st.db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := st.Init(ctx); err != nil {
				t.Fatal(err)
			}
			var stmt *sql.Stmt
			switch backend := st.backend.(type) {
			case *sqliteStoreBackend:
				stmt = backend.statements[upsertDiscoveredPeerSQL]
			case *pebbleStoreBackend:
				stmt = backend.discoveryStatement
			}
			if stmt == nil {
				t.Fatal("metadata upsert was not prepared")
			}
			peer := DiscoveredPeer{NodeID: testNodeID(2), URL: "ws://peer.example/internal/cluster/ws", State: "connected"}
			if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
				t.Fatal(err)
			}
			peer.LastError = "changed"
			if err := st.UpsertDiscoveredPeer(ctx, peer); err != nil {
				t.Fatal(err)
			}
			err = st.Close()
			closed = true
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stmt.ExecContext(ctx); err == nil || !strings.Contains(err.Error(), "statement is closed") {
				t.Fatalf("backend did not release metadata statement: %v", err)
			}
		})
	}
}
