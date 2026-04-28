package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

func (s *Store) Init(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    node_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    profile TEXT NOT NULL DEFAULT '{}',
    role TEXT NOT NULL DEFAULT 'user',
    system_reserved INTEGER NOT NULL DEFAULT 0,
    created_at_hlc TEXT NOT NULL,
    updated_at_hlc TEXT NOT NULL,
    deleted_at_hlc TEXT,
    version_username TEXT NOT NULL,
    version_password_hash TEXT NOT NULL,
    version_profile TEXT NOT NULL,
    version_role TEXT NOT NULL,
    version_deleted TEXT,
    origin_node_id INTEGER NOT NULL,
    PRIMARY KEY(node_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_users_username_deleted ON users(username, deleted_at_hlc);

CREATE TABLE IF NOT EXISTS messages (
    user_node_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    sender_node_id INTEGER NOT NULL,
    sender_user_id INTEGER NOT NULL,
    body BLOB NOT NULL,
    created_at_hlc TEXT NOT NULL,
    FOREIGN KEY(user_node_id, user_id) REFERENCES users(node_id, user_id),
    PRIMARY KEY(user_node_id, user_id, node_id, seq)
);

CREATE TABLE IF NOT EXISTS user_attachments (
    owner_node_id INTEGER NOT NULL,
    owner_user_id INTEGER NOT NULL,
    subject_node_id INTEGER NOT NULL,
    subject_user_id INTEGER NOT NULL,
    attachment_type TEXT NOT NULL,
    config_json TEXT NOT NULL DEFAULT '{}',
    attached_at_hlc TEXT NOT NULL,
    deleted_at_hlc TEXT,
    origin_node_id INTEGER NOT NULL,
    FOREIGN KEY(owner_node_id, owner_user_id) REFERENCES users(node_id, user_id),
    FOREIGN KEY(subject_node_id, subject_user_id) REFERENCES users(node_id, user_id),
    PRIMARY KEY(owner_node_id, owner_user_id, subject_node_id, subject_user_id, attachment_type)
);

CREATE TABLE IF NOT EXISTS event_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    origin_node_id INTEGER NOT NULL,
    value BLOB NOT NULL,
    UNIQUE(origin_node_id, event_id)
);

CREATE TABLE IF NOT EXISTS peer_ack_cursors (
    peer_node_id INTEGER NOT NULL,
    origin_node_id INTEGER NOT NULL,
    acked_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at_hlc TEXT NOT NULL,
    PRIMARY KEY(peer_node_id, origin_node_id)
);

CREATE TABLE IF NOT EXISTS origin_cursors (
    origin_node_id INTEGER PRIMARY KEY,
    applied_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at_hlc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_events (
    event_id INTEGER NOT NULL,
    source_node_id INTEGER NOT NULL,
    applied_at_hlc TEXT NOT NULL,
    PRIMARY KEY(source_node_id, event_id)
);

CREATE TABLE IF NOT EXISTS event_log_truncation_meta (
    origin_node_id INTEGER PRIMARY KEY,
    truncated_before_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at_hlc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loser_node_id INTEGER NOT NULL,
    loser_user_id INTEGER NOT NULL,
    winner_node_id INTEGER NOT NULL,
    winner_user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    detected_at_hlc TEXT NOT NULL,
    resolved_by_origin_node_id INTEGER,
    resolved_by_event_id INTEGER
);

CREATE TABLE IF NOT EXISTS tombstones (
    entity_type TEXT NOT NULL,
    entity_node_id INTEGER NOT NULL,
    entity_id INTEGER NOT NULL,
    deleted_at_hlc TEXT NOT NULL,
    expires_at_hlc TEXT,
    origin_node_id INTEGER NOT NULL,
    PRIMARY KEY(entity_type, entity_node_id, entity_id)
);

CREATE TABLE IF NOT EXISTS message_trim_stats (
    scope TEXT PRIMARY KEY,
    trimmed_total INTEGER NOT NULL DEFAULT 0,
    last_trimmed_at_hlc TEXT
);

CREATE TABLE IF NOT EXISTS event_log_trim_stats (
    scope TEXT PRIMARY KEY,
    trimmed_total INTEGER NOT NULL DEFAULT 0,
    last_trimmed_at_hlc TEXT
);

CREATE TABLE IF NOT EXISTS message_sequence_counters (
    user_node_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    next_seq INTEGER NOT NULL,
    PRIMARY KEY(user_node_id, user_id, node_id)
);

CREATE TABLE IF NOT EXISTS pending_projections (
    origin_node_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_node_id INTEGER NOT NULL,
    aggregate_id INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL,
    first_failed_at_hlc TEXT NOT NULL,
    last_failed_at_hlc TEXT NOT NULL,
    PRIMARY KEY(origin_node_id, event_id)
);

CREATE TABLE IF NOT EXISTS discovered_peers (
    node_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    zeromq_curve_server_public_key TEXT NOT NULL DEFAULT '',
    source_peer_node_id INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    first_seen_at_hlc TEXT NOT NULL,
    last_seen_at_hlc TEXT NOT NULL,
    last_connected_at_hlc TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    generation INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(node_id, url)
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
CREATE INDEX IF NOT EXISTS idx_messages_user_created ON messages(user_node_id, user_id, created_at_hlc DESC, node_id ASC, seq DESC);
CREATE INDEX IF NOT EXISTS idx_messages_node ON messages(node_id, user_node_id, user_id, seq);
CREATE INDEX IF NOT EXISTS idx_user_attachments_owner_type ON user_attachments(owner_node_id, owner_user_id, attachment_type, deleted_at_hlc);
CREATE INDEX IF NOT EXISTS idx_user_attachments_subject_type ON user_attachments(subject_node_id, subject_user_id, attachment_type, deleted_at_hlc);
CREATE INDEX IF NOT EXISTS idx_event_log_origin_event ON event_log(origin_node_id, event_id);
CREATE INDEX IF NOT EXISTS idx_pending_projections_failed ON pending_projections(last_failed_at_hlc, origin_node_id, event_id);
CREATE INDEX IF NOT EXISTS idx_discovered_peers_url ON discovered_peers(url);
CREATE INDEX IF NOT EXISTS idx_discovered_peers_state ON discovered_peers(state, last_seen_at_hlc);
`); err != nil {
		return fmt.Errorf("init message indexes: %w", err)
	}

	if err := s.validateSchemaVersion(ctx); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, `
INSERT INTO schema_meta(key, value)
VALUES('schema_version', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, defaultSchemaVersion); err != nil {
		return fmt.Errorf("store schema version: %w", err)
	}
	if err := s.ensureNodeIdentity(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureNodeIdentity(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin node identity init: %w", err)
	}
	defer tx.Rollback()

	nodeID, err := readNodeIDTx(ctx, tx)
	if err != nil {
		return err
	}
	if nodeID == 0 {
		if s.initialNodeID > 0 {
			nodeID = s.initialNodeID
		} else {
			nodeID, err = clock.GenerateNodeID()
			if err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO schema_meta(key, value)
VALUES(?, ?)
`, schemaMetaNodeIDKey, strconv.FormatInt(nodeID, 10)); err != nil {
			return fmt.Errorf("store node id: %w", err)
		}
	} else if s.initialNodeID > 0 && nodeID != s.initialNodeID {
		return fmt.Errorf("node id %d does not match existing node id %d", s.initialNodeID, nodeID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit node identity init: %w", err)
	}

	s.nodeID = nodeID
	s.ids = clock.NewIDGenerator()
	if s.clock == nil {
		s.clock = clock.NewClock(nodeID)
	}
	s.messageTrim = &sqliteMessageTrimRepository{db: s.db, clock: s.clock}
	if s.backend == nil {
		return fmt.Errorf("store backend is not initialized")
	}
	if err := s.backend.Bind(storeBackendBindings{
		NodeID:            s.nodeID,
		Clock:             s.clock,
		IDs:               s.ids,
		MessageWindowSize: s.messageWindowSize,
		UserRepository:    s.userRepository,
		Subscriptions:     s.subscriptions,
		Blacklists:        s.blacklists,
		MessageTrim:       s.messageTrim,
	}); err != nil {
		return fmt.Errorf("bind %s backend: %w", s.backend.Name(), err)
	}
	return nil
}

func (s *Store) validateSchemaVersion(ctx context.Context) error {
	var raw string
	err := s.db.QueryRowContext(ctx, `
SELECT value
FROM schema_meta
WHERE key = 'schema_version'
`).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("read schema version: %w", err)
	}

	version := strings.TrimSpace(raw)
	if version == "" || version == defaultSchemaVersion {
		return nil
	}
	return fmt.Errorf("unsupported schema version %q: rebuild the database with schema version %s", version, defaultSchemaVersion)
}

func readNodeIDTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `
SELECT value
FROM schema_meta
WHERE key = ?
`, schemaMetaNodeIDKey).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read node id: %w", err)
	}

	nodeID, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse node id %q: %w", raw, err)
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("node id must be positive")
	}
	return nodeID, nil
}
