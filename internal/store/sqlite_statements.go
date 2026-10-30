package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const sqliteInsertEventSQL = `
INSERT INTO event_log(event_id, origin_node_id, value)
VALUES(?, ?, ?)
`

const sqliteUpsertOriginSQL = `
INSERT INTO origin_cursors(origin_node_id, applied_event_id, updated_at_hlc)
VALUES(?, ?, ?)
ON CONFLICT(origin_node_id) DO UPDATE SET
    applied_event_id = CASE
        WHEN excluded.applied_event_id > origin_cursors.applied_event_id THEN excluded.applied_event_id
        ELSE origin_cursors.applied_event_id
    END,
    updated_at_hlc = excluded.updated_at_hlc
`

const sqliteReadMessageCounterSQL = `
SELECT next_seq
FROM message_sequence_counters
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`

const sqliteSeedMessageCounterSQL = `
SELECT COALESCE(MAX(seq), 0) + 1
FROM messages
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`

const sqliteWriteMessageCounterSQL = `
INSERT INTO message_sequence_counters(user_node_id, user_id, node_id, next_seq)
VALUES(?, ?, ?, ?)
ON CONFLICT(user_node_id, user_id, node_id) DO UPDATE SET next_seq = excluded.next_seq
`

const sqliteInsertMessageSQL = `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
`

const sqliteMessageOverflowSQL = `
SELECT 1
FROM messages
WHERE user_node_id = ? AND user_id = ?
ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
LIMIT 1 OFFSET ?
`

const sqliteTrimMessagesSQL = `
DELETE FROM messages
WHERE user_node_id = ? AND user_id = ?
  AND (node_id, seq) IN (
    SELECT node_id, seq
    FROM messages
    WHERE user_node_id = ? AND user_id = ?
    ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
    LIMIT -1 OFFSET ?
  )
`

const sqliteRecordMessageTrimSQL = `
INSERT INTO message_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES('global', ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = message_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`

// sqliteStatements 只保存固定 SQL 的执行计划，不保存用户、序列号或窗口边界。
// Bind 在身份初始化事务提交后调用 prepareSQLiteStatements；事务内绝不 DB.Prepare，
// 否则单连接池会等待被当前事务占用的连接。Close 由后端先于 DB.Close 调用。
type sqliteStatements map[string]*sql.Stmt

func prepareSQLiteStatements(db *sql.DB) (sqliteStatements, error) {
	queries := []string{
		sqliteInsertEventSQL, sqliteUpsertOriginSQL,
		sqliteReadMessageCounterSQL, sqliteSeedMessageCounterSQL, sqliteWriteMessageCounterSQL,
		sqliteInsertMessageSQL, sqliteRecordMessageTrimSQL, sqliteMessageUserSQL,
		sqliteMessageOverflowSQL, sqliteTrimMessagesSQL,
		upsertDiscoveredPeerSQL,
	}
	statements := make(sqliteStatements, len(queries))
	for _, query := range queries {
		stmt, err := db.PrepareContext(context.Background(), query)
		if err != nil {
			_ = statements.close()
			return nil, fmt.Errorf("prepare sqlite message statements: %w", err)
		}
		statements[query] = stmt
	}
	return statements, nil
}

func (s sqliteStatements) close() error {
	var err error
	for _, stmt := range s {
		err = errors.Join(err, stmt.Close())
	}
	return err
}

func (s sqliteStatements) execTx(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	if stmt := s[query]; stmt != nil {
		txStmt := tx.StmtContext(ctx, stmt)
		defer txStmt.Close()
		return txStmt.ExecContext(ctx, args...)
	}
	return tx.ExecContext(ctx, query, args...)
}

func (s sqliteStatements) queryRowTx(ctx context.Context, tx *sql.Tx, query string, args ...any) *sql.Row {
	if stmt := s[query]; stmt != nil {
		// QueryRow 的 Scan 消费结果；事务会在 Commit/Rollback 时关闭派生包装，
		// DB 级 statement 则继续复用，直到后端 Close。
		return tx.StmtContext(ctx, stmt).QueryRowContext(ctx, args...)
	}
	return tx.QueryRowContext(ctx, query, args...)
}

// 固定语句版保留原计数器缺失时从投影播种的逻辑。
func (b *sqliteStoreBackend) nextPreparedMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	if err := key.Validate(); err != nil {
		return 0, err
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("%w: user id and node id are required for message sequence", ErrInvalidInput)
	}
	var seq int64
	err := b.statements.queryRowTx(ctx, tx, sqliteReadMessageCounterSQL, key.NodeID, key.UserID, nodeID).Scan(&seq)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if err := b.statements.queryRowTx(ctx, tx, sqliteSeedMessageCounterSQL, key.NodeID, key.UserID, nodeID).Scan(&seq); err != nil {
			return 0, fmt.Errorf("seed next message sequence: %w", err)
		}
	case err != nil:
		return 0, fmt.Errorf("read next message sequence: %w", err)
	}
	if _, err := b.statements.execTx(ctx, tx, sqliteWriteMessageCounterSQL, key.NodeID, key.UserID, nodeID, seq+1); err != nil {
		return 0, fmt.Errorf("store next message sequence: %w", err)
	}
	return seq, nil
}
