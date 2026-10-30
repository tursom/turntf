package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/tursom/turntf/internal/clock"
)

// 不加载用户名、密码和 profile。保留 HLC 和整数扫描校验，避免把原读取错误
// 变成成功写入；未删除条件与原 GetUserTx(false) 相同，且每次都在当前事务读取。
const sqliteMessageUserSQL = `
SELECT role, system_reserved, origin_node_id,
       created_at_hlc, updated_at_hlc, version_username, version_password_hash,
       version_profile, version_role, version_deleted
FROM users
WHERE node_id = ? AND user_id = ? AND deleted_at_hlc IS NULL
`

func (b *sqliteStoreBackend) messageUserRoleTx(ctx context.Context, tx *sql.Tx, key UserKey) (string, error) {
	if err := key.Validate(); err != nil {
		return "", err
	}
	var role string
	var reserved, origin int64
	var raw [6]string
	var deletedVersion sql.NullString
	err := b.statements.queryRowTx(ctx, tx, sqliteMessageUserSQL, key.NodeID, key.UserID).Scan(
		&role, &reserved, &origin, &raw[0], &raw[1], &raw[2], &raw[3], &raw[4], &raw[5], &deletedVersion,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	fields := [...]string{"created_at", "updated_at", "version_username", "version_password_hash", "version_profile", "version_role"}
	for i, value := range raw {
		if err := validateSQLiteUserTimestamp(value, fields[i]); err != nil {
			return "", err
		}
	}
	if deletedVersion.Valid {
		if err := validateSQLiteUserTimestamp(deletedVersion.String, "version_deleted"); err != nil {
			return "", err
		}
	}
	// 仅供本次消息权限判断，不构造 User，也不进入 UserRepository 缓存。
	return role, nil
}

func validateSQLiteUserTimestamp(raw, field string) error {
	if canonicalSQLiteUserTimestamp(raw) {
		return nil
	}
	// 原解析器还接受带正号的非规范数字；必须回退而非拒绝，错误文本也保持一致。
	if _, err := clock.ParseTimestamp(raw); err != nil {
		return fmt.Errorf("parse user %s: %w", field, err)
	}
	return nil
}

// 此快速路径只接受 ParseTimestamp 的规范十进制子集，不构造 Timestamp 或拆分字符串。
func canonicalSQLiteUserTimestamp(raw string) bool {
	if len(raw) != 39 || raw[13] != '-' || raw[19] != '-' {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if i != 13 && i != 19 && (raw[i] < '0' || raw[i] > '9') {
			return false
		}
	}
	return raw[14:19] <= "65535" && raw[20:] > "0000000000000000000" && raw[20:] <= "9223372036854775807"
}
