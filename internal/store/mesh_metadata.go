package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// LoadMeshTopologyGeneration 从 schema_meta 表中读取当前 mesh 拓扑代际计数器。
//
// mesh 拓扑代际（generation）是一个单调递增的版本号，每次 mesh 拓扑发生变更时
// （如节点加入或离开）递增。该机制用于：
//   - 检测节点持有的拓扑信息是否过期
//   - 触发拓扑同步流程，更新节点间的连接信息
//
// 若 Store 或 db 为 nil，返回 0（用于未初始化状态下的安全调用）。
// 若 schema_meta 表中没有对应的 key 或值为空，也返回 0（首次启动）。
func (s *Store) LoadMeshTopologyGeneration(ctx context.Context) (uint64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	var raw string
	err := s.db.QueryRowContext(ctx, `
SELECT value
FROM schema_meta
WHERE key = ?
`, schemaMetaMeshTopologyGenerationKey).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read mesh topology generation: %w", err)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	generation, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse mesh topology generation %q: %w", raw, err)
	}
	return generation, nil
}

// StoreMeshTopologyGeneration 将 mesh 拓扑代际计数器写入 schema_meta 表。
// 使用 UPSERT（ON CONFLICT DO UPDATE）语义，key 重复时直接覆盖 value。
// 调用方负责确保 generation 单调递增（本方法不校验新旧值大小）。
// 一般由 mesh 管理模块在检测到拓扑变更后调用。
// 若 Store 或 db 为 nil，静默返回 nil（用于未初始化状态的安全调用）。
func (s *Store) StoreMeshTopologyGeneration(ctx context.Context, generation uint64) error {
	if s == nil || s.db == nil {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO schema_meta(key, value)
VALUES(?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, schemaMetaMeshTopologyGenerationKey, strconv.FormatUint(generation, 10)); err != nil {
		return fmt.Errorf("store mesh topology generation: %w", err)
	}
	return nil
}
