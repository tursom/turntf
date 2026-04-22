package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

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
