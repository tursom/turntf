package cluster

import (
	"context"

	"github.com/tursom/turntf/internal/store"
)

// meshGenerationPersistence 通过Store持久化网格拓扑代次计数器。
// 实现mesh.GenerationPersistence接口。
type meshGenerationPersistence struct {
	store *store.Store
}

// newMeshGenerationPersistence 创建一个新的持久化实例。
func newMeshGenerationPersistence(st *store.Store) *meshGenerationPersistence {
	if st == nil {
		return nil
	}
	return &meshGenerationPersistence{store: st}
}

// Load 从存储中加载拓扑代次。
func (p *meshGenerationPersistence) Load() (uint64, error) {
	if p == nil || p.store == nil {
		return 0, nil
	}
	return p.store.LoadMeshTopologyGeneration(context.Background())
}

// Store 将拓扑代次持久化到存储。
func (p *meshGenerationPersistence) Store(generation uint64) error {
	if p == nil || p.store == nil {
		return nil
	}
	return p.store.StoreMeshTopologyGeneration(context.Background(), generation)
}
