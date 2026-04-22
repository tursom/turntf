package cluster

import (
	"context"

	"github.com/tursom/turntf/internal/store"
)

type meshGenerationPersistence struct {
	store *store.Store
}

func newMeshGenerationPersistence(st *store.Store) *meshGenerationPersistence {
	if st == nil {
		return nil
	}
	return &meshGenerationPersistence{store: st}
}

func (p *meshGenerationPersistence) Load() (uint64, error) {
	if p == nil || p.store == nil {
		return 0, nil
	}
	return p.store.LoadMeshTopologyGeneration(context.Background())
}

func (p *meshGenerationPersistence) Store(generation uint64) error {
	if p == nil || p.store == nil {
		return nil
	}
	return p.store.StoreMeshTopologyGeneration(context.Background(), generation)
}
