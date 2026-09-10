package cluster

import (
	"context"
	"sync"
)

// transportWriteMutex separates waiting for the shared writer from owning I/O.
// A canceled waiter must neither block behind a slow frame nor close its stream.
// The zero value is ready for use, including transport wrappers built by tests.
type transportWriteMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *transportWriteMutex) LockContext(ctx context.Context) error {
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	select {
	case m.token <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *transportWriteMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *transportWriteMutex) Unlock() { <-m.token }
