package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/store"
)

type sessionLookupKey struct {
	nodeID       int64
	user         store.UserKey
	runtimeEpoch uint64
}

type sessionLookupFlight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	result  []store.OnlineSession
	err     error
}

// 只合并在途查询，不缓存结果。每个调用者独立取消；最后一个等待者
// 离开时取消网格查询，避免无人等待的请求继续占据拥塞连接或 pending 表。
func (m *Manager) sharedSessionLookup(ctx context.Context, nodeID int64, user store.UserKey) ([]store.OnlineSession, error) {
	m.mu.Lock()
	// 重启后的新查询不能加入仍等待旧进程回复的 flight。
	key := sessionLookupKey{nodeID: nodeID, user: user, runtimeEpoch: m.currentRuntimeEpochForNodeLocked(nodeID)}
	if m.ctx == nil {
		m.mu.Unlock()
		return m.resolveUserSessionsAtNodeUnshared(ctx, nodeID, user)
	}
	if err := m.ctx.Err(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if m.sessionLookups == nil {
		m.sessionLookups = make(map[sessionLookupKey]*sessionLookupFlight)
	}
	flight := m.sessionLookups[key]
	if flight == nil {
		queryCtx, cancel := context.WithTimeout(m.ctx, queryLoggedInUsersTimeout)
		flight = &sessionLookupFlight{done: make(chan struct{}), cancel: cancel}
		m.sessionLookups[key] = flight
		// Close 取消 m.ctx 后会取得同一把锁再 Wait；不能在其后新增工作。
		m.wg.Add(1)
		go func(f *sessionLookupFlight) {
			defer m.wg.Done()
			defer cancel()
			result, err := m.resolveUserSessionsAtNodeUnshared(queryCtx, nodeID, user)
			m.mu.Lock()
			f.result, f.err = result, err
			if m.sessionLookups[key] == f {
				delete(m.sessionLookups, key)
			}
			close(f.done)
			m.mu.Unlock()
		}(flight)
	}
	flight.waiters++
	m.mu.Unlock()
	defer func() {
		last := false
		m.mu.Lock()
		flight.waiters--
		if flight.waiters == 0 && m.sessionLookups[key] == flight {
			delete(m.sessionLookups, key)
			flight.cancel()
			last = true
		}
		m.mu.Unlock()
		if last {
			// 与原查询一样，在返回前移除 pending request；晚到回复不能
			// 认领无人等待的查询，也不能删除随后创建的新一轮 flight。
			<-flight.done
		}
	}()
	select {
	case <-ctx.Done():
		return nil, sessionLookupCallerError(ctx.Err(), nodeID)
	case <-flight.done:
		if err := ctx.Err(); err != nil {
			return nil, sessionLookupCallerError(err, nodeID)
		}
		return append([]store.OnlineSession(nil), flight.result...), flight.err
	}
}

func sessionLookupCallerError(err error, nodeID int64) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: timed out resolving user sessions on node %d", app.ErrServiceUnavailable, nodeID)
	}
	return err
}
