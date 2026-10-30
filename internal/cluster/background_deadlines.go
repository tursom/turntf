package cluster

import (
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// armBackgroundTimer 的零 deadline 表示不再调度。只有循环本身能操作 timer，
// 外部状态变化通过容量为 1 的通知通道合并；兼容旧式缓冲 timer 的残留通知。
func armBackgroundTimer(timer *time.Timer, deadline time.Time) <-chan time.Time {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	if deadline.IsZero() {
		return nil
	}
	timer.Reset(time.Until(deadline))
	return timer.C
}

// signalSnapshotWork 不获取 manager/session 锁，允许状态修改者持锁调用。
// 通知只提示重新检查真实状态；合并通知不会丢失新的 deadline 或脏标记。
func (m *Manager) signalSnapshotWork() {
	if m == nil {
		return
	}
	select {
	case m.snapshotDigestWake <- struct{}{}:
	default:
	}
}

func (s *session) signalSnapshotWork() {
	if s != nil && s.manager != nil {
		s.manager.signalSnapshotWork()
	}
}

// nextSnapshotWorkDeadline 只调度当前存在的工作。被 pull 阻塞的摘要由
// pull 完成/取消唤醒；未就绪和不兼容的会话等待其状态变化，不启动轮询。
func (m *Manager) nextSnapshotWorkDeadline(now time.Time) time.Time {
	if m == nil || m.store == nil {
		return time.Time{}
	}
	type candidate struct {
		sess      *session
		dirty     bool
		immediate bool
		lastSent  time.Time
		lastQueue time.Time
	}
	m.mu.Lock()
	candidates := make([]candidate, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer != nil && peer.active != nil && m.allowSnapshotTrafficForSessionLocked(peer.active) == nil {
			candidates = append(candidates, candidate{peer.active, peer.snapshotDigestDirty,
				peer.snapshotDigestImmediate, peer.lastSnapshotDigestAt, peer.lastSnapshotDigestQueuedAt})
		}
	}
	m.mu.Unlock()
	var next time.Time
	consider := func(deadline time.Time) {
		// 已过期但本轮无法处理（如时钟保护）的工作沿用原来的最小重试间隔，
		// 防止立即重置 timer 形成忙循环。正常请求在 flush 中已推进到下次期限。
		if !deadline.After(now) {
			deadline = now.Add(snapshotDigestSweepInterval)
		}
		if next.IsZero() || deadline.Before(next) {
			next = deadline
		}
	}
	for _, item := range candidates {
		s := item.sess
		s.mu.Lock()
		if s.closed || !s.replicationReady || s.remoteSnapshotVersion != internalproto.SnapshotVersion {
			s.mu.Unlock()
			continue
		}
		for _, request := range s.pendingSnapshotParts {
			if request != nil {
				consider(request.retryAt)
			}
		}
		if item.dirty && len(s.pendingPulls) == 0 && len(s.pendingSnapshotParts) == 0 {
			deadline := now
			if !item.immediate && !item.lastSent.IsZero() {
				deadline = item.lastSent.Add(snapshotDigestMinInterval)
			}
			if queued := item.lastQueue.Add(snapshotDigestSweepInterval); !item.lastQueue.IsZero() && queued.After(deadline) {
				deadline = queued
			}
			consider(deadline)
		}
		s.mu.Unlock()
	}
	return next
}
