package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

// handleTimeSyncRequest 处理来自对等节点的时间同步请求。
// 记录请求接收时间和响应发送时间（NTP协议的T2和T3），立即回复响应。
func (m *Manager) handleTimeSyncRequest(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	req := envelope.GetTimeSyncRequest()
	if req == nil {
		return errors.New("time sync request body cannot be empty")
	}
	if req.RequestId == 0 {
		return errors.New("time sync request id cannot be empty")
	}

	// T2: 服务器接收时间 (ServerReceiveTime)
	receivedAtMs := m.clock.PhysicalTimeMs()
	// T3: 服务器发送时间 (ServerSendTime)
	sentAtMs := m.clock.PhysicalTimeMs()
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_TimeSyncResponse{
			TimeSyncResponse: &internalproto.TimeSyncResponse{
				RequestId:           req.RequestId,
				ClientSendTimeMs:    req.ClientSendTimeMs,
				ServerReceiveTimeMs: receivedAtMs,
				ServerSendTimeMs:    sentAtMs,
			},
		},
	})
	return nil
}

// handleTimeSyncResponse 处理时间同步响应。
// 取出对应的等待通道并发送结果。
func (m *Manager) handleTimeSyncResponse(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	resp := envelope.GetTimeSyncResponse()
	if resp == nil {
		return errors.New("time sync response body cannot be empty")
	}
	if resp.RequestId == 0 {
		return errors.New("time sync response id cannot be empty")
	}

	if !sess.resolveTimeSync(resp.RequestId, timeSyncResult{
		response:     resp,
		receivedAtMs: m.clock.PhysicalTimeMs(),
	}) {
		return nil
	}
	return nil
}

// performTimeSync 执行一次完整的时间同步流程。
// 收集样本（默认7个），选择RTT最小的样本，更新时钟状态。
func (m *Manager) performTimeSync(sess *session) error {
	var (
		best timeSyncSample
		err  error
	)
	if m.timeSyncer != nil {
		best, err = m.timeSyncer(sess)
	} else {
		best, err = m.collectTimeSyncSample(sess)
	}
	if err != nil {
		state, reason := m.recordTimeSyncFailure(sess, err)
		if state == clockStateRejected {
			return fmt.Errorf("peer %d clock rejected after time sync failure: %s", sess.peerID, reason)
		}
		return nil
	}

	sess.setClockOffset(best.offsetMs)
	sess.observeRTT(best.rttMs)
	state, reason := m.recordTimeSyncSample(sess, best)
	if state == clockStateRejected {
		return fmt.Errorf("peer %d clock rejected after time sync sample: %s", sess.peerID, reason)
	}
	return nil
}

// collectTimeSyncSample 执行多次时间同步往返（默认7次），
// 选择RTT最小的样本作为最佳结果，并计算抖动和可信度。
//
// 不确定性计算：max(RTT/2, Jitter/2) + 50ms
// 这提供了对时钟偏移的保守误差估计。
func (m *Manager) collectTimeSyncSample(sess *session) (timeSyncSample, error) {
	var (
		best      timeSyncSample
		found     bool
		lastErr   error
		minRTT    int64
		maxRTT    int64
		haveRange bool
	)

	for range timeSyncSampleCount {
		sample, err := m.timeSyncRoundTrip(sess)
		if err != nil {
			lastErr = err
			continue
		}
		if !haveRange {
			minRTT = sample.rttMs
			maxRTT = sample.rttMs
			haveRange = true
		} else {
			if sample.rttMs < minRTT {
				minRTT = sample.rttMs
			}
			if sample.rttMs > maxRTT {
				maxRTT = sample.rttMs
			}
		}
		// 选择RTT最小的样本作为最佳样本
		if !found || sample.rttMs < best.rttMs {
			best = sample
			found = true
		}
	}
	if found {
		jitterMs := maxRTT - minRTT
		best.uncertaintyMs = maxInt64(best.rttMs/2, jitterMs/2) + 50
		best.credible = best.rttMs <= m.cfg.ClockCredibleRttMs
		if best.sampledAt.IsZero() {
			best.sampledAt = time.Now().UTC()
		}
		return best, nil
	}
	if lastErr == nil {
		lastErr = errors.New("time sync failed without successful samples")
	}
	return timeSyncSample{}, lastErr
}

// timeSyncRoundTrip 执行单次NTP风格的4时间戳往返测量。
//
// 时间戳定义：
//
//	T1 (ClientSendTime):    客户端发出请求的物理时间
//	T2 (ServerReceiveTime): 服务器收到请求的物理时间
//	T3 (ServerSendTime):    服务器发出响应的物理时间
//	T4 (ClientReceiveTime): 客户端收到响应的物理时间
//
// 计算公式：
//
//	时钟偏移 = ((T2 - T1) + (T3 - T4)) / 2
//	RTT      = (T4 - T1) - (T3 - T2)
//
// 该公式假设往返的网络延迟是对称的。如果RTT过大，
// 则样本被标记为不可信。
func (m *Manager) timeSyncRoundTrip(sess *session) (timeSyncSample, error) {
	requestID, resultCh := sess.beginTimeSync()
	// T1: 客户端发送时间
	clientSendTimeMs := m.clock.PhysicalTimeMs()
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_TimeSyncRequest{
			TimeSyncRequest: &internalproto.TimeSyncRequest{
				RequestId:        requestID,
				ClientSendTimeMs: clientSendTimeMs,
			},
		},
	})

	timer := time.NewTimer(m.clockSyncTimeout())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		sess.cancelTimeSync(requestID, context.Canceled)
		return timeSyncSample{}, ctx.Err()
	case result := <-resultCh:
		if result.err != nil {
			return timeSyncSample{}, result.err
		}
		if result.response == nil {
			return timeSyncSample{}, errors.New("time sync response was empty")
		}

		// T2和T3来自服务器的响应
		serverReceiveMs := result.response.ServerReceiveTimeMs
		serverSendMs := result.response.ServerSendTimeMs
		// T4: 客户端接收时间
		clientReceiveMs := result.receivedAtMs
		// 偏移 = ((T2 - T1) + (T3 - T4)) / 2
		offsetMs := ((serverReceiveMs - clientSendTimeMs) + (serverSendMs - clientReceiveMs)) / 2
		// RTT = (T4 - T1) - (T3 - T2)
		rttMs := clientReceiveMs - clientSendTimeMs - (serverSendMs - serverReceiveMs)
		if rttMs < 0 {
			rttMs = 0
		}
		return timeSyncSample{
			offsetMs:  offsetMs,
			rttMs:     rttMs,
			sampledAt: time.Now().UTC(),
		}, nil
	case <-timer.C:
		sess.cancelTimeSync(requestID, context.DeadlineExceeded)
		return timeSyncSample{}, fmt.Errorf("time sync with peer %d timed out", sess.peerID)
	}
}

// sessionSyncLoop 是每个会话的后台同步循环。
// 定期执行时间同步、数据追赶和反熵检查。
func (m *Manager) sessionSyncLoop(sess *session) {
	if m.ctx == nil {
		return
	}
	timeSyncTicker := time.NewTicker(timeSyncInterval)
	defer timeSyncTicker.Stop()
	catchupTicker := time.NewTicker(catchupRetryInterval)
	defer catchupTicker.Stop()
	antiEntropyTicker := time.NewTicker(antiEntropyInterval)
	defer antiEntropyTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timeSyncTicker.C:
			if sess.isClosed() {
				return
			}
			if err := m.performTimeSync(sess); err != nil {
				m.logSessionWarn("periodic_time_sync_failed", sess, err).
					Msg("periodic time sync failed")
				sess.close()
				return
			}
			state, reason := m.peerClockState(sess.peerID)
			eventName := "periodic_time_sync_succeeded"
			logger := m.logSessionDebug(eventName, sess)
			if state != string(clockStateTrusted) {
				eventName = "periodic_time_sync_observing"
				logger = m.logSessionWarn(eventName, sess, nil)
			}
			logger.
				Str("clock_state", state).
				Str("clock_reason", reason).
				Int64("offset_ms", sess.clockOffset()).
				Int64("rtt_ms", sess.smoothedRTTMs).
				Msg("periodic time sync completed")
		case <-catchupTicker.C:
			if sess.isClosed() {
				return
			}
			if _, err := m.requestCatchupIfNeeded(sess); err != nil {
				m.logSessionWarn("periodic_catchup_failed", sess, err).
					Msg("periodic catchup failed")
				sess.close()
				return
			}
		case <-antiEntropyTicker.C:
			if sess.isClosed() {
				return
			}
			m.markSnapshotDigestDirty(sess.peerID, false)
		}
	}
}

// markPeerClockSynced 将一个对等节点标记为已同步（用于外部提供的时钟信息）。
func (m *Manager) markPeerClockSynced(sess *session, offsetMs int64) {
	_, _ = m.recordTimeSyncSample(sess, timeSyncSample{
		offsetMs:      offsetMs,
		rttMs:         maxInt64(sess.smoothedRTTMs, 1),
		uncertaintyMs: 50,
		sampledAt:     time.Now().UTC(),
		credible:      true,
	})
}

// clearPeerClockSync 清除对等节点的时钟信任状态。
func (m *Manager) clearPeerClockSync(sess *session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return
	}
	cleared := false
	if peer.trustedSession == sess {
		peer.trustedSession = nil
		peer.clockState = clockStateObserving
		cleared = true
	}
	m.recomputeClockOffsetLocked()
	m.refreshNodeClockStateLocked()
	if cleared {
		m.logSessionEvent("peer_clock_untrusted", sess).
			Msg("peer clock is no longer trusted")
	}
}

// recomputeClockOffsetLocked 使用所有可信对等节点的中位数偏移重新计算时钟偏移。
// 使用中位数而非平均值可以抵抗偏差异常值。
func (m *Manager) recomputeClockOffsetLocked() {
	offsets := make([]int64, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer.trustedSession != nil {
			offsets = append(offsets, peer.clockOffsetMs)
		}
	}
	if len(offsets) == 0 {
		m.clock.SetOffsetMs(0)
		return
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	// 取中位数作为全局时钟偏移
	m.clock.SetOffsetMs(offsets[len(offsets)/2])
}

// validateBatchHLC 验证事件批次中的HLC（混合逻辑时钟）时间戳。
// 检查批次sent_at_hlc和每个事件的hlc是否超出本地物理时间+最大偏差。
// 同时确保批次时间戳不早于批次中任意事件的时间戳。
func (m *Manager) validateBatchHLC(sentAtRaw string, events []*internalproto.ReplicatedEvent) error {
	if m.cfg.MaxClockSkewMs == 0 {
		return nil
	}
	maxAllowedWallTime := m.clock.WallTimeMs() + m.cfg.MaxClockSkewMs
	sentAt, err := clock.ParseTimestamp(strings.TrimSpace(sentAtRaw))
	if err != nil {
		return fmt.Errorf("parse event batch sent_at_hlc: %w", err)
	}
	if sentAt.WallTimeMs > maxAllowedWallTime {
		return fmt.Errorf("event batch sent_at_hlc %s exceeds local wall time %d by more than %dms", sentAt, m.clock.WallTimeMs(), m.cfg.MaxClockSkewMs)
	}
	maxEventHLC := clock.Timestamp{}
	for _, event := range events {
		if event == nil {
			continue
		}
		hlc, err := clock.ParseTimestamp(strings.TrimSpace(event.Hlc))
		if err != nil {
			return fmt.Errorf("parse replicated event hlc: %w", err)
		}
		if hlc.WallTimeMs > maxAllowedWallTime {
			return fmt.Errorf("replicated event hlc %s exceeds local wall time %d by more than %dms", hlc, m.clock.WallTimeMs(), m.cfg.MaxClockSkewMs)
		}
		if maxEventHLC == (clock.Timestamp{}) || hlc.Compare(maxEventHLC) > 0 {
			maxEventHLC = hlc
		}
	}
	// 批次sent_at不应早于批次中任意事件的HLC
	if maxEventHLC != (clock.Timestamp{}) && sentAt.Compare(maxEventHLC) < 0 {
		return fmt.Errorf("event batch sent_at_hlc %s is earlier than max event hlc %s", sentAt, maxEventHLC)
	}
	return nil
}

// peerClockState 返回指定对等节点时钟状态的字符串表示。
func (m *Manager) peerClockState(peerID int64) (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, reason := m.peerClockStateLocked(peerID)
	return string(state), reason
}
