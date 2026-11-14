package api

import (
	"context"
	"sync"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// 每连接最多接纳 64 个后台查询/定向 Relay；满额明确拒绝当前请求，
// 不让等待名额挡住尚未读取的 Stream/Ping，也不缓存鉴权或查询结果。
const clientRealtimeSendConcurrency = 64

type realtimeSendGroup struct {
	ctx       context.Context
	cancel    context.CancelFunc
	slots     chan struct{}
	pending   sync.WaitGroup
	failed    chan error
	stopClose func() bool
	closeDone chan struct{}
	session   *clientWSSession

	streamMu      sync.Mutex
	streams       map[realtimeStreamKey][]realtimeStreamWork
	streamCount   int
	streamBytes   int
	streamPending sync.WaitGroup
	streamWorkers sync.WaitGroup

	replyMu      sync.Mutex
	replies      chan []byte
	replyCount   int
	replyBytes   int
	replyClosed  bool
	replyPending sync.WaitGroup
	replyDone    chan struct{}
}

func newRealtimeSendGroup(ctx context.Context, s *clientWSSession) *realtimeSendGroup {
	ctx, cancel := context.WithCancel(ctx)
	g := &realtimeSendGroup{
		ctx: ctx, cancel: cancel, slots: make(chan struct{}, clientRealtimeSendConcurrency),
		failed: make(chan error, 1), session: s, closeDone: make(chan struct{}),
		streams: make(map[realtimeStreamKey][]realtimeStreamWork),
		replies: make(chan []byte, clientRealtimeReplyRequests), replyDone: make(chan struct{}),
	}
	// WebSocket Receive 只在读取前检查 context；必须 Close 才能中断已经阻塞的读写。
	g.stopClose = context.AfterFunc(ctx, func() {
		defer close(g.closeDone)
		_ = s.conn.Close()
	})
	go g.runReplies()
	return g
}

func (g *realtimeSendGroup) submit(req *internalproto.SendMessageRequest) error {
	return g.submitWork(req.GetRequestId(), func(ctx context.Context) error { return g.session.handleSendMessage(ctx, req) })
}

func (g *realtimeSendGroup) submitLookup(req *internalproto.ResolveUserSessionsRequest) error {
	return g.submitWork(req.GetRequestId(), func(ctx context.Context) error { return g.session.handleResolveUserSessions(ctx, req) })
}

// Targeted transient Relay and session discovery share one admission limit.
// Dedicated Stream frames use separate lanes and never wait for these slots.
func (g *realtimeSendGroup) submitWork(requestID uint64, handle func(context.Context) error) error {
	if err := g.ctx.Err(); err != nil {
		return err
	}
	select {
	case g.slots <- struct{}{}:
	case <-g.ctx.Done():
		return g.ctx.Err()
	default:
		return g.replyError("service_unavailable", "realtime background request limit reached", requestID)
	}
	if err := g.ctx.Err(); err != nil {
		<-g.slots
		return err
	}
	g.pending.Add(1)
	go func() {
		defer g.pending.Done()
		defer func() { <-g.slots }()
		// 完整复用逐请求的权限、黑名单、session 查询和 peer 路由路径。
		// 业务错误已由 handler 按 request_id 回复；仅传输写失败终止整个会话。
		if err := handle(g.ctx); err != nil {
			g.fail(err)
		}
	}()
	return nil
}

func (g *realtimeSendGroup) fail(err error) {
	select {
	case g.failed <- err:
	default:
	}
	g.cancel()
}

// Only readLoop submits work, and it stops submitting while a state RPC drains.
// Workers never Add to these groups, so Wait cannot race new zero-count work.
func (g *realtimeSendGroup) drain() {
	g.pending.Wait()
	g.streamPending.Wait()
	g.replyPending.Wait()
}

func (g *realtimeSendGroup) finish() error {
	g.cancel()
	if g.stopClose() {
		_ = g.session.conn.Close()
	} else {
		<-g.closeDone
	}
	g.pending.Wait()
	g.streamWorkers.Wait()
	g.streamPending.Wait()
	<-g.replyDone
	g.replyPending.Wait()
	select {
	case err := <-g.failed:
		return err
	default:
		return nil
	}
}
