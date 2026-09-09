package api

import (
	"context"
	"sync"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// 每连接最多保留 16 个处理中的请求，以及 readLoop 已解码、等待名额的一个请求。
// 不增加 busy 错误、不缓存验证结果；满额时直接对读取端施加背压。
const clientRealtimeSendConcurrency = 16

type realtimeSendGroup struct {
	ctx       context.Context
	cancel    context.CancelFunc
	slots     chan struct{}
	pending   sync.WaitGroup
	failed    chan error
	stopClose func() bool
	closeDone chan struct{}
	session   *clientWSSession
}

func newRealtimeSendGroup(ctx context.Context, s *clientWSSession) *realtimeSendGroup {
	ctx, cancel := context.WithCancel(ctx)
	g := &realtimeSendGroup{ctx: ctx, cancel: cancel, slots: make(chan struct{}, clientRealtimeSendConcurrency), failed: make(chan error, 1), session: s, closeDone: make(chan struct{})}
	// WebSocket Receive 只在读取前检查 context；必须 Close 才能中断已经阻塞的读写。
	g.stopClose = context.AfterFunc(ctx, func() {
		defer close(g.closeDone)
		_ = s.conn.Close()
	})
	return g
}

func (g *realtimeSendGroup) submit(req *internalproto.SendMessageRequest) error {
	select {
	case g.slots <- struct{}{}:
	case <-g.ctx.Done():
		return g.ctx.Err()
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
		if err := g.session.handleSendMessage(g.ctx, req); err != nil {
			select {
			case g.failed <- err:
			default:
			}
			g.cancel()
		}
	}()
	return nil
}

func (g *realtimeSendGroup) finish() error {
	g.cancel()
	if g.stopClose() {
		_ = g.session.conn.Close()
	} else {
		<-g.closeDone
	}
	g.pending.Wait()
	select {
	case err := <-g.failed:
		return err
	default:
		return nil
	}
}
