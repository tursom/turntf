package api

import (
	"context"
	"errors"

	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

const (
	clientRealtimeReplyRequests = 128
	clientRealtimeReplyBytes    = 1 << 20
)

var errRealtimeReplyFull = errors.New("realtime reader response budget exceeded")

// reply queues only reader-owned Pong/admission/invalid-frame responses.
// Background handlers and stream lanes keep waiting for actual writes through
// the original session lock, preserving state barriers without another general
// outbound queue. The reader never waits for a busy physical socket here.
func (g *realtimeSendGroup) reply(envelope *internalproto.ServerEnvelope) error {
	data, err := gproto.Marshal(envelope)
	if err != nil {
		g.fail(err)
		return err
	}
	g.replyMu.Lock()
	if err := g.ctx.Err(); err != nil {
		g.replyMu.Unlock()
		return err
	}
	if g.replyClosed {
		g.replyMu.Unlock()
		return context.Canceled
	}
	if g.replyCount >= clientRealtimeReplyRequests || len(data) > clientRealtimeReplyBytes-g.replyBytes {
		g.replyMu.Unlock()
		g.fail(errRealtimeReplyFull)
		return errRealtimeReplyFull
	}
	g.replyCount++
	g.replyBytes += len(data)
	g.replyPending.Add(1)
	// The budget includes the worker's active write, so channel capacity is
	// available under this producer lock; this send cannot wait on the socket.
	g.replies <- data
	g.replyMu.Unlock()
	return nil
}

func (g *realtimeSendGroup) replyError(code, message string, requestID uint64) error {
	g.session.logWarn("client_error_response", errors.New(message)).
		Str("code", code).Uint64("request_id", requestID).
		Msg("client transport returning error")
	return g.reply(&internalproto.ServerEnvelope{
		Body: &internalproto.ServerEnvelope_Error{Error: clientWSError(code, message, requestID)},
	})
}

func (g *realtimeSendGroup) releaseReply(data []byte) {
	g.replyMu.Lock()
	g.replyCount--
	g.replyBytes -= len(data)
	g.replyMu.Unlock()
	g.replyPending.Done()
}

func (g *realtimeSendGroup) runReplies() {
	defer close(g.replyDone)
	defer func() {
		// Exclude late producers before draining cancellation leftovers. All
		// successful Adds must get a Done, including frames never sent.
		g.replyMu.Lock()
		g.replyClosed = true
		for len(g.replies) > 0 {
			data := <-g.replies
			g.replyCount--
			g.replyBytes -= len(data)
			g.replyPending.Done()
		}
		g.replyMu.Unlock()
	}()
	for {
		select {
		case <-g.ctx.Done():
			return
		case data := <-g.replies:
			if g.ctx.Err() != nil {
				g.releaseReply(data)
				return
			}
			err := g.session.writeEncodedEnvelope(data)
			if err != nil {
				g.fail(err)
			}
			g.releaseReply(data)
			if err != nil {
				return
			}
		}
	}
}
