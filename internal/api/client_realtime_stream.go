package api

import (
	"errors"

	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

const (
	clientRealtimeStreamRequests = 64
	clientRealtimeStreamBytes    = 8 << 20
)

var errRealtimeStreamFull = errors.New("realtime stream request budget exceeded")

// Epoch deliberately does not partition a lane: Resume/Open of a new epoch
// cannot overtake already accepted frames belonging to the same stream.
type realtimeStreamKey struct {
	nodeID, userID, servingNodeID int64
	sessionID, streamID           string
}

type realtimeStreamWork struct {
	request *internalproto.StreamFrameRequest
	bytes   int
}

func (g *realtimeSendGroup) submitStream(req *internalproto.StreamFrameRequest) error {
	size := gproto.Size(req)
	g.streamMu.Lock()
	if err := g.ctx.Err(); err != nil {
		g.streamMu.Unlock()
		return err
	}
	// Count both queued and executing frames, including a worker waiting for
	// its response write. A zero-ID stream cannot report an admission failure
	// to a pending RPC: close on overflow instead of silently dropping data.
	if g.streamCount >= clientRealtimeStreamRequests || size > clientRealtimeStreamBytes-g.streamBytes {
		g.streamMu.Unlock()
		g.fail(errRealtimeStreamFull)
		return errRealtimeStreamFull
	}
	target, session := req.GetTarget(), req.GetTargetSession()
	key := realtimeStreamKey{
		nodeID: target.GetNodeId(), userID: target.GetUserId(),
		servingNodeID: session.GetServingNodeId(), sessionID: session.GetSessionId(),
		streamID: string(req.GetStreamId()),
	}
	queue, exists := g.streams[key]
	g.streams[key] = append(queue, realtimeStreamWork{request: req, bytes: size})
	g.streamCount++
	g.streamBytes += size
	g.streamPending.Add(1)
	if !exists {
		g.streamWorkers.Add(1)
		go g.runStream(key)
	}
	g.streamMu.Unlock()
	return nil
}

func (g *realtimeSendGroup) runStream(key realtimeStreamKey) {
	defer g.streamWorkers.Done()
	for {
		g.streamMu.Lock()
		queue := g.streams[key]
		if g.ctx.Err() != nil {
			for _, work := range queue {
				g.streamCount--
				g.streamBytes -= work.bytes
				g.streamPending.Done()
			}
			delete(g.streams, key)
			g.streamMu.Unlock()
			return
		}
		if len(queue) == 0 {
			// Delete while holding the submission lock. A replacement worker
			// can start only after this lane has finished all handler calls.
			delete(g.streams, key)
			g.streamMu.Unlock()
			return
		}
		work := queue[0]
		queue[0] = realtimeStreamWork{}
		g.streams[key] = queue[1:]
		g.streamMu.Unlock()

		if err := g.session.handleStreamFrame(g.ctx, work.request); err != nil {
			g.fail(err)
		}
		g.streamMu.Lock()
		g.streamCount--
		g.streamBytes -= work.bytes
		empty := len(g.streams[key]) == 0
		if empty {
			// Retire together with the last slot release. Otherwise empty
			// lanes could accumulate while their workers await rescheduling.
			delete(g.streams, key)
		}
		g.streamPending.Done()
		g.streamMu.Unlock()
		if empty {
			return
		}
	}
}
