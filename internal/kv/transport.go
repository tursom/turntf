package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// ConsensusSender must finish in bounded time. Production uses the cancellable
// WithConsensusSender adapter; this signature remains for existing callers.
type ConsensusSender func(target raft.ServerAddress, payload []byte) error
type TransportOption func(*Transport)

func WithTransportContext(ctx context.Context) TransportOption {
	return func(t *Transport) { t.parent = ctx }
}
func WithConsensusSender(send func(context.Context, raft.ServerAddress, []byte) error) TransportOption {
	return func(t *Transport) { t.sendContext = send }
}
func WithBinaryConsensus(supports func(raft.ServerAddress) bool) TransportOption {
	return func(t *Transport) { t.binaryPeer = supports }
}

type pendingRPC struct {
	target raft.ServerAddress
	method string
	binary bool
	result chan rpcWire
}

type Transport struct {
	local       raft.ServerAddress
	send        ConsensusSender
	sendContext func(context.Context, raft.ServerAddress, []byte) error
	binaryPeer  func(raft.ServerAddress) bool
	parent      context.Context
	ctx         context.Context
	cancel      context.CancelFunc
	timeout     time.Duration
	consumer    chan raft.RPC
	workers     chan struct{}
	mu          sync.Mutex
	next        uint64
	pending     map[uint64]*pendingRPC
	closed      bool
	wg          sync.WaitGroup
	closeOnce   sync.Once
}

func NewTransport(local raft.ServerAddress, send ConsensusSender, options ...TransportOption) *Transport {
	t := &Transport{local: local, send: send, parent: context.Background(), timeout: 10 * time.Second,
		consumer: make(chan raft.RPC, 128), workers: make(chan struct{}, 128), pending: make(map[uint64]*pendingRPC)}
	for _, option := range options {
		option(t)
	}
	t.ctx, t.cancel = context.WithCancel(t.parent)
	return t
}
func (t *Transport) Consumer() <-chan raft.RPC                                  { return t.consumer }
func (t *Transport) LocalAddr() raft.ServerAddress                              { return t.local }
func (t *Transport) EncodePeer(_ raft.ServerID, addr raft.ServerAddress) []byte { return []byte(addr) }
func (t *Transport) DecodePeer(b []byte) raft.ServerAddress                     { return raft.ServerAddress(b) }
func (t *Transport) SetHeartbeatHandler(func(raft.RPC))                         {}
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.cancel()
		t.mu.Unlock()
		t.wg.Wait()
		close(t.consumer)
	})
	return nil
}

func (t *Transport) sendRPC(ctx context.Context, target raft.ServerAddress, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.sendContext != nil {
		return t.sendContext(ctx, target, payload)
	}
	if t.send == nil {
		return errors.New("kv: consensus sender is nil")
	}
	return t.send(target, payload)
}

// Deliver is for trusted in-process adapters without a mesh source. Production
// must use DeliverFrom so a response is bound to its authenticated outer source.
func (t *Transport) Deliver(payload []byte) error { return t.DeliverFrom("", payload) }

func (t *Transport) DeliverFrom(source raft.ServerAddress, payload []byte) error {
	w, err := decodeRPC(payload)
	if err != nil {
		return err
	}
	if source != "" && w.Source != "" && string(source) != w.Source {
		return errors.New("kv: consensus source mismatch")
	}
	if source == "" {
		source = raft.ServerAddress(w.Source)
	}
	t.mu.Lock()
	if t.closed || t.ctx.Err() != nil {
		t.mu.Unlock()
		return errors.New("kv: transport closed")
	}
	if w.response {
		p := t.pending[w.ID]
		if p == nil {
			t.mu.Unlock()
			return nil
		} // Late/duplicate response.
		// Legacy JSON responses omit Source; DeliverFrom supplies it in production.
		if (source != "" && p.target != source) || p.method != w.Type || p.binary != w.binary {
			t.mu.Unlock()
			return errors.New("kv: consensus response does not match request")
		}
		delete(t.pending, w.ID)
		p.result <- w
		t.mu.Unlock()
		return nil
	}
	// A request can share its ID with an outgoing request. Direction is checked
	// before touching pending. Bounded workers free the mesh reader immediately,
	// allowing bidirectional calls even when send invokes Deliver synchronously.
	select {
	case t.workers <- struct{}{}:
		t.wg.Add(1)
	default:
		t.mu.Unlock()
		return errors.New("kv: consensus request queue full")
	}
	t.mu.Unlock()
	go func() {
		defer t.wg.Done()
		defer func() { <-t.workers }()
		t.serve(source, w)
	}()
	return nil
}

func (t *Transport) serve(source raft.ServerAddress, w rpcWire) {
	ctx, cancel := context.WithTimeout(t.ctx, t.timeout)
	defer cancel()
	resp := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{Command: w.value, RespChan: resp}
	var result raft.RPCResponse
	if q, ok := w.value.(*raft.InstallSnapshotRequest); ok {
		if q.Size < 0 || int64(len(w.snapshot)) != q.Size {
			result.Error = errors.New("kv: snapshot body unavailable; upgrade peer for binary snapshot transfer")
		} else {
			rpc.Reader = bytes.NewReader(w.snapshot)
		}
	}
	if result.Error == nil {
		select {
		case t.consumer <- rpc:
		case <-ctx.Done():
			return
		}
		select {
		case result = <-resp:
		case <-ctx.Done():
			return
		}
	}
	value, _ := rpcValue(w.Type, true)
	if result.Response != nil {
		if reflect.TypeOf(result.Response) != reflect.TypeOf(value) {
			result.Error = errors.New("kv: invalid raft response type")
		} else {
			value = result.Response
		}
	}
	out := rpcWire{ID: w.ID, Type: w.Type, Source: string(t.local), binary: w.binary, response: true, value: value}
	if result.Error != nil {
		out.Error = result.Error.Error()
	}
	encoded, err := encodeRPC(out)
	if err == nil {
		_ = t.sendRPC(ctx, source, encoded)
	} // Caller times out on send failure.
}

func (t *Transport) call(parent context.Context, target raft.ServerAddress, typ string, req, resp any, snapshot []byte) error {
	ctx, cancel := context.WithTimeout(parent, t.timeout)
	defer cancel()
	binary := t.binaryPeer != nil && t.binaryPeer(target)
	if typ == "snapshot" && !binary && len(snapshot) != 0 {
		return errors.New("kv: peer does not support snapshot body transfer; upgrade peer")
	}
	t.mu.Lock()
	if t.closed || t.ctx.Err() != nil {
		t.mu.Unlock()
		return errors.New("kv: transport closed")
	}
	t.next++
	if t.next == 0 {
		t.next++
	}
	rid := t.next
	p := &pendingRPC{target: target, method: typ, binary: binary, result: make(chan rpcWire, 1)}
	t.pending[rid] = p
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		if t.pending[rid] == p {
			delete(t.pending, rid)
		}
		t.mu.Unlock()
	}()
	data, err := encodeRPC(rpcWire{ID: rid, Type: typ, Source: string(t.local), binary: binary, value: req, snapshot: snapshot})
	if err != nil {
		return err
	}
	if err := t.sendRPC(ctx, target, data); err != nil {
		return err
	}
	select {
	case w := <-p.result:
		if w.Error != "" {
			return errors.New(w.Error)
		}
		if reflect.TypeOf(w.value) != reflect.TypeOf(resp) {
			return errors.New("kv: invalid consensus response type")
		}
		reflect.ValueOf(resp).Elem().Set(reflect.ValueOf(w.value).Elem())
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kv: consensus rpc: %w", ctx.Err())
	}
}

func (t *Transport) AppendEntries(_ raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return t.call(t.ctx, target, "append", args, resp, nil)
}
func (t *Transport) RequestVote(_ raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return t.call(t.ctx, target, "vote", args, resp, nil)
}
func (t *Transport) RequestPreVote(_ raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	return t.call(t.ctx, target, "prevote", args, resp, nil)
}
func (t *Transport) InstallSnapshot(_ raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	if args.Size < 0 {
		return errors.New("kv: negative snapshot size")
	}
	if args.Size > maxConsensusSnapshotBytes {
		return fmt.Errorf("kv: snapshot exceeds single-frame limit of %d bytes", maxConsensusSnapshotBytes)
	}
	if args.Size > 0 && (t.binaryPeer == nil || !t.binaryPeer(target)) {
		return errors.New("kv: peer does not support snapshot body transfer; upgrade peer")
	}
	// A snapshot travels in one mesh frame, subject to the transport's configured
	// frame limit. Read exactly the declared size, without trusting an allocation
	// size from a remote peer or consuming an unbounded reader.
	var body []byte
	if data != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(data, args.Size))
		if err != nil {
			return err
		}
	}
	if int64(len(body)) != args.Size {
		return errors.New("kv: truncated snapshot body")
	}
	return t.call(t.ctx, target, "snapshot", args, resp, body)
}
func (t *Transport) TimeoutNow(_ raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return t.call(t.ctx, target, "timeout", args, resp, nil)
}
func (t *Transport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	ctx, cancel := context.WithCancel(t.ctx)
	p := &pipeline{t: t, id: id, target: target, ctx: ctx, cancel: cancel, ch: make(chan raft.AppendFuture), queue: make(chan *future, 2), slots: make(chan struct{}, 2)}
	go p.publish()
	return p, nil
}

type pipeline struct {
	t      *Transport
	id     raft.ServerID
	target raft.ServerAddress
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan raft.AppendFuture
	queue  chan *future
	slots  chan struct{}
}

func (p *pipeline) AppendEntries(a *raft.AppendEntriesRequest, r *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	if p.ctx.Err() != nil {
		return nil, raft.ErrPipelineShutdown
	}
	wait, cancel := context.WithTimeout(p.ctx, p.t.timeout)
	defer cancel()
	select {
	case p.slots <- struct{}{}:
	case <-wait.Done():
		return nil, raft.ErrPipelineShutdown
	}
	f := &future{req: a, resp: r, start: time.Now(), done: make(chan struct{})}
	select {
	case p.queue <- f:
	case <-wait.Done():
		<-p.slots
		return nil, raft.ErrPipelineShutdown
	}
	go func() {
		f.err = p.t.call(p.ctx, p.target, "append", a, r, nil)
		if f.err != nil {
			// Raft's pipeline decoder reads Success without calling Error().
			r.Success = false
		}
		close(f.done)
	}()
	return f, nil
}

func (p *pipeline) publish() {
	// Like raft.NetworkTransport, leave Consumer open on Close: Raft's decoder
	// reads it without checking ok and exits via its separate stop channel.
	for {
		select {
		case f := <-p.queue:
			select {
			case <-f.done:
			case <-p.ctx.Done():
				return
			}
			select {
			case p.ch <- f:
				if f.err != nil || !f.resp.Success || f.resp.Term > f.req.Term {
					// Raft stops consuming after this result. Wake blocked senders
					// before they can wait for a consumer that has already exited.
					p.cancel()
					return
				}
				<-p.slots
			case <-p.ctx.Done():
				return
			}
		case <-p.ctx.Done():
			return
		}
	}
}
func (p *pipeline) Consumer() <-chan raft.AppendFuture { return p.ch }
func (p *pipeline) Close() error                       { p.cancel(); return nil }

type future struct {
	req   *raft.AppendEntriesRequest
	resp  *raft.AppendEntriesResponse
	start time.Time
	done  chan struct{}
	err   error
}

func (f *future) Error() error                          { <-f.done; return f.err }
func (f *future) Start() time.Time                      { return f.start }
func (f *future) Request() *raft.AppendEntriesRequest   { return f.req }
func (f *future) Response() *raft.AppendEntriesResponse { return f.resp }
