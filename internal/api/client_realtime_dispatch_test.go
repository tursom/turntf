package api

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	gproto "google.golang.org/protobuf/proto"
)

type realtimeTestConn struct {
	in     chan []byte
	out    chan *internalproto.ServerEnvelope
	closed chan struct{}
	once   sync.Once
}

func (c *realtimeTestConn) Receive(ctx context.Context) ([]byte, error) {
	select {
	case b := <-c.in:
		return b, nil
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (c *realtimeTestConn) Send(ctx context.Context, b []byte) error {
	e := new(internalproto.ServerEnvelope)
	if err := gproto.Unmarshal(b, e); err != nil {
		return err
	}
	select {
	case <-c.closed:
		return io.EOF
	case c.out <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *realtimeTestConn) Close() error     { c.once.Do(func() { close(c.closed) }); return nil }
func (*realtimeTestConn) RemoteAddr() string { return "test" }
func (*realtimeTestConn) Transport() string  { return "ws" }

type realtimeBlockingResolver struct {
	entered chan struct{}
	release chan struct{}
	ref     store.SessionRef
}

func (r *realtimeBlockingResolver) ResolveUserSessions(ctx context.Context, u store.UserKey) ([]store.OnlineSession, error) {
	select {
	case r.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-r.release:
		return []store.OnlineSession{{User: u, SessionRef: r.ref}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func realtimeFixture(t *testing.T, realtime bool) (*clientWSSession, *realtimeTestConn, *realtimeBlockingResolver) {
	t.Helper()
	a := newAuthenticatedTestAPI(t)
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	user, err := a.http.service.GetUser(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	c := &realtimeTestConn{in: make(chan []byte, 128), out: make(chan *internalproto.ServerEnvelope, 128), closed: make(chan struct{})}
	r := &realtimeBlockingResolver{entered: make(chan struct{}, 128), release: make(chan struct{}), ref: store.SessionRef{ServingNodeID: testNodeID(2), SessionID: "remote"}}
	a.http.service.sessions = r
	s := &clientWSSession{http: a.http, conn: c, principal: &requestPrincipal{User: user}, realtimeOnly: realtime, seen: make(map[clientMessageCursor]struct{})}
	return s, c, r
}
func enqueueRealtime(t *testing.T, s *clientWSSession, c *realtimeTestConn, r *realtimeBlockingResolver, id uint64) {
	t.Helper()
	b, err := gproto.Marshal(&internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_SendMessage{SendMessage: &internalproto.SendMessageRequest{RequestId: id, Target: &internalproto.UserRef{NodeId: s.principal.User.NodeID, UserId: s.principal.User.ID}, TargetSession: clientProtoSessionRef(r.ref), Body: []byte("packet"), DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT}}})
	if err != nil {
		t.Fatal(err)
	}
	c.in <- b
}
func awaitRealtime(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("session lookup remained serialized")
	}
}

func TestRealtimeTargetedLookupParallelAndDisconnect(t *testing.T) {
	s, c, r := realtimeFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.readLoop(ctx) }()
	defer func() {
		cancel()
		_ = c.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("readLoop did not join canceled lookups")
		}
	}()
	enqueueRealtime(t, s, c, r, 1)
	enqueueRealtime(t, s, c, r, 2)
	awaitRealtime(t, r.entered)
	awaitRealtime(t, r.entered)
	_ = c.Close()
	awaitRealtime(t, done)
}

func TestOrdinaryStreamTargetedLookupStaysSerial(t *testing.T) {
	s, c, r := realtimeFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.readLoop(ctx) }()
	defer func() { cancel(); _ = c.Close(); <-done }()
	enqueueRealtime(t, s, c, r, 1)
	enqueueRealtime(t, s, c, r, 2)
	awaitRealtime(t, r.entered)
	select {
	case <-r.entered:
		t.Fatal("ordinary stream became concurrent")
	case <-time.After(50 * time.Millisecond):
	}
	close(r.release)
	awaitRealtime(t, r.entered)
}
