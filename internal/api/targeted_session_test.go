package api

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/tursom/turntf/internal/store"
)

type targetedSessionSink struct {
	routingSink
	node  int64
	calls int
	err   error
}

func (s *targetedSessionSink) ResolveUserSessions(context.Context, store.UserKey) ([]store.OnlineSession, error) {
	return nil, errors.New("unrelated session nodes must not be queried")
}
func (s *targetedSessionSink) ResolveUserSessionsAtNode(_ context.Context, _ store.UserKey, node int64) ([]store.OnlineSession, error) {
	s.node = node
	s.calls++
	return s.sessions, s.err
}

func TestTargetedDispatchUsesOnlyServingNode(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "target.db"), store.Options{NodeID: testNodeID(1)})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}
	user, _, err := st.CreateUser(ctx, store.CreateUserParams{Username: "target", PasswordHash: "hash", Role: store.RoleUser})
	if err != nil {
		t.Fatal(err)
	}
	ref := store.SessionRef{ServingNodeID: testNodeID(2), SessionID: "session"}
	sink := &targetedSessionSink{routingSink: routingSink{sessions: []store.OnlineSession{{User: user.Key(), SessionRef: ref, TransientCapable: true}}}}
	svc := New(st, sink)
	send := func() error {
		_, err := svc.DispatchTransientPacketTo(ctx, user.Key(), user.Key(), []byte("data"), store.DeliveryModeRouteRetry, ref)
		return err
	}
	if err := send(); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 1 || sink.node != ref.ServingNodeID || len(sink.routed) != 1 {
		t.Fatal("wrong lookup or route")
	}
	sink.sessions = nil
	if err := send(); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing session: %v", err)
	}
	sink.err = context.DeadlineExceeded
	if err := send(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lookup error: %v", err)
	}
	if len(sink.routed) != 1 || sink.calls != 3 {
		t.Fatal("lookup was cached or invalid packets were routed")
	}
}
