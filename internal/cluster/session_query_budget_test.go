package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
	"google.golang.org/protobuf/proto"
)

type sessionBudgetDialer struct {
	Dialer
	enabled atomic.Bool
	budgets chan time.Duration
}

func (d *sessionBudgetDialer) Dial(ctx context.Context, endpoint string) (TransportConn, error) {
	c, err := d.Dialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return &sessionBudgetConn{TransportConn: c, owner: d}, nil
}

type sessionBudgetConn struct {
	TransportConn
	owner *sessionBudgetDialer
}

func (c *sessionBudgetConn) Send(ctx context.Context, p []byte) error {
	if c.owner.enabled.Load() {
		var env mesh.ClusterEnvelope
		if proto.Unmarshal(p, &env) == nil && env.GetForwardedPacket().GetTrafficClass() == mesh.TrafficControlQuery {
			deadline, ok := ctx.Deadline()
			budget := time.Duration(0)
			if ok {
				budget = time.Until(deadline)
			}
			select {
			case c.owner.budgets <- budget:
			default:
			}
			return context.DeadlineExceeded
		}
	}
	return c.TransportConn.Send(ctx, p)
}

func TestSessionLookupDeadlineIncludesSending(t *testing.T) {
	target := newMeshClockTestManager(t)
	server := newClusterHTTPTestServer(t, target.Handler())
	if err := target.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	source, err := NewManager(Config{NodeID: testNodeID(2), AdvertisePath: websocketPath, ClusterSecret: "secret", MessageWindowSize: store.DefaultMessageWindowSize, MaxClockSkewMs: DefaultMaxClockSkewMs, DiscoveryDisabled: true, Peers: []Peer{{URL: websocketURL(server.URL) + websocketPath}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	spy := &sessionBudgetDialer{Dialer: source.websocket, budgets: make(chan time.Duration, 1)}
	binding, err := source.BuildMeshRuntime()
	if err != nil {
		t.Fatal(err)
	}
	binding.adapters[mesh.TransportWebSocket].dialer = spy.Dial
	source.meshRuntime = binding
	if err := binding.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer binding.Close()
	waitForMeshRoute(t, source, target.cfg.NodeID, mesh.TrafficControlQuery)
	spy.enabled.Store(true)
	_, err = source.resolveUserSessionsAtNode(context.Background(), target.cfg.NodeID, store.UserKey{NodeID: target.cfg.NodeID, UserID: 4097})
	if !errors.Is(err, app.ErrServiceUnavailable) {
		t.Fatalf("send timeout must report unavailable: %v", err)
	}
	select {
	case budget := <-spy.budgets:
		if budget <= 0 || budget > queryLoggedInUsersTimeout {
			t.Fatalf("send uses %v instead of the %v query budget", budget, queryLoggedInUsersTimeout)
		}
	case <-time.After(time.Second):
		t.Fatal("query did not reach transport")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = source.resolveUserSessionsAtNode(ctx, target.cfg.NodeID, store.UserKey{NodeID: target.cfg.NodeID, UserID: 4097})
	if !errors.Is(err, app.ErrServiceUnavailable) {
		t.Fatalf("explicit send timeout must remain unavailable: %v", err)
	}
	select {
	case budget := <-spy.budgets:
		if budget <= 0 || budget > 100*time.Millisecond {
			t.Fatalf("caller deadline was extended: %v", budget)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit-deadline query did not reach transport")
	}
	source.mu.Lock()
	pending := len(source.pendingResolveSessions)
	source.mu.Unlock()
	if pending != 0 {
		t.Fatal("send timeout leaked pending query")
	}
}
