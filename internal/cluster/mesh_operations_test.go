package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/api"
	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/trace"
)

// 不启动任何 HTTP 监听器。C 在 A/B 已开始服务、且 A 已读过成员快照后才加入。
func TestMeshOperationsDynamicThreeNodesWithoutHTTP(t *testing.T) {
	ca := newTCPTestCA(t)
	start := func(slot uint16, upstream string) (*Manager, *api.Service) {
		t.Helper()
		cfg := ca.config(t, testNodeID(slot), nil)
		cfg.DiscoveryDisabled = true // 禁止自动补直连，物理拓扑严格保持 A-B-C。
		cfg.TCPMTLS.AllowedNodeIDs = []int64{testNodeID(1), testNodeID(2), testNodeID(3)}
		if upstream != "" {
			cfg.Peers = []Peer{{URL: upstream}}
		}
		st := newReplicationTestStore(t, fmt.Sprintf("dynamic-ops-%d", slot), slot)
		mgr, err := NewManager(cfg, st)
		if err != nil {
			t.Fatal(err)
		}
		service := api.New(st, mgr)
		if err := mgr.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = mgr.Close() })
		return mgr, service
	}
	a, serviceA := start(1, "")
	b, _ := start(2, tcpTestEndpoint(a.MeshRuntime().tcp))
	waitForMeshRoute(t, a, b.cfg.NodeID, mesh.TrafficControlQuery)
	initial, err := serviceA.ClusterNodes(context.Background())
	if err != nil || len(initial.Nodes) != 2 {
		t.Fatalf("initial members: %+v / %v", initial, err)
	}
	c, serviceC := start(3, tcpTestEndpoint(b.MeshRuntime().tcp))
	waitForMeshRoute(t, a, c.cfg.NodeID, mesh.TrafficControlQuery)
	waitForMeshRoute(t, c, a.cfg.NodeID, mesh.TrafficTransientInteractive)
	waitForMeshRoute(t, a, c.cfg.NodeID, mesh.TrafficTransientInteractive)
	if got := len(a.MeshRuntime().Runtime().Adjacencies()); got != 1 {
		t.Fatalf("A must have only A-B adjacency, got %d", got)
	}
	if got := len(c.MeshRuntime().Runtime().Adjacencies()); got != 1 {
		t.Fatalf("C must have only B-C adjacency, got %d", got)
	}
	nodes, err := serviceA.ClusterNodes(context.Background())
	if err != nil || len(nodes.Nodes) != 3 || nodes.Nodes[2].NodeID != c.cfg.NodeID || nodes.Nodes[2].ConfiguredURL != "" {
		t.Fatalf("dynamic indirect member missing: %+v / %v", nodes, err)
	}
	for _, pair := range []struct {
		service *api.Service
		target  *Manager
	}{{serviceA, c}, {serviceC, a}} {
		data, err := pair.service.NodeOperationsStatus(context.Background(), pair.target.cfg.NodeID)
		if err != nil {
			t.Fatal(err)
		}
		var status struct {
			NodeID int64 `json:"node_id"`
			Window int   `json:"message_window_size"`
		}
		if err := json.Unmarshal(data, &status); err != nil || status.NodeID != pair.target.cfg.NodeID || status.Window != pair.target.store.MessageWindowSize() {
			t.Fatalf("not target status: %s / %v", data, err)
		}
	}
	// 由 A 请求 C 发起 C->A 探测，再反向 A->C，两个方向都要有真实 probe_reached。
	for _, direction := range [][2]int64{{c.cfg.NodeID, a.cfg.NodeID}, {a.cfg.NodeID, c.cfg.NodeID}} {
		probe, err := serviceA.NodeProbe(context.Background(), direction[0], direction[1])
		if err != nil || probe.SourceNodeID != direction[0] || probe.Status != "dispatched" {
			t.Fatalf("remote source probe: %+v / %v", probe, err)
		}
		waitFor(t, 5*time.Second, func() bool {
			result, err := serviceA.ClusterTraceEvents(context.Background(), probe.TraceID)
			if err != nil || len(result.Nodes) != 3 {
				return false
			}
			var reached, started, forwarded bool
			for _, node := range result.Nodes {
				if node.Error != "" {
					return false
				}
				if node.NodeID == direction[1] {
					reached = hasTraceStage(node.Events, "probe_reached")
				}
				if node.NodeID == direction[0] {
					started = hasTraceStage(node.Events, "probe_started")
				}
				if node.NodeID == b.cfg.NodeID {
					forwarded = hasTraceStage(node.Events, "forwarded")
				}
			}
			return reached && started && forwarded
		})
	}
	if _, err := serviceA.NodeOperationsStatus(context.Background(), testNodeID(99)); err == nil {
		t.Fatal("unknown node returned success")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := serviceA.NodeOperationsStatus(ctx, c.cfg.NodeID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled operation: %v", err)
	}
	_ = c.Close()
	waitFor(t, 5*time.Second, func() bool {
		_, ok := b.MeshRuntime().DescribeRoute(c.cfg.NodeID, mesh.TrafficControlQuery)
		return !ok
	})
	ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if data, err := serviceA.NodeOperationsStatus(ctx, c.cfg.NodeID); err == nil || len(data) != 0 {
		t.Fatalf("disconnected C returned local/fake status: %s / %v", data, err)
	}
	id, _ := trace.NewID()
	result, err := serviceA.ClusterTraceEvents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	foundFailure := false
	for _, node := range result.Nodes {
		if node.NodeID == c.cfg.NodeID && node.Error != "" && len(node.Events) == 0 {
			foundFailure = true
		}
	}
	if !foundFailure {
		t.Fatalf("disconnected member needs explicit error: %+v", result)
	}
}

func TestMeshOperationsNilManagerSingleNode(t *testing.T) {
	var manager *Manager
	service := api.New(newReplicationTestStore(t, "ops-single-node", 1), manager)
	if _, err := service.NodeOperationsStatus(context.Background(), testNodeID(1)); err != nil {
		t.Fatalf("single-node status: %v", err)
	}
	if _, err := service.NodeOperationsStatus(context.Background(), testNodeID(2)); !errors.Is(err, app.ErrServiceUnavailable) {
		t.Fatalf("single-node remote operation: %v", err)
	}
	id, _ := trace.NewID()
	result, err := service.ClusterTraceEvents(context.Background(), id)
	if err != nil || len(result.Nodes) != 1 || result.Nodes[0].NodeID != testNodeID(1) {
		t.Fatalf("single-node trace: %+v / %v", result, err)
	}
}

func TestMeshOperationsResponseMatching(t *testing.T) {
	m := newHandshakeTestManager(t)
	result := make(chan app.OperationsResponse, 1)
	m.pendingOperations[7] = pendingOperation{targetNodeID: testNodeID(3), result: result}
	packet := &mesh.ForwardedPacket{SourceNodeId: testNodeID(3), TargetNodeId: m.cfg.NodeID}
	query := &mesh.QueryResponse{RequestId: 7, Kind: meshQueryOperationsResponseKind, Payload: []byte(`{"status":{"node_id":123}}`)}
	packet.SourceNodeId = testNodeID(2)
	if err := m.handleOperationsResponse(packet, query); err == nil {
		t.Fatal("wrong source accepted")
	}
	packet.SourceNodeId = testNodeID(3)
	query.RequestId = 8
	if err := m.handleOperationsResponse(packet, query); err != nil {
		t.Fatal(err)
	}
	select {
	case <-result:
		t.Fatal("wrong source or request id completed query")
	default:
	}
	query.RequestId = 7
	if err := m.handleOperationsResponse(packet, query); err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatal("matching response missing")
	}
	// 重复响应不得阻塞读取循环。
	if err := m.handleOperationsResponse(packet, query); err != nil {
		t.Fatal(err)
	}
	packet.TargetNodeId++
	if err := m.handleOperationsResponse(packet, query); err == nil {
		t.Fatal("wrong destination accepted")
	}
}

func TestMeshOperationsCancellationAndCloseCleanup(t *testing.T) {
	a, _, c := startLinearMeshManagers(t)
	waitForMeshRoute(t, a, c.cfg.NodeID, mesh.TrafficControlQuery)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	defer close(release)
	c.SetLocalOperationsHandler(func(ctx context.Context, _ app.OperationsRequest) (app.OperationsResponse, error) {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return app.OperationsResponse{}, ctx.Err()
		case <-release:
			return app.OperationsResponse{}, nil
		}
	})
	for _, mode := range []string{"cancel", "deadline", "close"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := a.QueryOperations(ctx, c.cfg.NodeID, app.OperationsRequest{Operation: app.OperationStatus})
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("request never reached C")
			}
			if mode == "close" {
				_ = a.Close()
			} else if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("canceled/closed request succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("request did not stop")
			}
			a.mu.Lock()
			pending := len(a.pendingOperations)
			a.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending leaked: %d", pending)
			}
		})
	}
	_ = c.Close()
	if len(c.operationsSlots) != 0 {
		t.Fatal("execution slots leaked on Close")
	}
}

func TestMeshOperationsRemoteErrorsAndExecutionBound(t *testing.T) {
	a, _, c := startLinearMeshManagers(t)
	waitForMeshRoute(t, a, c.cfg.NodeID, mesh.TrafficControlQuery)
	for _, remoteErr := range []error{app.ErrOpsRateLimited, context.DeadlineExceeded, context.Canceled} {
		c.SetLocalOperationsHandler(func(context.Context, app.OperationsRequest) (app.OperationsResponse, error) {
			return app.OperationsResponse{}, remoteErr
		})
		if _, err := a.QueryOperations(context.Background(), c.cfg.NodeID, app.OperationsRequest{Operation: app.OperationStatus}); !errors.Is(err, remoteErr) {
			t.Fatalf("remote error lost: want %v got %v", remoteErr, err)
		}
	}
	c.SetLocalOperationsHandler(nil)
	if _, err := a.QueryOperations(context.Background(), c.cfg.NodeID, app.OperationsRequest{Operation: app.OperationStatus}); !errors.Is(err, app.ErrServiceUnavailable) {
		t.Fatalf("missing handler returned success: %v", err)
	}
	waitFor(t, time.Second, func() bool { return len(c.operationsSlots) == 0 })
	for range cap(c.operationsSlots) {
		c.operationsSlots <- struct{}{}
	}
	defer func() {
		for range cap(c.operationsSlots) {
			<-c.operationsSlots
		}
	}()
	if _, err := a.QueryOperations(context.Background(), c.cfg.NodeID, app.OperationsRequest{Operation: app.OperationStatus}); !errors.Is(err, app.ErrServiceUnavailable) {
		t.Fatalf("saturated executor did not reject: %v", err)
	}
}

func TestMeshOperationsValidationAndBudgets(t *testing.T) {
	m := newHandshakeTestManager(t)
	packet := &mesh.ForwardedPacket{SourceNodeId: testNodeID(2), TargetNodeId: m.cfg.NodeID}
	for _, body := range []string{`{`, `{"request":{"operation":"proxy","url":"http://peer"}}`,
		`{"request":{"operation":"probe","target_node_id":0}}`,
		`{"request":{"operation":"trace","trace_id":"invalid"}}`,
		`{"request":{"operation":"status","target_node_id":123}}`} {
		if err := m.handleOperationsRequest(packet, &mesh.QueryRequest{RequestId: 1, Payload: []byte(body)}); err == nil {
			t.Fatalf("invalid operation accepted: %s", body)
		}
	}
	if _, err := m.QueryOperations(context.Background(), testNodeID(2), app.OperationsRequest{Operation: "proxy"}); err == nil {
		t.Fatal("unsupported outgoing operation accepted")
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() { _ = m.Close() })
	for id := uint64(1); id <= maxPendingOperations; id++ {
		m.pendingOperations[id] = pendingOperation{}
	}
	if _, err := m.QueryOperations(context.Background(), testNodeID(2), app.OperationsRequest{Operation: app.OperationStatus}); !errors.Is(err, app.ErrServiceUnavailable) {
		t.Fatalf("pending budget ignored: %v", err)
	}
	_ = m.Close()
	if len(m.pendingOperations) != 0 {
		t.Fatal("Close did not clear pending map")
	}
}
