package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/trace"
)

type operationsTestSink struct {
	fakeClusterStatusSink
	handler app.OperationsHandler
	nodes   []int64
	query   func(context.Context, int64, app.OperationsRequest) (app.OperationsResponse, error)
}

func (s *operationsTestSink) SetLocalOperationsHandler(handler app.OperationsHandler) {
	s.handler = handler
}
func (s *operationsTestSink) KnownNodeIDs() []int64 { return s.nodes }
func (s *operationsTestSink) QueryOperations(ctx context.Context, id int64, req app.OperationsRequest) (app.OperationsResponse, error) {
	return s.query(ctx, id, req)
}

func TestOpsRemoteHTTPContractsAndAdmin(t *testing.T) {
	const remote int64 = 9223372036854775806
	const target int64 = 9007199254740995
	id := "00112233445566778899aabbccddeeff"
	sink := &operationsTestSink{nodes: []int64{testNodeID(1), remote, target}}
	var calls atomic.Int32
	remoteStatus := json.RawMessage(fmt.Sprintf(`{"node_id":%d,"last_event_sequence":%d,"mesh":{"enabled":true},"peers":[]}`, remote, target))
	sink.query = func(ctx context.Context, nodeID int64, req app.OperationsRequest) (app.OperationsResponse, error) {
		calls.Add(1)
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 4*time.Second {
			t.Error("remote HTTP query needs a bounded context")
		}
		if err := ctx.Err(); err != nil {
			return app.OperationsResponse{}, err
		}
		if nodeID != remote {
			return app.OperationsResponse{}, fmt.Errorf("%w: node unreachable", app.ErrServiceUnavailable)
		}
		switch req.Operation {
		case app.OperationStatus:
			return app.OperationsResponse{Status: remoteStatus}, nil
		case app.OperationProbe:
			return app.OperationsResponse{Probe: &app.ProbeResult{TraceID: id, SourceNodeID: remote, TargetNodeID: req.TargetNodeID, Status: "dispatched"}}, nil
		case app.OperationTrace:
			return app.OperationsResponse{Events: []trace.Event{{TraceID: id, NodeID: remote, Stage: "probe_reached"}}}, nil
		}
		return app.OperationsResponse{}, errors.New("unexpected operation")
	}
	testAPI := newAuthenticatedTestAPIWithSink(t, sink)
	if sink.handler == nil {
		t.Fatal("api.New failed to register local handler")
	}
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	admin := loginToken(t, testAPI.handler, key, "root-password")
	user := createUserAs(t, testAPI.handler, admin, "ops-user", "password", store.RoleUser)
	userToken := loginToken(t, testAPI.handler, user, "password")
	headers := map[string]string{"Authorization": "Bearer " + admin}
	statusPath := fmt.Sprintf("/ops/nodes/%d/status", remote)
	probePath := fmt.Sprintf("/ops/nodes/%d/probes", remote)
	tracePath := "/ops/traces/" + id + "/cluster"
	for _, endpoint := range []struct{ method, path string }{{http.MethodGet, statusPath}, {http.MethodPost, probePath}, {http.MethodGet, tracePath}} {
		doJSONWithHeaders(t, testAPI.handler, endpoint.method, endpoint.path, nil, nil, http.StatusUnauthorized)
		doJSONWithHeaders(t, testAPI.handler, endpoint.method, endpoint.path, nil,
			map[string]string{"Authorization": "Bearer " + userToken}, http.StatusForbidden)
	}
	if calls.Load() != 0 {
		t.Fatal("unauthorized request reached mesh")
	}
	data := doJSONWithHeaders(t, testAPI.handler, http.MethodGet, statusPath, nil, headers, http.StatusOK)
	if !bytes.Equal(bytes.TrimSpace(data), remoteStatus) {
		t.Fatalf("remote status changed JSON/int64: %s", data)
	}
	local := doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/status", nil, headers, http.StatusOK)
	localByID := doJSONWithHeaders(t, testAPI.handler, http.MethodGet,
		fmt.Sprintf("/ops/nodes/%d/status", key.NodeID), nil, headers, http.StatusOK)
	if !bytes.Equal(local, localByID) {
		t.Fatalf("local status contract changed: %s / %s", local, localByID)
	}
	for _, bad := range []string{"0", "-1", "9223372036854775808", "abc"} {
		doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/nodes/"+bad+"/status", nil, headers, http.StatusBadRequest)
		doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/ops/nodes/"+bad+"/probes", nil, headers, http.StatusBadRequest)
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/traces/invalid/cluster", nil, headers, http.StatusBadRequest)
	for _, body := range []any{map[string]any{"target_node_id": 0}, map[string]any{"target_node_id": 1, "url": "http://peer"},
		map[string]any{"target_node_id": 1, "padding": strings.Repeat("x", 300)}} {
		doJSONWithHeaders(t, testAPI.handler, http.MethodPost, probePath, body, headers, http.StatusBadRequest)
	}
	var probe app.ProbeResult
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, probePath,
		map[string]any{"target_node_id": target}, headers, http.StatusAccepted), &probe)
	if probe.SourceNodeID != remote || probe.TargetNodeID != target || probe.TraceID != id || probe.Status != "dispatched" {
		t.Fatalf("probe lost source/int64: %+v", probe)
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/ops/probes", map[string]any{"target_node_id": target}, headers, http.StatusTooManyRequests)
	var aggregate app.ClusterTrace
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, tracePath, nil, headers, http.StatusOK), &aggregate)
	if aggregate.TraceID != id || len(aggregate.Nodes) != 3 {
		t.Fatalf("aggregate members: %+v", aggregate)
	}
	for _, node := range aggregate.Nodes {
		if node.Events == nil {
			t.Fatal("events must be [] even on failure")
		}
		if node.NodeID == target && node.Error == "" {
			t.Fatal("unreachable member did not report error")
		}
		if node.NodeID == remote && (node.Error != "" || len(node.Events) != 1 || node.Events[0].Stage != "probe_reached") {
			t.Fatalf("remote trace missing: %+v", node)
		}
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, fmt.Sprintf("/ops/nodes/%d/status", target), nil, headers, http.StatusServiceUnavailable)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, statusPath, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+admin)
	recorder := httptest.NewRecorder()
	testAPI.handler.ServeHTTP(recorder, req)
	if recorder.Code == http.StatusOK {
		t.Fatalf("canceled request returned status: %s", recorder.Body)
	}
}

func TestOpsClusterTraceQueriesDynamicMembersInParallel(t *testing.T) {
	id, _ := trace.NewID()
	sink := &operationsTestSink{nodes: []int64{testNodeID(1), 101, 102}}
	entered := make(chan int64, 2)
	release := make(chan struct{})
	sink.query = func(ctx context.Context, node int64, _ app.OperationsRequest) (app.OperationsResponse, error) {
		entered <- node
		select {
		case <-release:
			return app.OperationsResponse{}, nil
		case <-ctx.Done():
			return app.OperationsResponse{}, ctx.Err()
		}
	}
	api := newAuthenticatedTestAPIWithSink(t, sink)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan app.ClusterTrace, 1)
	go func() {
		result, _ := api.http.service.ClusterTraceEvents(ctx, id)
		done <- result
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("trace requests were serialized")
		}
	}
	close(release)
	result := <-done
	if len(result.Nodes) != 3 {
		t.Fatalf("members: %+v", result)
	}
	// 后续请求重新读取 provider；不缓存上次成员列表。
	sink.nodes = []int64{testNodeID(1)}
	result, err := api.http.service.ClusterTraceEvents(context.Background(), id)
	if err != nil || len(result.Nodes) != 1 {
		t.Fatalf("stale membership: %+v / %v", result, err)
	}
}

func TestOpsSourceProbeRateLimitAndFailure(t *testing.T) {
	sink := &testProbeSink{}
	api := newAuthenticatedTestAPIWithSink(t, sink)
	service := api.http.service
	_, err := service.NodeProbe(context.Background(), service.store.NodeID(), testNodeID(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.handleLocalOperation(context.Background(), app.OperationsRequest{Operation: app.OperationProbe, TargetNodeID: testNodeID(2)}); !errors.Is(err, app.ErrOpsRateLimited) {
		t.Fatalf("source rate limit bypass: %v", err)
	}
	if sink.calls != 1 {
		t.Fatal("rate limited source dispatched another probe")
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, app.ErrOpsRateLimited} {
		recorder := httptest.NewRecorder()
		writeOpsError(recorder, err)
		if recorder.Code != http.StatusRequestTimeout && recorder.Code != http.StatusGatewayTimeout && recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("operation error hidden: %d", recorder.Code)
		}
	}
}
