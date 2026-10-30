package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/trace"
)

type operationsQuerier interface {
	QueryOperations(context.Context, int64, app.OperationsRequest) (app.OperationsResponse, error)
}

// handleLocalOperation 同时服务本机 HTTP 和 mesh RPC，只访问本机的 Service/trace store。
func (s *Service) handleLocalOperation(ctx context.Context, req app.OperationsRequest) (app.OperationsResponse, error) {
	if err := ctx.Err(); err != nil {
		return app.OperationsResponse{}, err
	}
	switch req.Operation {
	case app.OperationStatus:
		status, err := s.OperationsStatus(ctx)
		if err != nil {
			return app.OperationsResponse{}, err
		}
		data, err := json.Marshal(status)
		return app.OperationsResponse{Status: data}, err
	case app.OperationTrace:
		if !trace.ValidID(req.TraceID) {
			return app.OperationsResponse{}, fmt.Errorf("%w: invalid trace id", store.ErrInvalidInput)
		}
		return app.OperationsResponse{Events: s.TraceEvents(req.TraceID)}, nil
	case app.OperationProbe:
		if req.TargetNodeID <= 0 {
			return app.OperationsResponse{}, fmt.Errorf("%w: invalid probe target", store.ErrInvalidInput)
		}
		// 源节点也限速，多个 HTTP 入口不能绕过每节点每秒一次的约束。
		s.opsProbeMu.Lock()
		now := time.Now()
		if !s.lastOpsProbe.IsZero() && now.Sub(s.lastOpsProbe) < time.Second {
			s.opsProbeMu.Unlock()
			return app.OperationsResponse{}, app.ErrOpsRateLimited
		}
		s.lastOpsProbe = now
		s.opsProbeMu.Unlock()
		id, err := s.ProbeRoute(ctx, req.TargetNodeID)
		if err != nil && !trace.ValidID(id) {
			return app.OperationsResponse{}, err
		}
		status := "dispatched"
		if err != nil {
			status = "failed"
		}
		return app.OperationsResponse{Probe: &app.ProbeResult{TraceID: id, SourceNodeID: s.store.NodeID(),
			TargetNodeID: req.TargetNodeID, Status: status}}, nil
	default:
		return app.OperationsResponse{}, fmt.Errorf("%w: unsupported operation", store.ErrInvalidInput)
	}
}

func (s *Service) queryNodeOperation(ctx context.Context, nodeID int64, req app.OperationsRequest) (app.OperationsResponse, error) {
	if nodeID <= 0 {
		return app.OperationsResponse{}, fmt.Errorf("%w: node_id must be positive", store.ErrInvalidInput)
	}
	if nodeID == s.store.NodeID() {
		return s.handleLocalOperation(ctx, req)
	}
	querier, ok := s.eventSink.(operationsQuerier)
	if !ok {
		return app.OperationsResponse{}, fmt.Errorf("%w: mesh operations unavailable", app.ErrServiceUnavailable)
	}
	return querier.QueryOperations(ctx, nodeID, req)
}

func (s *Service) NodeOperationsStatus(ctx context.Context, nodeID int64) (json.RawMessage, error) {
	response, err := s.queryNodeOperation(ctx, nodeID, app.OperationsRequest{Operation: app.OperationStatus})
	if err != nil {
		return nil, err
	}
	// 防止旧版本或不完整的响应被当作目标状态返回。
	var identity struct {
		NodeID int64 `json:"node_id"`
	}
	if err := json.Unmarshal(response.Status, &identity); err != nil || identity.NodeID != nodeID {
		return nil, fmt.Errorf("%w: invalid status from node %d", app.ErrServiceUnavailable, nodeID)
	}
	return response.Status, nil
}

func (s *Service) NodeProbe(ctx context.Context, source, target int64) (app.ProbeResult, error) {
	response, err := s.queryNodeOperation(ctx, source, app.OperationsRequest{Operation: app.OperationProbe, TargetNodeID: target})
	if err != nil {
		return app.ProbeResult{}, err
	}
	if response.Probe == nil || response.Probe.SourceNodeID != source || response.Probe.TargetNodeID != target ||
		!trace.ValidID(response.Probe.TraceID) || (response.Probe.Status != "dispatched" && response.Probe.Status != "failed") {
		return app.ProbeResult{}, fmt.Errorf("%w: invalid probe response from node %d", app.ErrServiceUnavailable, source)
	}
	return *response.Probe, nil
}

// ClusterTraceEvents 每次取实时成员快照，最多 16 个并发查询，保留每节点失败。
// 已知但断开的节点仍参与查询，以 error 显示信息缺口；空 events 只表示该节点没有记录。
func (s *Service) ClusterTraceEvents(ctx context.Context, id string) (app.ClusterTrace, error) {
	if !trace.ValidID(id) {
		return app.ClusterTrace{}, fmt.Errorf("%w: invalid trace id", store.ErrInvalidInput)
	}
	ids := []int64{s.store.NodeID()}
	if provider, ok := s.eventSink.(interface{ KnownNodeIDs() []int64 }); ok {
		ids = provider.KnownNodeIDs()
	}
	seen := map[int64]bool{s.store.NodeID(): true}
	for _, nodeID := range ids {
		if nodeID > 0 {
			seen[nodeID] = true
		}
	}
	ids = make([]int64, 0, len(seen))
	for nodeID := range seen {
		ids = append(ids, nodeID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := app.ClusterTrace{TraceID: id, Nodes: make([]app.NodeTrace, len(ids))}
	var wg sync.WaitGroup
	workers := min(16, len(ids))
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for i := start; i < len(ids); i += workers {
				entry := app.NodeTrace{NodeID: ids[i], Events: []trace.Event{}}
				response, err := s.queryNodeOperation(ctx, ids[i], app.OperationsRequest{Operation: app.OperationTrace, TraceID: id})
				if err != nil {
					entry.Error = err.Error()
				} else if response.Events != nil {
					entry.Events = response.Events
				}
				result.Nodes[i] = entry
			}
		}(worker)
	}
	wg.Wait()
	return result, nil
}
