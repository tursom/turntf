package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/trace"
)

const (
	meshQueryOperationsRequestKind  = "operations.request.v1"
	meshQueryOperationsResponseKind = "operations.response.v1"
	operationsTimeout               = 4 * time.Second
	maxPendingOperations            = 128
)

type operationsWireRequest struct {
	Request           app.OperationsRequest `json:"request"`
	DeadlineUnixMilli int64                 `json:"deadline_unix_milli"`
}

type pendingOperation struct {
	targetNodeID int64
	result       chan app.OperationsResponse
}

// SetLocalOperationsHandler 在 Service 构建后注入，cluster 无需依赖 api。
func (m *Manager) SetLocalOperationsHandler(handler app.OperationsHandler) {
	// 单节点启动会将 typed nil Manager 作为 EventSink 传给 api.New。
	if m == nil {
		return
	}
	m.mu.Lock()
	m.operationsHandler = handler
	m.mu.Unlock()
}

func validateOperationsRequest(req app.OperationsRequest) error {
	switch req.Operation {
	case app.OperationStatus:
		if req.TargetNodeID == 0 && req.TraceID == "" {
			return nil
		}
	case app.OperationProbe:
		if req.TargetNodeID > 0 && req.TraceID == "" {
			return nil
		}
	case app.OperationTrace:
		if req.TargetNodeID == 0 && trace.ValidID(req.TraceID) {
			return nil
		}
	}
	return fmt.Errorf("%w: invalid mesh operation", store.ErrInvalidInput)
}

// QueryOperations 只经已认证的 mesh 查询通道访问目标，不使用 HTTP 或地址映射。
// pending 在成功、路由失败、取消和关闭时都移除；响应同时匹配请求 ID 和源节点。
func (m *Manager) QueryOperations(ctx context.Context, nodeID int64, req app.OperationsRequest) (app.OperationsResponse, error) {
	if m == nil {
		return app.OperationsResponse{}, app.ErrServiceUnavailable
	}
	if err := validateOperationsRequest(req); err != nil {
		return app.OperationsResponse{}, err
	}
	if nodeID <= 0 {
		return app.OperationsResponse{}, fmt.Errorf("%w: invalid node id", store.ErrInvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, operationsTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return app.OperationsResponse{}, err
	}
	m.mu.Lock()
	if m.ctx == nil || m.ctx.Err() != nil {
		m.mu.Unlock()
		return app.OperationsResponse{}, fmt.Errorf("%w: manager is not running", app.ErrServiceUnavailable)
	}
	lifetime := m.ctx
	if len(m.pendingOperations) >= maxPendingOperations {
		m.mu.Unlock()
		return app.OperationsResponse{}, fmt.Errorf("%w: operations query budget exhausted", app.ErrServiceUnavailable)
	}
	m.nextOperationsID++
	id := m.nextOperationsID
	result := make(chan app.OperationsResponse, 1)
	m.pendingOperations[id] = pendingOperation{targetNodeID: nodeID, result: result}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.pendingOperations, id)
		m.mu.Unlock()
	}()
	// Close 也必须能取消正在等待连接写入的请求，而不仅是响应等待。
	stop := context.AfterFunc(lifetime, cancel)
	defer stop()
	deadline, _ := ctx.Deadline()
	payload, err := json.Marshal(operationsWireRequest{Request: req, DeadlineUnixMilli: deadline.UnixMilli()})
	if err != nil {
		return app.OperationsResponse{}, err
	}
	err = m.routeMeshEnvelope(ctx, nodeID, mesh.TrafficControlQuery, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_QueryRequest{QueryRequest: &mesh.QueryRequest{
			RequestId: id, Kind: meshQueryOperationsRequestKind, Payload: payload,
		}},
	})
	if err != nil {
		return app.OperationsResponse{}, fmt.Errorf("%w: node %d: %w", app.ErrServiceUnavailable, nodeID, err)
	}
	select {
	case <-ctx.Done():
		return app.OperationsResponse{}, ctx.Err()
	case response := <-result:
		if err := ctx.Err(); err != nil {
			return app.OperationsResponse{}, err
		}
		if response.Error != "" {
			cause := app.ErrServiceUnavailable
			switch response.ErrorCode {
			case "rate_limited":
				cause = app.ErrOpsRateLimited
			case "deadline_exceeded":
				cause = context.DeadlineExceeded
			case "canceled":
				cause = context.Canceled
			}
			return app.OperationsResponse{}, fmt.Errorf("%w: node %d: %s", cause, nodeID, response.Error)
		}
		return response, nil
	}
}

// handleOperationsRequest 只做验证和有界准入，耗时的状态读取/ProbeRoute 在独立任务执行。
// Add 与 Close 的取消检查受同一把锁保护，关闭时等待已准入任务退出。
func (m *Manager) handleOperationsRequest(packet *mesh.ForwardedPacket, query *mesh.QueryRequest) error {
	if packet == nil || packet.SourceNodeId <= 0 || packet.TargetNodeId != m.cfg.NodeID || query.RequestId == 0 {
		return errors.New("invalid operations request envelope")
	}
	var wire operationsWireRequest
	if len(query.Payload) > 1024 {
		return errors.New("operations request too large")
	}
	if err := json.Unmarshal(query.Payload, &wire); err != nil {
		return err
	}
	if err := validateOperationsRequest(wire.Request); err != nil {
		return err
	}
	m.mu.Lock()
	if m.ctx == nil || m.ctx.Err() != nil {
		m.mu.Unlock()
		return app.ErrServiceUnavailable
	}
	lifetime, handler := m.ctx, m.operationsHandler
	select {
	case m.operationsSlots <- struct{}{}:
	default:
		m.mu.Unlock()
		return m.sendOperationsResponse(lifetime, packet.SourceNodeId, query.RequestId, app.OperationsResponse{Error: "operations execution budget exhausted"})
	}
	m.wg.Add(1)
	m.mu.Unlock()
	source, id := packet.SourceNodeId, query.RequestId
	go func() {
		defer m.wg.Done()
		defer func() { <-m.operationsSlots }()
		deadline := time.Now().Add(operationsTimeout)
		if supplied := time.UnixMilli(wire.DeadlineUnixMilli); supplied.Before(deadline) {
			deadline = supplied
		}
		ctx, cancel := context.WithDeadline(lifetime, deadline)
		defer cancel()
		var response app.OperationsResponse
		var err error
		switch {
		case ctx.Err() != nil:
			err = ctx.Err()
		case handler == nil:
			err = errors.New("local operations handler is unavailable")
		default:
			response, err = handler(ctx, wire.Request)
		}
		if err != nil {
			response = app.OperationsResponse{Error: err.Error()}
			switch {
			case errors.Is(err, app.ErrOpsRateLimited):
				response.ErrorCode = "rate_limited"
			case errors.Is(err, context.DeadlineExceeded):
				response.ErrorCode = "deadline_exceeded"
			case errors.Is(err, context.Canceled):
				response.ErrorCode = "canceled"
			}
		}
		// 响应队列持有自己的发送上下文；不能使用马上会被 defer cancel 的执行上下文。
		_ = m.sendOperationsResponse(lifetime, source, id, response)
	}()
	return nil
}

func (m *Manager) sendOperationsResponse(ctx context.Context, target int64, id uint64, response app.OperationsResponse) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	binding := m.MeshRuntime()
	if binding == nil {
		return app.ErrServiceUnavailable
	}
	return binding.RouteQueryResponse(ctx, target, &mesh.ClusterEnvelope{
		Body: &mesh.ClusterEnvelope_QueryResponse{QueryResponse: &mesh.QueryResponse{
			RequestId: id, Kind: meshQueryOperationsResponseKind, Payload: payload,
		}},
	})
}

func (m *Manager) handleOperationsResponse(packet *mesh.ForwardedPacket, query *mesh.QueryResponse) error {
	if packet == nil || packet.TargetNodeId != m.cfg.NodeID || query.RequestId == 0 {
		return errors.New("invalid operations response envelope")
	}
	m.mu.Lock()
	pending, ok := m.pendingOperations[query.RequestId]
	m.mu.Unlock()
	if !ok {
		return nil // 迟到或重复的响应不能重新创建 pending。
	}
	if packet.SourceNodeId != pending.targetNodeID {
		return errors.New("operations response source mismatch")
	}
	var response app.OperationsResponse
	if err := json.Unmarshal(query.Payload, &response); err != nil {
		return err
	}
	select {
	case pending.result <- response:
	default:
	}
	return nil
}
