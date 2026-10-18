package app

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/tursom/turntf/internal/trace"
)

const (
	OperationStatus = "status"
	OperationProbe  = "probe"
	OperationTrace  = "trace"
)

var ErrOpsRateLimited = errors.New("probe rate limit exceeded")

// OperationsRequest 仅允许三个本地运维操作，不包含 HTTP 地址、路径或认证令牌。
// 请求发往哪个节点由 mesh 信封指定；TargetNodeID 仅用于该节点发起探测。
type OperationsRequest struct {
	Operation    string `json:"operation"`
	TargetNodeID int64  `json:"target_node_id,omitempty"`
	TraceID      string `json:"trace_id,omitempty"`
}

// ProbeResult 与本机 /ops/probes 的 JSON 契约一致；dispatched 不代表抵达。
type ProbeResult struct {
	TraceID      string `json:"trace_id"`
	SourceNodeID int64  `json:"source_node_id"`
	TargetNodeID int64  `json:"target_node_id"`
	Status       string `json:"status"`
}

// OperationsResponse 的 Status 保留 /ops/status 的原始 JSON 和 int64 精度。
type OperationsResponse struct {
	Status    json.RawMessage `json:"status,omitempty"`
	Probe     *ProbeResult    `json:"probe,omitempty"`
	Events    []trace.Event   `json:"events,omitempty"`
	Error     string          `json:"error,omitempty"`
	ErrorCode string          `json:"error_code,omitempty"`
}

type OperationsHandler func(context.Context, OperationsRequest) (OperationsResponse, error)

type NodeTrace struct {
	NodeID int64         `json:"node_id"`
	Events []trace.Event `json:"events"`
	Error  string        `json:"error,omitempty"`
}

type ClusterTrace struct {
	TraceID string      `json:"trace_id"`
	Nodes   []NodeTrace `json:"nodes"`
}
