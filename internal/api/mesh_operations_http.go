package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/trace"
)

func (h *HTTP) requireOpsAdmin(w http.ResponseWriter, r *http.Request) bool {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return false
	}
	if err := h.authorizer.ReadOpsStatus(actorFromPrincipal(principal)); err != nil {
		writeStoreError(w, err)
		return false
	}
	return true
}

func writeOpsError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, app.ErrOpsRateLimited):
		writeError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, err.Error())
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusRequestTimeout, err.Error())
	default:
		writeStoreError(w, err)
	}
}

func (h *HTTP) handleOpsNodeStatus(w http.ResponseWriter, r *http.Request) {
	if !h.requireOpsAdmin(w, r) {
		return
	}
	nodeID, ok := parsePositivePathInt(w, r, "node_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	status, err := h.service.NodeOperationsStatus(ctx, nodeID)
	if err != nil {
		writeOpsError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, status)
}

func (h *HTTP) handleOpsClusterTrace(w http.ResponseWriter, r *http.Request) {
	if !h.requireOpsAdmin(w, r) {
		return
	}
	id := r.PathValue("trace_id")
	if !trace.ValidID(id) {
		writeError(w, http.StatusBadRequest, "invalid trace id")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	result, err := h.service.ClusterTraceEvents(ctx, id)
	if err != nil {
		writeOpsError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, result)
}
