package api

import (
	"errors"
	"net/http"

	"github.com/tursom/turntf/internal/kv"
	"github.com/tursom/turntf/internal/permission"
)

type kvCreateDatabaseRequest struct {
	Name string `json:"name"`
}
type kvPutRequest struct {
	Value []byte `json:"value"`
}
type kvTxnRequest struct {
	Compare []kv.Compare `json:"compare"`
	Puts    []kv.Put     `json:"puts"`
	Deletes []string     `json:"deletes"`
}
type kvGrantRequest struct {
	Permission kv.Permission `json:"permission"`
}

func (h *HTTP) kvPrincipal(w http.ResponseWriter, r *http.Request) (kv.Principal, KVService, bool) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return kv.Principal{}, nil, false
	}
	// KV 始终需要可识别的用户；关闭认证时不能解引用 nil 或隐式授予管理员权限。
	if principal == nil {
		writeError(w, http.StatusUnauthorized, "kv requires authentication")
		return kv.Principal{}, nil, false
	}
	if h.service == nil || h.service.kvService == nil {
		writeError(w, http.StatusServiceUnavailable, "kv consensus is not enabled")
		return kv.Principal{}, nil, false
	}
	return kv.Principal{Subject: formatUserSubject(principal.User.Key()), SystemAdmin: permission.IsAdminRole(principal.User.Role)}, h.service.kvService, true
}

func (h *HTTP) handleKVDatabases(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	owner := r.URL.Query().Get("owner")
	if owner != "" {
		key, err := parseUserSubject(owner)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid owner")
			return
		}
		owner = formatUserSubject(key)
	}
	items, rev, err := s.KVDatabases(r.Context(), principal, owner)
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "revision": rev})
}

func (h *HTTP) handleKVAccess(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	access, rev, err := s.KVAccess(r.Context(), principal, r.PathValue("database"))
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"database": access, "revision": rev})
}
func writeKVError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, kv.ErrPermission):
		writeError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, kv.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, kv.ErrCompareFail):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, err.Error())
	}
}
func (h *HTTP) handleKVCreateDatabase(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	var req kvCreateDatabaseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	result, err := s.KVCreateDatabase(r.Context(), principal, req.Name)
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 201, result)
}
func (h *HTTP) handleKVGet(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	entry, rev, err := s.KVGet(r.Context(), principal, r.PathValue("database"), r.PathValue("key"))
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"revision": rev, "entry": entry})
}
func (h *HTTP) handleKVList(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	items, rev, err := s.KVList(r.Context(), principal, r.PathValue("database"), r.URL.Query().Get("prefix"))
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"revision": rev, "items": items})
}
func (h *HTTP) handleKVPut(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	var req kvPutRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	result, err := s.KVPut(r.Context(), principal, r.PathValue("database"), r.PathValue("key"), req.Value)
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
func (h *HTTP) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	result, err := s.KVDelete(r.Context(), principal, r.PathValue("database"), r.PathValue("key"))
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
func (h *HTTP) handleKVTxn(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	var req kvTxnRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	result, err := s.KVTxn(r.Context(), principal, r.PathValue("database"), req.Compare, req.Puts, req.Deletes)
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
func (h *HTTP) handleKVGrant(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	var req kvGrantRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	target := r.PathValue("principal")
	if target != "*" {
		key, err := parseUserSubject(target)
		if err != nil {
			writeError(w, http.StatusBadRequest, "principal must be nodeID:userID or *")
			return
		}
		target = formatUserSubject(key)
	}
	if req.Permission & ^(kv.PermissionRead|kv.PermissionWrite|kv.PermissionAdmin) != 0 {
		writeError(w, http.StatusBadRequest, "invalid database permission")
		return
	}
	result, err := s.KVGrant(r.Context(), principal, r.PathValue("database"), target, req.Permission)
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
func (h *HTTP) handleKVRevoke(w http.ResponseWriter, r *http.Request) {
	principal, s, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	result, err := s.KVRevoke(r.Context(), principal, r.PathValue("database"), r.PathValue("principal"))
	if err != nil {
		writeKVError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
