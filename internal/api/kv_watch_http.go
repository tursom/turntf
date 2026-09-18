package api

import (
	"encoding/json"
	"net/http"
)

func (h *HTTP) handleKVWatch(w http.ResponseWriter, r *http.Request) {
	principal, service, ok := h.kvPrincipal(w, r)
	if !ok {
		return
	}
	watch, err := service.KVWatch(principal, r.PathValue("database"), r.URL.Query().Get("prefix"))
	if err != nil {
		writeKVError(w, err)
		return
	}
	defer watch.Close()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case change, open := <-watch.C:
			if !open {
				return
			}
			if prefix := r.URL.Query().Get("prefix"); prefix != "" && (len(change.Key) < len(prefix) || change.Key[:len(prefix)] != prefix) {
				continue
			}
			if change.Database != r.PathValue("database") {
				continue
			}
			if err := enc.Encode(change); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
