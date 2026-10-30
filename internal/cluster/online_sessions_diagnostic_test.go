package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"google.golang.org/protobuf/proto"
)

// 后台 mesh 日志也会写入此处；读取快照时必须与写入同步。
type sessionDiagnosticLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *sessionDiagnosticLogs) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *sessionDiagnosticLogs) events(t *testing.T, name string) []map[string]any {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	var events []map[string]any
	for _, line := range bytes.Split(w.buf.Bytes(), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if event["event"] == name {
			events = append(events, event)
		}
	}
	return events
}

type sessionDiagnosticDialer struct {
	Dialer
	sendErr error
}

func (d *sessionDiagnosticDialer) Dial(ctx context.Context, endpoint string) (TransportConn, error) {
	conn, err := d.Dialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return &sessionDiagnosticConn{TransportConn: conn, sendErr: d.sendErr}, nil
}

type sessionDiagnosticConn struct {
	TransportConn
	sendErr error
}

func (c *sessionDiagnosticConn) Send(ctx context.Context, p []byte) error {
	var env mesh.ClusterEnvelope
	if proto.Unmarshal(p, &env) == nil && env.GetForwardedPacket().GetTrafficClass() == mesh.TrafficControlQuery {
		// 人为占用发送时间，随后失败或丢弃请求以独立控制回复等待阶段。
		timer := time.NewTimer(25 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return c.sendErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.TransportConn.Send(ctx, p)
}

func newSessionDiagnosticSource(t *testing.T, sendErr error) (*Manager, int64) {
	t.Helper()
	target := newMeshClockTestManager(t)
	server := newClusterHTTPTestServer(t, target.Handler())
	if err := target.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	source, err := NewManager(Config{NodeID: testNodeID(2), AdvertisePath: websocketPath, ClusterSecret: "secret", MessageWindowSize: store.DefaultMessageWindowSize, MaxClockSkewMs: DefaultMaxClockSkewMs, DiscoveryDisabled: true, Peers: []Peer{{URL: websocketURL(server.URL) + websocketPath}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := source.BuildMeshRuntime()
	if err != nil {
		t.Fatal(err)
	}
	dialer := &sessionDiagnosticDialer{Dialer: source.websocket, sendErr: sendErr}
	binding.adapters[mesh.TransportWebSocket].dialer = dialer.Dial
	source.meshRuntime = binding
	if err := binding.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binding.Close() })
	waitForMeshRoute(t, source, target.cfg.NodeID, mesh.TrafficControlQuery)
	return source, target.cfg.NodeID
}

func TestSessionLookupDiagnosticStages(t *testing.T) {
	const privateText = "private-session-credentials-user-4097"
	for _, mode := range []string{"no_runtime", "send_failure", "response_deadline", "response_cancel", "response_pending_failure", "response_remote_error", "success"} {
		t.Run(mode, func(t *testing.T) {
			logs := &sessionDiagnosticLogs{}
			previous := log.Logger
			log.Logger = zerolog.New(logs).Level(zerolog.DebugLevel)
			// 注册在 manager 清理之前，确保后台日志结束后再恢复全局 logger。
			t.Cleanup(func() { log.Logger = previous })
			var sendErr error
			if mode == "send_failure" {
				sendErr = fmt.Errorf("%w: %s", context.DeadlineExceeded, privateText)
			}
			var source *Manager
			var targetID int64
			if mode == "no_runtime" {
				source, targetID = newMeshClockTestManager(t), testNodeID(2)
			} else {
				source, targetID = newSessionDiagnosticSource(t, sendErr)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if mode == "response_deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
			}
			done := make(chan error, 1)
			go func() {
				_, err := source.resolveUserSessionsAtNodeUnshared(ctx, targetID, store.UserKey{NodeID: targetID, UserID: 4097})
				done <- err
			}()
			var routed map[string]any
			if mode != "no_runtime" && mode != "send_failure" {
				waitFor(t, time.Second, func() bool {
					events := logs.events(t, "session_lookup_request_routed")
					if len(events) == 1 {
						routed = events[0]
						return true
					}
					return false
				})
				// 保证回复阶段有可测量的独立等待，发现把总耗时误记为发送耗时的回归。
				time.Sleep(30 * time.Millisecond)
				requestID := uint64(routed["query_request_id"].(float64))
				switch mode {
				case "response_cancel":
					cancel()
				case "response_pending_failure":
					source.cancelResolveUserSessionsQuery(requestID, fmt.Errorf("%w: %s", errSessionClosed, privateText))
				case "response_remote_error", "success":
					response := &internalproto.QueryResolveUserSessionsResponse{}
					if mode == "response_remote_error" {
						response.ErrorCode, response.ErrorMessage = "forbidden", privateText
					}
					if !source.resolveResolveUserSessionsQuery(requestID, resolveUserSessionsQueryResult{response: response}) {
						t.Fatal("query was no longer pending")
					}
				}
			}
			var err error
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("lookup did not finish")
			}
			source.mu.Lock()
			pending, requestID := len(source.pendingResolveSessions), source.nextResolveSessionsQueryID
			source.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending queries leaked: %d", pending)
			}
			failures := logs.events(t, "session_lookup_failed")
			if mode == "success" {
				if err != nil || len(failures) != 0 || routed["level"] != "debug" {
					t.Fatalf("successful lookup: err=%v failures=%v routed=%v", err, failures, routed)
				}
				return
			}
			wantErr, kind, stage := app.ErrServiceUnavailable, "deadline_exceeded", "response"
			switch mode {
			case "no_runtime":
				kind, stage = "no_route", "send"
			case "send_failure":
				stage = "send"
			case "response_cancel":
				wantErr, kind = context.Canceled, "canceled"
			case "response_pending_failure":
				kind = "peer_session_closed"
			case "response_remote_error":
				wantErr, kind = store.ErrForbidden, "forbidden"
			}
			if !errors.Is(err, wantErr) || len(failures) != 1 {
				t.Fatalf("err=%v failures=%v", err, failures)
			}
			event := failures[0]
			if event["stage"] != stage || event["error_kind"] != kind || event["level"] != "warn" || event["component"] != "cluster" || event["local_node_id"] != float64(source.cfg.NodeID) || event["target_node_id"] != float64(targetID) || event["query_request_id"] != float64(requestID) {
				t.Fatalf("incorrect diagnostic or correlation fields: %v", event)
			}
			for _, key := range []string{"error_type", "error"} {
				if value, ok := event[key].(string); !ok || value == "" {
					t.Fatalf("missing %s: %v", key, event)
				}
			}
			sendMS, sendOK := event["send_elapsed_ms"].(float64)
			totalMS, totalOK := event["elapsed_ms"].(float64)
			if !sendOK || !totalOK || sendMS < 0 || totalMS < sendMS || (mode != "no_runtime" && sendMS < 20) {
				t.Fatalf("invalid elapsed times: %v", event)
			}
			if stage == "response" && (sendMS != routed["send_elapsed_ms"] || totalMS-sendMS < 20) {
				t.Fatalf("response wait changed send duration: failure=%v routed=%v", event, routed)
			}
			encoded, _ := json.Marshal(event)
			for _, forbidden := range []string{privateText, "4097", "credentials", "session_uuid", "payload"} {
				if strings.Contains(string(encoded), forbidden) {
					t.Fatalf("diagnostic leaked %q: %s", forbidden, encoded)
				}
			}
		})
	}
}
