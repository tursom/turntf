package cluster

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketTransportSendReceivePayload(t *testing.T) {
	transport := newWebSocketTransport()
	accepted := make(chan TransportConn, 1)
	upgradeErrs := make(chan error, 1)
	server := newIPv4WebSocketTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := transport.Upgrade(w, r)
		if err != nil {
			upgradeErrs <- err
			return
		}
		accepted <- conn
	}))
	defer server.Close()

	clientConn, err := transport.Dial(context.Background(), websocketURL(server.URL))
	if err != nil {
		t.Fatalf("dial websocket transport: %v", err)
	}
	defer clientConn.Close()

	serverConn := waitForAcceptedTransport(t, accepted, upgradeErrs)
	defer serverConn.Close()

	clientPayload := []byte("client payload")
	if err := clientConn.Send(context.Background(), clientPayload); err != nil {
		t.Fatalf("send client payload: %v", err)
	}
	gotClientPayload, err := serverConn.Receive(context.Background())
	if err != nil {
		t.Fatalf("receive client payload: %v", err)
	}
	if !bytes.Equal(gotClientPayload, clientPayload) {
		t.Fatalf("unexpected client payload: got=%q want=%q", gotClientPayload, clientPayload)
	}

	serverPayload := []byte("server payload")
	if err := serverConn.Send(context.Background(), serverPayload); err != nil {
		t.Fatalf("send server payload: %v", err)
	}
	gotServerPayload, err := clientConn.Receive(context.Background())
	if err != nil {
		t.Fatalf("receive server payload: %v", err)
	}
	if !bytes.Equal(gotServerPayload, serverPayload) {
		t.Fatalf("unexpected server payload: got=%q want=%q", gotServerPayload, serverPayload)
	}
	if clientConn.Transport() != transportWebSocket || serverConn.Transport() != transportWebSocket {
		t.Fatalf("unexpected transport names: client=%q server=%q", clientConn.Transport(), serverConn.Transport())
	}
	if clientConn.Direction() != "outbound" || serverConn.Direction() != "inbound" {
		t.Fatalf("unexpected directions: client=%q server=%q", clientConn.Direction(), serverConn.Direction())
	}
}

func TestWebSocketTransportRejectsNonBinaryFrame(t *testing.T) {
	transport := newWebSocketTransport()
	receiveErrs := make(chan error, 1)
	upgradeErrs := make(chan error, 1)
	server := newIPv4WebSocketTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := transport.Upgrade(w, r)
		if err != nil {
			upgradeErrs <- err
			return
		}
		defer conn.Close()
		_, err = conn.Receive(context.Background())
		receiveErrs <- err
	}))
	defer server.Close()

	rawConn, _, err := websocket.DefaultDialer.Dial(websocketURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial raw websocket: %v", err)
	}
	defer rawConn.Close()

	if err := rawConn.WriteMessage(websocket.TextMessage, []byte("text frame")); err != nil {
		t.Fatalf("write text frame: %v", err)
	}

	select {
	case err := <-receiveErrs:
		if !errors.Is(err, errNonBinaryWebSocketFrame) {
			t.Fatalf("unexpected receive error: %v", err)
		}
	case err := <-upgradeErrs:
		t.Fatalf("upgrade websocket: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for non-binary frame rejection")
	}
}

func TestWebSocketTransportReceiveReturnsErrorAfterRemoteClose(t *testing.T) {
	transport := newWebSocketTransport()
	accepted := make(chan TransportConn, 1)
	upgradeErrs := make(chan error, 1)
	server := newIPv4WebSocketTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := transport.Upgrade(w, r)
		if err != nil {
			upgradeErrs <- err
			return
		}
		accepted <- conn
	}))
	defer server.Close()

	rawConn, _, err := websocket.DefaultDialer.Dial(websocketURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial raw websocket: %v", err)
	}
	serverConn := waitForAcceptedTransport(t, accepted, upgradeErrs)
	defer serverConn.Close()

	if err := rawConn.Close(); err != nil {
		t.Fatalf("close raw websocket: %v", err)
	}
	if _, err := serverConn.Receive(context.Background()); err == nil {
		t.Fatal("expected receive error after remote close")
	}
}

func waitForAcceptedTransport(t *testing.T, accepted <-chan TransportConn, errs <-chan error) TransportConn {
	t.Helper()
	select {
	case conn := <-accepted:
		return conn
	case err := <-errs:
		t.Fatalf("upgrade websocket: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for accepted websocket transport")
	}
	return nil
}

func websocketURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func newIPv4WebSocketTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ipv4 loopback: %v", err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: handler},
	}
	server.Start()
	return server
}
