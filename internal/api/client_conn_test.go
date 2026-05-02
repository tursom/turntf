package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

var wsUpgrader = websocket.Upgrader{}

// TestClientWSConnReceiveResetsReadDeadline 验证每次 Receive() 调用前都会重置读取截止时间，
// 使得只要客户端持续发送数据，连接就不会因 WebSocket 读超时而断开。
func TestClientWSConnReceiveResetsReadDeadline(t *testing.T) {
	t.Parallel()

	// 使用 pingInterval 间隔发送健康检查消息，确保远大于 clientWSReadTimeout 后仍能正常接收
	pingInterval := 10 * time.Second
	testDuration := 3*clientWSReadTimeout + 20*time.Second

	serverReady := make(chan struct{})
	var serverConn *clientWSConn
	serverDone := make(chan error, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		serverConn = newClientWSConn(conn)
		close(serverReady)

		// 不断调用 Receive()，验证即使超过 clientWSReadTimeout 也不会超时
		for {
			_, err := serverConn.Receive(context.Background())
			if err != nil {
				serverDone <- err
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]
	client, _, err := websocket.DefaultDialer.Dial(wsURL+"/ws/client", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	<-serverReady

	start := time.Now()
	pingCount := 0

	// 以 pingInterval 间隔循环发送 protobuf Ping 消息，验证整个 testDuration 内连接保持活跃
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	timeout := time.After(testDuration)
loop:
	for {
		select {
		case err := <-serverDone:
			t.Fatalf("server receive returned error after %v (%d pings sent): %v", time.Since(start), pingCount, err)
		case <-timeout:
			break loop
		case <-ticker.C:
			pingCount++
			pingEnvelope := &internalproto.ClientEnvelope{
				Body: &internalproto.ClientEnvelope_Ping{
					Ping: &internalproto.Ping{RequestId: uint64(pingCount)},
				},
			}
			data, err := gproto.Marshal(pingEnvelope)
			if err != nil {
				t.Fatalf("marshal ping: %v", err)
			}
			if err := client.WriteMessage(websocket.BinaryMessage, data); err != nil {
				t.Fatalf("write ping #%d: %v", pingCount, err)
			}
		}
	}

	if pingCount == 0 {
		t.Fatal("expected at least one ping to be sent")
	}
	t.Logf("connection survived %v with %d pings (%v each) - read deadline was properly reset",
		time.Since(start), pingCount, pingInterval)
}

// TestClientWSConnReceiveTimesOutWithoutData 验证当超过 clientWSReadTimeout 无数据到达时，
// Receive() 会返回读取超时错误。
func TestClientWSConnReceiveTimesOutWithoutData(t *testing.T) {
	t.Parallel()

	serverReady := make(chan *clientWSConn, 1)
	serverDone := make(chan error, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		wsc := newClientWSConn(conn)
		serverReady <- wsc

		// 第一次 Receive() 应成功（接收客户端发来的登录消息）
		_, err = wsc.Receive(context.Background())
		if err != nil {
			serverDone <- err
			return
		}
		// 第二次 Receive() 应超时（客户端不再发送任何数据）
		start := time.Now()
		_, err = wsc.Receive(context.Background())
		elapsed := time.Since(start)
		t.Logf("second receive returned after %v: %v", elapsed, err)

		// 确认返回的是读取超时错误，且用时接近 clientWSReadTimeout
		if err == nil {
			serverDone <- nil
			return
		}
		if elapsed < clientWSReadTimeout {
			t.Errorf("expected read timeout after at least %v, got %v", clientWSReadTimeout, elapsed)
		}
		serverDone <- err
	}))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]
	client, _, err := websocket.DefaultDialer.Dial(wsURL+"/ws/client", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	wsc := <-serverReady
	defer wsc.Close()

	// 发送一条消息触发第一次成功的 Receive()
	pingEnvelope := &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Ping{
			Ping: &internalproto.Ping{RequestId: 1},
		},
	}
	data, _ := gproto.Marshal(pingEnvelope)
	if err := client.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write initial ping: %v", err)
	}

	err = <-serverDone
	if err == nil {
		t.Fatal("expected read timeout error, got nil")
	}
	t.Logf("expected timeout: %v", err)
}
