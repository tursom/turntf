//go:build zeromq

package api

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/pebbe/zmq4"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/cluster"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestClientZeroMQLoginAndPushesBytesMessages(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	addr := nextAPIZeroMQTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := cluster.NewZeroMQMuxListener(addr)
	listener.SetClientAccept(func(conn cluster.TransportConn) {
		testAPI.http.AcceptZeroMQConn(conn)
	})
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("start zeromq mux listener: %v", err)
	}
	defer listener.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	body := []byte{0xff, 0x00, 'z'}
	doJSONWithHeaders(t, testAPI.handler, "POST", userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": body,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, 201)

	socket := dialClientZeroMQ(t, addr)
	defer socket.Close()

	writeClientEnvelopeZMQ(t, socket, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password: "alice-password",
			},
		},
	})

	loginResp := readServerEnvelopeZMQ(t, socket).GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != aliceKey.UserID || loginResp.ProtocolVersion != internalproto.ClientProtocolVersion {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}

	users, err := testAPI.http.ListLoggedInUsers(context.Background())
	if err != nil {
		t.Fatalf("list logged in users: %v", err)
	}
	if len(users) != 1 || users[0].UserID != aliceKey.UserID {
		t.Fatalf("unexpected logged in users: %+v", users)
	}

	pushed := readServerEnvelopeZMQ(t, socket).GetMessagePushed()
	if pushed == nil || !senderMatchesRef(pushed.Message.GetSender(), adminKey) || !bytes.Equal(pushed.Message.GetBody(), body) {
		t.Fatalf("unexpected pushed message: %+v", pushed)
	}
}

func TestClientZeroMQSendMessageRPC(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	addr := nextAPIZeroMQTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := cluster.NewZeroMQMuxListener(addr)
	listener.SetClientAccept(func(conn cluster.TransportConn) {
		testAPI.http.AcceptZeroMQConn(conn)
	})
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("start zeromq mux listener: %v", err)
	}
	defer listener.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	socket := dialClientZeroMQ(t, addr)
	defer socket.Close()
	loginClientZeroMQ(t, socket, aliceKey, "alice-password")

	writeClientEnvelopeZMQ(t, socket, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 42,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("hello over zeromq"),
			},
		},
	})

	resp := readServerEnvelopeZMQ(t, socket).GetSendMessageResponse()
	if resp == nil || resp.RequestId != 42 || resp.GetMessage() == nil || string(resp.GetMessage().GetBody()) != "hello over zeromq" {
		t.Fatalf("unexpected send response: %+v", resp)
	}
}

func TestClientZeroMQCurveLoginAndSendMessageRPC(t *testing.T) {
	t.Parallel()
	if !zmq4.HasCurve() {
		t.Skip("libzmq was built without CURVE support")
	}

	testAPI := newAuthenticatedTestAPI(t)
	addr := nextAPIZeroMQTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	curve := newClientZeroMQCurveTestConfig(t)

	listener := cluster.NewZeroMQMuxListenerWithConfig(addr, curve)
	listener.SetClientAccept(func(conn cluster.TransportConn) {
		testAPI.http.AcceptZeroMQConn(conn)
	})
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("start zeromq curve mux listener: %v", err)
	}
	defer listener.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	socket := dialClientZeroMQWithCurve(t, addr, curve)
	defer socket.Close()
	loginClientZeroMQ(t, socket, aliceKey, "alice-password")

	writeClientEnvelopeZMQ(t, socket, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 43,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("hello over curve zeromq"),
			},
		},
	})
	resp := readServerEnvelopeZMQ(t, socket).GetSendMessageResponse()
	if resp == nil || resp.RequestId != 43 || string(resp.GetMessage().GetBody()) != "hello over curve zeromq" {
		t.Fatalf("unexpected send response: %+v", resp)
	}
}

func nextAPIZeroMQTCPAddress(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen zeromq test port: %v", err)
	}
	defer ln.Close()
	return "tcp://" + ln.Addr().String()
}

func dialClientZeroMQ(t *testing.T, bindURL string) *zmq4.Socket {
	t.Helper()

	socket, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		t.Fatalf("new zeromq socket: %v", err)
	}
	if err := socket.SetLinger(0); err != nil {
		t.Fatalf("set zeromq linger: %v", err)
	}
	if err := socket.SetImmediate(true); err != nil {
		t.Fatalf("set zeromq immediate: %v", err)
	}
	identity := time.Now().UTC().Format("20060102150405.000000000")
	if err := socket.SetIdentity(identity); err != nil {
		t.Fatalf("set zeromq identity: %v", err)
	}
	if err := socket.Connect(bindURL); err != nil {
		t.Fatalf("connect zeromq socket: %v", err)
	}
	data, err := gproto.Marshal(&internalproto.ZeroMQMuxHello{
		Role:            internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLIENT,
		ProtocolVersion: internalproto.ZeroMQMuxProtocolVersion,
	})
	if err != nil {
		t.Fatalf("marshal zeromq mux hello: %v", err)
	}
	if _, err := socket.SendBytes(data, 0); err != nil {
		t.Fatalf("send zeromq mux hello: %v", err)
	}
	return socket
}

func dialClientZeroMQWithCurve(t *testing.T, bindURL string, cfg cluster.ZeroMQConfig) *zmq4.Socket {
	t.Helper()

	socket, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		t.Fatalf("new zeromq socket: %v", err)
	}
	if err := socket.SetLinger(0); err != nil {
		socket.Close()
		t.Fatalf("set zeromq linger: %v", err)
	}
	if err := socket.SetImmediate(true); err != nil {
		socket.Close()
		t.Fatalf("set zeromq immediate: %v", err)
	}
	if err := socket.SetCurveServerkey(cfg.Curve.ServerPublicKey); err != nil {
		socket.Close()
		t.Fatalf("set zeromq curve server key: %v", err)
	}
	if err := socket.SetCurvePublickey(cfg.Curve.ClientPublicKey); err != nil {
		socket.Close()
		t.Fatalf("set zeromq curve client public key: %v", err)
	}
	if err := socket.SetCurveSecretkey(cfg.Curve.ClientSecretKey); err != nil {
		socket.Close()
		t.Fatalf("set zeromq curve client secret key: %v", err)
	}
	identity := time.Now().UTC().Format("20060102150405.000000000")
	if err := socket.SetIdentity(identity); err != nil {
		socket.Close()
		t.Fatalf("set zeromq identity: %v", err)
	}
	if err := socket.Connect(bindURL); err != nil {
		socket.Close()
		t.Fatalf("connect zeromq socket: %v", err)
	}
	data, err := gproto.Marshal(&internalproto.ZeroMQMuxHello{
		Role:            internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLIENT,
		ProtocolVersion: internalproto.ZeroMQMuxProtocolVersion,
	})
	if err != nil {
		socket.Close()
		t.Fatalf("marshal zeromq mux hello: %v", err)
	}
	if _, err := socket.SendBytes(data, 0); err != nil {
		socket.Close()
		t.Fatalf("send zeromq mux hello: %v", err)
	}
	return socket
}

func newClientZeroMQCurveTestConfig(t *testing.T) cluster.ZeroMQConfig {
	t.Helper()

	serverPublic, serverSecret, err := zmq4.NewCurveKeypair()
	if err != nil {
		t.Fatalf("new server curve keypair: %v", err)
	}
	clientPublic, clientSecret, err := zmq4.NewCurveKeypair()
	if err != nil {
		t.Fatalf("new client curve keypair: %v", err)
	}
	return cluster.ZeroMQConfig{
		Enabled:  true,
		Security: cluster.ZeroMQSecurityCurve,
		Curve: cluster.ZeroMQCurveConfig{
			ServerPublicKey:         serverPublic,
			ServerSecretKey:         serverSecret,
			ClientPublicKey:         clientPublic,
			ClientSecretKey:         clientSecret,
			AllowedClientPublicKeys: []string{clientPublic},
		},
	}
}

func loginClientZeroMQ(t *testing.T, socket *zmq4.Socket, key store.UserKey, password string) {
	t.Helper()

	writeClientEnvelopeZMQ(t, socket, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:     &internalproto.UserRef{NodeId: key.NodeID, UserId: key.UserID},
				Password: password,
			},
		},
	})
	loginResp := readServerEnvelopeZMQ(t, socket).GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != key.UserID {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}
}

func writeClientEnvelopeZMQ(t *testing.T, socket *zmq4.Socket, envelope *internalproto.ClientEnvelope) {
	t.Helper()

	data, err := gproto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal client envelope: %v", err)
	}
	if _, err := socket.SendBytes(data, 0); err != nil {
		t.Fatalf("send client envelope: %v", err)
	}
}

func readServerEnvelopeZMQ(t *testing.T, socket *zmq4.Socket) *internalproto.ServerEnvelope {
	t.Helper()

	poller := zmq4.NewPoller()
	poller.Add(socket, zmq4.POLLIN)
	polled, err := poller.Poll(5 * time.Second)
	if err != nil {
		t.Fatalf("poll server envelope: %v", err)
	}
	if len(polled) == 0 {
		t.Fatal("timed out waiting for zeromq server envelope")
	}
	frames, err := socket.RecvMessageBytes(0)
	if err != nil {
		t.Fatalf("recv server envelope: %v", err)
	}
	if len(frames) == 0 {
		t.Fatal("expected zeromq server envelope")
	}
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(frames[len(frames)-1], &envelope); err != nil {
		t.Fatalf("unmarshal server envelope: %v", err)
	}
	return &envelope
}
