//go:build zeromq

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	zmq4 "github.com/pebbe/zmq4/draft"
	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

func TestZeroMQRouterClosesInboundConnWhenDealerDisconnects(t *testing.T) {
	addr := nextZeroMQTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accepted := make(chan TransportConn, 1)
	listener := NewZeroMQMuxListener(addr)
	listener.SetClientAccept(func(conn TransportConn) {
		accepted <- conn
	})
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("start zeromq mux listener: %v", err)
	}
	defer listener.Close()

	dealer := dialZeroMQTestDealer(t, addr, "disconnecting-client")
	writeZeroMQTestMuxHello(t, dealer, internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLIENT)

	var inbound TransportConn
	select {
	case inbound = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for zeromq inbound connection")
	}
	defer inbound.Close()

	if err := dealer.Close(); err != nil {
		t.Fatalf("close zeromq dealer: %v", err)
	}

	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer receiveCancel()
	_, err := inbound.Receive(receiveCtx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("zeromq inbound connection remained open after dealer disconnected")
	}
	if err == nil {
		t.Fatal("expected zeromq inbound connection to close")
	}
}

func TestZeroMQRouterIgnoresUnknownIdentityDisconnectFrame(t *testing.T) {
	addr := nextZeroMQTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accepted := make(chan TransportConn, 1)
	listener := NewZeroMQMuxListener(addr)
	listener.SetClientAccept(func(conn TransportConn) {
		accepted <- conn
	})
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("start zeromq mux listener: %v", err)
	}
	defer listener.Close()

	dealer := dialZeroMQTestDealer(t, addr, "unknown-empty-frame")
	defer dealer.Close()
	if _, err := dealer.SendBytes(nil, 0); err != nil {
		t.Fatalf("send empty zeromq frame: %v", err)
	}
	writeZeroMQTestMuxHello(t, dealer, internalproto.ZeroMQMuxHello_ZERO_MQ_ROLE_CLIENT)

	select {
	case inbound := <-accepted:
		defer inbound.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("listener stopped accepting after unknown identity empty frame")
	}
}

func TestZeroMQDealerMonitorClosesOutboundConnWhenRouterDisconnects(t *testing.T) {
	addr := nextZeroMQTCPAddress(t)
	router, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		t.Fatalf("create zeromq router: %v", err)
	}
	if err := router.SetLinger(0); err != nil {
		_ = router.Close()
		t.Fatalf("set zeromq router linger: %v", err)
	}
	if err := router.Bind(addr); err != nil {
		_ = router.Close()
		t.Fatalf("bind zeromq router: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := newZeroMQDialerWithConfig(ZeroMQConfig{}, nil).Dial(ctx, "zmq+"+addr)
	if err != nil {
		_ = router.Close()
		t.Fatalf("dial zeromq router: %v", err)
	}
	defer conn.Close()

	poller := zmq4.NewPoller()
	poller.Add(router, zmq4.POLLIN)
	polled, err := poller.Poll(2 * time.Second)
	if err != nil {
		_ = router.Close()
		t.Fatalf("poll zeromq mux hello: %v", err)
	}
	if len(polled) == 0 {
		_ = router.Close()
		t.Fatal("timed out waiting for zeromq mux hello")
	}
	if _, err := router.RecvMessageBytes(0); err != nil {
		_ = router.Close()
		t.Fatalf("receive zeromq mux hello: %v", err)
	}
	if err := router.Close(); err != nil {
		t.Fatalf("close zeromq router: %v", err)
	}

	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer receiveCancel()
	_, err = conn.Receive(receiveCtx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("zeromq outbound connection remained open after router disconnected")
	}
	if err == nil {
		t.Fatal("expected zeromq outbound connection to close")
	}
}

func dialZeroMQTestDealer(t *testing.T, addr, identity string) *zmq4.Socket {
	t.Helper()

	socket, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		t.Fatalf("create zeromq dealer: %v", err)
	}
	if err := socket.SetLinger(0); err != nil {
		_ = socket.Close()
		t.Fatalf("set zeromq dealer linger: %v", err)
	}
	if err := socket.SetIdentity(identity); err != nil {
		_ = socket.Close()
		t.Fatalf("set zeromq dealer identity: %v", err)
	}
	if err := socket.Connect(addr); err != nil {
		_ = socket.Close()
		t.Fatalf("connect zeromq dealer: %v", err)
	}
	return socket
}

func writeZeroMQTestMuxHello(t *testing.T, socket *zmq4.Socket, role internalproto.ZeroMQMuxHello_Role) {
	t.Helper()

	payload, err := gproto.Marshal(&internalproto.ZeroMQMuxHello{
		Role:            role,
		ProtocolVersion: internalproto.ZeroMQMuxProtocolVersion,
	})
	if err != nil {
		t.Fatalf("marshal zeromq mux hello: %v", err)
	}
	if _, err := socket.SendBytes(payload, 0); err != nil {
		t.Fatalf("send zeromq mux hello: %v", err)
	}
}
