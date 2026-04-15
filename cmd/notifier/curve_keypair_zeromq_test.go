//go:build zeromq

package main

import (
	"testing"

	"github.com/pebbe/zmq4"
)

func TestGenerateCurveKeypairUsesZeroMQCompatibleFormat(t *testing.T) {
	t.Parallel()

	if !zmq4.HasCurve() {
		t.Skip("libzmq was built without CURVE support")
	}

	serverPublicKey, serverSecretKey, err := generateCurveKeypair()
	if err != nil {
		t.Fatalf("generate server curve keypair: %v", err)
	}
	clientPublicKey, clientSecretKey, err := generateCurveKeypair()
	if err != nil {
		t.Fatalf("generate client curve keypair: %v", err)
	}

	serverSocket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		t.Fatalf("new server socket: %v", err)
	}
	defer serverSocket.Close()
	if err := serverSocket.SetCurveServer(1); err != nil {
		t.Fatalf("set curve server mode: %v", err)
	}
	if err := serverSocket.SetCurveSecretkey(serverSecretKey); err != nil {
		t.Fatalf("set curve server secret key: %v", err)
	}

	clientSocket, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		t.Fatalf("new client socket: %v", err)
	}
	defer clientSocket.Close()
	if err := clientSocket.SetCurveServerkey(serverPublicKey); err != nil {
		t.Fatalf("set curve server public key: %v", err)
	}
	if err := clientSocket.SetCurvePublickey(clientPublicKey); err != nil {
		t.Fatalf("set curve client public key: %v", err)
	}
	if err := clientSocket.SetCurveSecretkey(clientSecretKey); err != nil {
		t.Fatalf("set curve client secret key: %v", err)
	}
}
