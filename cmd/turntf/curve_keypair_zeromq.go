//go:build zeromq

package main

import "github.com/pebbe/zmq4"

func generateCurveKeypair() (publicKey string, secretKey string, err error) {
	return zmq4.NewCurveKeypair()
}
