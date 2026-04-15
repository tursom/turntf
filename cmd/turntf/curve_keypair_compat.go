//go:build !zeromq

package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"fmt"
)

func generateCurveKeypair() (publicKey string, secretKey string, err error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", fmt.Errorf("read curve secret: %w", err)
	}

	privateKey, err := ecdh.X25519().NewPrivateKey(secret)
	if err != nil {
		return "", "", fmt.Errorf("build curve private key: %w", err)
	}

	publicKey, err = encodeZ85(privateKey.PublicKey().Bytes())
	if err != nil {
		return "", "", fmt.Errorf("encode curve public key: %w", err)
	}
	secretKey, err = encodeZ85(secret)
	if err != nil {
		return "", "", fmt.Errorf("encode curve secret key: %w", err)
	}
	return publicKey, secretKey, nil
}
