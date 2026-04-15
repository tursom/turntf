//go:build !zeromq

package main

import (
	"bytes"
	"crypto/ecdh"
	"testing"
)

func TestGenerateCurveKeypairCompatProducesMatchingPublicKey(t *testing.T) {
	t.Parallel()

	publicKey, secretKey, err := generateCurveKeypair()
	if err != nil {
		t.Fatalf("generate curve keypair: %v", err)
	}
	if len(publicKey) != 40 || len(secretKey) != 40 {
		t.Fatalf("unexpected key lengths: public=%d secret=%d", len(publicKey), len(secretKey))
	}

	publicBytes, err := decodeZ85(publicKey)
	if err != nil {
		t.Fatalf("decode public key: %v", err)
	}
	secretBytes, err := decodeZ85(secretKey)
	if err != nil {
		t.Fatalf("decode secret key: %v", err)
	}
	if len(publicBytes) != 32 || len(secretBytes) != 32 {
		t.Fatalf("unexpected decoded lengths: public=%d secret=%d", len(publicBytes), len(secretBytes))
	}

	privateKey, err := ecdh.X25519().NewPrivateKey(secretBytes)
	if err != nil {
		t.Fatalf("build private key: %v", err)
	}
	if !bytes.Equal(privateKey.PublicKey().Bytes(), publicBytes) {
		t.Fatalf("derived public key does not match generated public key")
	}
}

func TestGenerateCurveKeypairCompatReturnsDifferentKeys(t *testing.T) {
	t.Parallel()

	publicA, secretA, err := generateCurveKeypair()
	if err != nil {
		t.Fatalf("generate first curve keypair: %v", err)
	}
	publicB, secretB, err := generateCurveKeypair()
	if err != nil {
		t.Fatalf("generate second curve keypair: %v", err)
	}

	if publicA == publicB && secretA == secretB {
		t.Fatalf("expected successive generated keypairs to differ")
	}
}

func TestZ85RoundTrip(t *testing.T) {
	t.Parallel()

	plain := []byte{
		0x00, 0x01, 0x02, 0x03,
		0x10, 0x11, 0x12, 0x13,
		0x20, 0x21, 0x22, 0x23,
		0x30, 0x31, 0x32, 0x33,
		0x40, 0x41, 0x42, 0x43,
		0x50, 0x51, 0x52, 0x53,
		0x60, 0x61, 0x62, 0x63,
		0x70, 0x71, 0x72, 0x73,
	}

	encoded, err := encodeZ85(plain)
	if err != nil {
		t.Fatalf("encode z85: %v", err)
	}
	decoded, err := decodeZ85(encoded)
	if err != nil {
		t.Fatalf("decode z85: %v", err)
	}
	if !bytes.Equal(decoded, plain) {
		t.Fatalf("unexpected z85 round trip result")
	}
}
