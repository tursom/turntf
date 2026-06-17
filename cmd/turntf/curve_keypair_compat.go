//go:build !zeromq

package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"fmt"
)

// generateCurveKeypair 生成一个 CurveZMQ 密钥对（兼容实现，不使用 zmq4 库）。
//
// 此文件通过 //go:build !zeromq 构建约束在未启用 zmq4 标签时编译，
// 作为 curve_keypair_zeromq.go 的替代实现。它使用 Go 标准库 crypto/ecdh
// 通过 X25519 椭圆曲线生成密钥对，与 ZeroMQ 的 CURVE 安全机制兼容。
//
// CurveZMQ 基于 Curve25519 椭圆曲线，密钥对包含:
//   - 公钥 (publicKey): 32 字节 X25519 公钥，经 Z85 编码为 40 字符可打印字符串
//   - 私钥 (secretKey): 32 字节随机种子（即 X25519 私钥的原始字节），经 Z85 编码为 40 字符可打印字符串
//
// 返回值:
//   - publicKey: Z85 编码的公钥字符串（40 字符）。
//   - secretKey: Z85 编码的私钥字符串（40 字符）。
//   - err: 生成过程中的任何错误。
//
// 注意: Curve25519 的私钥就是 32 字节随机数，无需像传统椭圆曲线那样单独生成。
//
// 兼容性说明: 与 zmq4 的 NewCurveKeypair 生成相同格式的密钥，可互换使用。
func generateCurveKeypair() (publicKey string, secretKey string, err error) {
	// 生成 32 字节随机数作为 X25519 私钥种子
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", fmt.Errorf("read curve secret: %w", err)
	}

	// 通过 crypto/ecdh 库从种子构造 X25519 私钥，
	// 此步骤同时验证种子的合法性（X25519 会对密钥进行钳制处理）
	privateKey, err := ecdh.X25519().NewPrivateKey(secret)
	if err != nil {
		return "", "", fmt.Errorf("build curve private key: %w", err)
	}

	// 将 32 字节公钥编码为 Z85 字符串
	publicKey, err = encodeZ85(privateKey.PublicKey().Bytes())
	if err != nil {
		return "", "", fmt.Errorf("encode curve public key: %w", err)
	}
	// 将 32 字节私钥种子编码为 Z85 字符串
	secretKey, err = encodeZ85(secret)
	if err != nil {
		return "", "", fmt.Errorf("encode curve secret key: %w", err)
	}
	return publicKey, secretKey, nil
}
