//go:build zeromq

package main

import "github.com/pebbe/zmq4"

// generateCurveKeypair 生成一个 CurveZMQ 密钥对（zmq4 原生实现）。
//
// 此文件通过 //go:build zeromq 构建约束在启用 zmq4 标签时编译，
// 直接委托给 zmq4 库的 NewCurveKeypair 函数生成密钥对。
//
// 与 curve_keypair_compat.go 的关系：
//   - 当存在 zmq4 库时使用本实现，直接调用 ZeroMQ 原生的密钥生成函数
//   - 当没有 zmq4 库时使用 curve_keypair_compat.go 中的纯 Go 实现
//   - 两者生成完全兼容的 CurveZMQ 密钥对，可以互换使用
//
// 返回值:
//   - publicKey: Z85 编码的公钥字符串（40 字符）。
//   - secretKey: Z85 编码的私钥字符串（40 字符）。
//   - err: zmq4 库返回的任何错误。
func generateCurveKeypair() (publicKey string, secretKey string, err error) {
	return zmq4.NewCurveKeypair()
}
