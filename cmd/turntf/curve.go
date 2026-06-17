package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// curveConfig 保存 ZeroMQ CURVE 安全机制的完整密钥配置。
// 包含服务器和客户端各自的公私钥对，用于配置 TOML 配置文件中的 [services.zeromq.curve] 段。
// CurveZMQ 使用 Curve25519 椭圆曲线进行双向认证和加密通信。
type curveConfig struct {
	// ServerPublicKey 服务端公钥（Z85 编码，40 字符）
	ServerPublicKey string
	// ServerSecretKey 服务端私钥（Z85 编码，40 字符）
	ServerSecretKey string
	// ClientPublicKey 客户端公钥（Z85 编码，40 字符）
	ClientPublicKey string
	// ClientSecretKey 客户端私钥（Z85 编码，40 字符）
	ClientSecretKey string
}

// newCurveCommand 创建 curve 子命令，用于管理 ZeroMQ CURVE 密钥对。
// 该命令本身不执行任何操作（输出帮助信息），主要通过其子命令 gen 来生成密钥对。
func newCurveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "curve",
		Short: "Manage ZeroMQ CURVE helpers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "gen",
		Short: "Generate a ZeroMQ CURVE config snippet",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCurveGen(cmd.OutOrStdout())
		},
	})
	return cmd
}

// runCurveGen 生成一套完整的 CURVE 密钥对配置并以 TOML 格式输出。
// 它生成服务端和客户端各一对密钥，并将客户端公钥自动加入 allowed_client_public_keys 列表，
// 输出可直接复制到配置文件中使用。
// stdout: 输出写入的目标写入器（通常是 os.Stdout）。
func runCurveGen(stdout io.Writer) error {
	cfg, err := generateCurveConfig()
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(stdout, `[services.zeromq]
security = "curve"

[services.zeromq.curve]
server_public_key = %q
server_secret_key = %q
client_public_key = %q
client_secret_key = %q
allowed_client_public_keys = [%q]
`, cfg.ServerPublicKey, cfg.ServerSecretKey, cfg.ClientPublicKey, cfg.ClientSecretKey, cfg.ClientPublicKey)
	return err
}

// generateCurveConfig 生成服务端和客户端两套 CurveZMQ 密钥对并组装为 curveConfig。
// 每次调用都会生成全新的随机密钥对，确保 server 和 client 的密钥互不相同。
// 生成过程中如果任一套密钥对生成失败，立即返回错误。
func generateCurveConfig() (curveConfig, error) {
	serverPublicKey, serverSecretKey, err := generateCurveKeypair()
	if err != nil {
		return curveConfig{}, err
	}
	clientPublicKey, clientSecretKey, err := generateCurveKeypair()
	if err != nil {
		return curveConfig{}, err
	}
	return curveConfig{
		ServerPublicKey: serverPublicKey,
		ServerSecretKey: serverSecretKey,
		ClientPublicKey: clientPublicKey,
		ClientSecretKey: clientSecretKey,
	}, nil
}
