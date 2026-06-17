// Package main 提供 TurnTF 服务的 CLI 入口点。
// 使用 Cobra 库构建命令行界面，命令树结构为：
//
//	turntf              - 根命令，无参数时默认启动 serve 子命令
//	  ├── serve         - 启动 TurnTF 服务（核心启动流程）
//	  ├── completion    - 生成 Shell 自动补全脚本
//	  ├── hash          - 计算 bcrypt 密码哈希
//	  ├── curve         - 管理 ZeroMQ CURVE 密钥对
//	  │   └── gen       - 生成 CURVE 配置片段
//	  └── password      - 密码相关辅助操作
//
// 服务启动生命周期：配置加载 -> 存储初始化 -> Mesh 网络组建 ->
// HTTP API 服务 -> ZeroMQ 监听 -> 信号处理与优雅关闭。
package main

import (
	"context"
	"os"

	"github.com/rs/zerolog/log"
)

// main 是程序入口点。
// 初始化默认日志记录器后创建 Cobra 根命令，传入标准输入/输出/错误流。
// 命令执行失败时通过 Fatal 日志记录错误并退出。
func main() {
	configureDefaultLogger(os.Stderr)
	cmd := newRootCommand(commandIO{
		Context: context.Background(),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	})
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("command_failed")
	}
}
