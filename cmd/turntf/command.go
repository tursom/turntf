package main

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// commandIO 封装命令执行所需的上下文和标准输入/输出/错误流。
// 用于解耦命令与全局 os.Stdin/Stdout/Stderr，方便测试时注入模拟流。
type commandIO struct {
	// Context 用于控制命令的运行生命周期，支持超时和取消
	Context context.Context
	// Stdin 标准输入流
	Stdin io.Reader
	// Stdout 标准输出流
	Stdout io.Writer
	// Stderr 标准错误流
	Stderr io.Writer
}

// run 是测试辅助函数，用给定参数和标准输出执行命令，丢弃标准错误输出。
func run(args []string, stdout io.Writer) error {
	return runWithIO(args, commandIO{
		Context: context.Background(),
		Stdin:   os.Stdin,
		Stdout:  stdout,
		Stderr:  io.Discard,
	})
}

// runWithIO 是测试辅助函数，用完整的 IO 配置执行命令。
// 用于需要在测试中控制输入流或观察错误输出的场景。
func runWithIO(args []string, ioCfg commandIO) error {
	cmd := newRootCommand(ioCfg)
	cmd.SetArgs(args)
	return cmd.Execute()
}

// newRootCommand 创建 Cobra 根命令。
// 根命令的 Use 名称为 "turntf"；无参数运行时默认执行 serveRuntime 启动服务。
// 注册以下子命令：
//   - serve：启动服务
//   - hash：计算密码哈希
//   - curve：管理 ZeroMQ CURVE 密钥对
//   - completion：生成 Shell 自动补全脚本
func newRootCommand(ioCfg commandIO) *cobra.Command {
	ioCfg = normalizeCommandIO(ioCfg)
	cmd := &cobra.Command{
		Use:           "turntf",
		Short:         "Run the TurnTF service and helper commands",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serveRuntime(cmd.Context(), defaultConfigPath, ioCfg.Stderr)
		},
	}
	cmd.SetIn(ioCfg.Stdin)
	cmd.SetOut(ioCfg.Stdout)
	cmd.SetErr(ioCfg.Stderr)
	cmd.SetContext(ioCfg.Context)
	cmd.CompletionOptions.DisableDefaultCmd = true

	cmd.AddCommand(newServeCommand(ioCfg))
	cmd.AddCommand(newHashCommand(ioCfg))
	cmd.AddCommand(newCurveCommand())
	cmd.AddCommand(newCompletionCommand())
	return cmd
}

// normalizeCommandIO 为 commandIO 的空值字段设置默认值：
// Context 默认为 context.Background()，
// Stdin 默认为 os.Stdin，
// Stdout 和 Stderr 默认为 io.Discard（丢弃输出）。
func normalizeCommandIO(ioCfg commandIO) commandIO {
	if ioCfg.Context == nil {
		ioCfg.Context = context.Background()
	}
	if ioCfg.Stdin == nil {
		ioCfg.Stdin = io.Reader(os.Stdin)
	}
	if ioCfg.Stdout == nil {
		ioCfg.Stdout = io.Discard
	}
	if ioCfg.Stderr == nil {
		ioCfg.Stderr = io.Discard
	}
	return ioCfg
}
