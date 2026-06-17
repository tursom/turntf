package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/tursom/turntf/internal/auth"
)

// newHashCommand 创建 "hash" 子命令，用于生成 bcrypt 密码哈希。
// 支持三种密码输入方式：
//   - --password 标志直接指定
//   - --stdin 标志从标准输入读取
//   - 交互式终端输入（带确认和回显隐藏）
func newHashCommand(ioCfg commandIO) *cobra.Command {
	var password string
	var readStdin bool
	cmd := &cobra.Command{
		Use:   "hash",
		Short: "Generate a bcrypt password hash",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if password != "" && readStdin {
				return fmt.Errorf("--password and --stdin cannot be used together")
			}
			return runHash(password, readStdin, ioCfg.Stdout, ioCfg.Stdin)
		},
	}
	cmd.Flags().StringVar(&password, "password", "", "password to hash")
	cmd.Flags().BoolVar(&readStdin, "stdin", false, "read password from stdin")
	return cmd
}

// runHash 解析密码输入后调用 auth.HashPassword 生成 bcrypt 哈希，输出到标准输出。
func runHash(password string, readStdin bool, stdout io.Writer, stdin io.Reader) error {
	plain, err := resolvePasswordInput(stdout, stdin, password, readStdin)
	if err != nil {
		return err
	}
	hash, err := auth.HashPassword(plain)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, hash)
	return err
}
