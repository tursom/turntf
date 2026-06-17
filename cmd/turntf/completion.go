package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// newCompletionCommand 创建 "completion" 子命令，用于生成 Shell 自动补全脚本。
// 支持 bash、zsh、fish、powershell 四种 Shell。
// zsh 模式下额外注入 go run 补全分发，支持开发时通过 "go run ./cmd/turntf" 补全。
func newCompletionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:       "completion [bash|zsh|fish|powershell]",
		Short:     "Generate shell completion scripts",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
		RunE: func(cmd *cobra.Command, args []string) error {
			root := cmd.Root()
			out := cmd.OutOrStdout()
			switch args[0] {
			case "bash":
				return root.GenBashCompletion(out)
			case "zsh":
				return writeZshCompletion(root, out)
			case "fish":
				return root.GenFishCompletion(out, true)
			case "powershell":
				return root.GenPowerShellCompletion(out)
			default:
				return fmt.Errorf("unsupported completion shell %q", args[0])
			}
		},
	}
	return cmd
}

// writeZshCompletion 生成 Zsh 补全脚本。
// 在标准 Cobra 生成的 Zsh 补全基础上额外注入 go run 补全分发，使
// "go run ./cmd/turntf [subcommand]" 也能获得自动补全支持。
func writeZshCompletion(root *cobra.Command, out io.Writer) error {
	var buf bytes.Buffer
	if err := root.GenZshCompletion(&buf); err != nil {
		return err
	}
	script := bytes.Replace(buf.Bytes(), []byte("#compdef turntf\n"), []byte("#compdef turntf ./turntf\n"), 1)
	buf.Reset()
	buf.Write(script)
	buf.WriteString(zshGoRunCompletionDispatch())
	_, err := out.Write(buf.Bytes())
	return err
}

// zshGoRunCompletionDispatch 返回 Zsh 补全分发函数 _turntf_go_dispatch 的定义。
// 该函数通过 compdef 注册到 "go" 命令，当检测到 "go run ./cmd/turntf" 时
// 提供子命令（serve、hash、curve、completion）和标志补全。
func zshGoRunCompletionDispatch() string {
	return `
	_turntf_go_dispatch() {
	  if [[ ${words[2]-} == run ]]; then
	    case ${words[3]-} in
	      ./cmd/turntf|cmd/turntf)
	        local -a _turntf_root_commands
	        local -a _turntf_curve_commands
	        local -a _turntf_completion_shells

	        _turntf_root_commands=(
	          'serve:Start the TurnTF service'
	          'hash:Generate a bcrypt password hash'
	          'curve:Manage ZeroMQ CURVE helpers'
	          'completion:Generate shell completion scripts'
	          'help:Help about any command'
	        )
	        _turntf_curve_commands=(
	          'gen:Generate a ZeroMQ CURVE config snippet'
	          'help:Help about any command'
	        )
	        _turntf_completion_shells=(
	          'bash:Generate bash completion'
	          'zsh:Generate zsh completion'
	          'fish:Generate fish completion'
	          'powershell:Generate powershell completion'
	        )

	        if (( CURRENT == 4 )); then
	          _describe -t commands 'turntf command' _turntf_root_commands
	          return
	        fi

	        case ${words[4]-} in
	          serve)
	            if [[ ${words[CURRENT-1]-} == --config || ${words[CURRENT-1]-} == -c ]]; then
	              _files
	              return
	            fi
	            compadd -h --help --config -c
	            return
	            ;;
	          hash)
	            if [[ ${words[CURRENT-1]-} == --password ]]; then
	              return
	            fi
	            compadd -h --help --password --stdin
	            return
	            ;;
	          curve)
	            if (( CURRENT == 5 )); then
	              _describe -t curve-commands 'curve command' _turntf_curve_commands
	              return
	            fi
	            case ${words[5]-} in
	              gen)
	                compadd -h --help
	                return
	                ;;
	            esac
	            ;;
	          completion)
	            if (( CURRENT == 5 )); then
	              _describe -t completion-shells 'completion shell' _turntf_completion_shells
	              return
	            fi
	            ;;
	        esac
	        return
	        ;;
	    esac
	  fi
	  if (( $+functions[_go] )); then
	    _go
	  fi
	}

	if (( $+functions[compdef] )); then
	  compdef _turntf ./turntf
	  compdef -p _turntf '*/turntf'
	  compdef _turntf_go_dispatch go
	fi
	`
}
