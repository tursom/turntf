package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

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

func writeZshCompletion(root *cobra.Command, out io.Writer) error {
	var buf bytes.Buffer
	if err := root.GenZshCompletion(&buf); err != nil {
		return err
	}
	script := bytes.Replace(buf.Bytes(), []byte("#compdef notifier\n"), []byte("#compdef notifier ./notifier\n"), 1)
	buf.Reset()
	buf.Write(script)
	buf.WriteString(zshGoRunCompletionDispatch())
	_, err := out.Write(buf.Bytes())
	return err
}

func zshGoRunCompletionDispatch() string {
	return `
_notifier_go_dispatch() {
  if [[ ${words[2]-} == run ]]; then
    case ${words[3]-} in
      ./cmd/notifier|cmd/notifier)
        local -a _notifier_root_commands
        local -a _notifier_curve_commands
        local -a _notifier_completion_shells

        _notifier_root_commands=(
          'serve:Start the notifier service'
          'hash:Generate a bcrypt password hash'
          'curve:Manage ZeroMQ CURVE helpers'
          'completion:Generate shell completion scripts'
          'help:Help about any command'
        )
        _notifier_curve_commands=(
          'gen:Generate a ZeroMQ CURVE config snippet'
          'help:Help about any command'
        )
        _notifier_completion_shells=(
          'bash:Generate bash completion'
          'zsh:Generate zsh completion'
          'fish:Generate fish completion'
          'powershell:Generate powershell completion'
        )

        if (( CURRENT == 4 )); then
          _describe -t commands 'notifier command' _notifier_root_commands
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
              _describe -t curve-commands 'curve command' _notifier_curve_commands
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
              _describe -t completion-shells 'completion shell' _notifier_completion_shells
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
  compdef _notifier ./notifier
  compdef -p _notifier '*/notifier'
  compdef _notifier_go_dispatch go
fi
`
}
