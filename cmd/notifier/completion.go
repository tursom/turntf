package main

import (
	"fmt"
	"io"
	"strings"
)

type completionFlag struct {
	Name        string
	Description string
	ValueName   string
	IsPath      bool
}

type completionCommand struct {
	Name        string
	Description string
	Flags       []completionFlag
	Subcommands []completionCommand
}

var notifierCompletionSpec = completionCommand{
	Name: "notifier",
	Subcommands: []completionCommand{
		{
			Name:        "serve",
			Description: "start notifier service",
			Flags: []completionFlag{
				{Name: "-h", Description: "show help"},
				{Name: "--help", Description: "show help"},
				{Name: "-config", Description: "path to TOML config file", ValueName: "config file", IsPath: true},
			},
		},
		{
			Name:        "hash",
			Description: "generate password hash",
			Flags: []completionFlag{
				{Name: "-h", Description: "show help"},
				{Name: "--help", Description: "show help"},
				{Name: "-password", Description: "password to hash", ValueName: "password"},
				{Name: "-stdin", Description: "read password from stdin"},
			},
		},
		{
			Name:        "curve",
			Description: "manage ZeroMQ CURVE helpers",
			Flags: []completionFlag{
				{Name: "-h", Description: "show help"},
				{Name: "--help", Description: "show help"},
			},
			Subcommands: []completionCommand{
				{
					Name:        "gen",
					Description: "generate ZeroMQ CURVE config snippet",
					Flags: []completionFlag{
						{Name: "-h", Description: "show help"},
						{Name: "--help", Description: "show help"},
					},
				},
				{
					Name:        "help",
					Description: "show help",
				},
				{
					Name:        "-h",
					Description: "show help",
				},
				{
					Name:        "--help",
					Description: "show help",
				},
			},
		},
		{
			Name:        "completion",
			Description: "generate shell completion scripts",
			Flags: []completionFlag{
				{Name: "-h", Description: "show help"},
				{Name: "--help", Description: "show help"},
			},
			Subcommands: []completionCommand{
				{
					Name:        "zsh",
					Description: "generate zsh completion script",
				},
				{
					Name:        "help",
					Description: "show help",
				},
				{
					Name:        "-h",
					Description: "show help",
				},
				{
					Name:        "--help",
					Description: "show help",
				},
			},
		},
		{
			Name:        "help",
			Description: "show help",
		},
		{
			Name:        "-h",
			Description: "show help",
		},
		{
			Name:        "--help",
			Description: "show help",
		},
	},
}

func runCompletion(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		printCompletionUsage(stdout)
		return nil
	}

	switch args[0] {
	case "help", "-h", "--help":
		printCompletionUsage(stdout)
		return nil
	case "zsh":
		_, err := io.WriteString(stdout, zshCompletionScript())
		return err
	default:
		return fmt.Errorf("unknown completion shell %q\n\n%s", args[0], completionUsageText())
	}
}

func printCompletionUsage(w io.Writer) {
	fmt.Fprintln(w, completionUsageText())
}

func completionUsageText() string {
	return "usage:\n  notifier completion zsh\n  notifier completion help"
}

func zshCompletionScript() string {
	var b strings.Builder
	b.WriteString("#compdef notifier\n\n")
	b.WriteString("_notifier() {\n")
	b.WriteString("  local context state line\n")
	b.WriteString("  typeset -A opt_args\n\n")
	b.WriteString("  local -a _notifier_root_commands\n")
	b.WriteString("  local -a _notifier_curve_commands\n")
	b.WriteString("  local -a _notifier_completion_commands\n\n")
	b.WriteString("  _notifier_root_commands=(\n")
	writeDescribeArray(&b, notifierCompletionSpec.Subcommands)
	b.WriteString("  )\n\n")
	b.WriteString("  _notifier_curve_commands=(\n")
	writeDescribeArray(&b, commandSpec("curve").Subcommands)
	b.WriteString("  )\n\n")
	b.WriteString("  _notifier_completion_commands=(\n")
	writeDescribeArray(&b, commandSpec("completion").Subcommands)
	b.WriteString("  )\n\n")
	b.WriteString("  _arguments -C \\\n")
	writeArgumentSpecs(&b, nil, "  ")
	b.WriteString("    '1:command:->command' \\\n")
	b.WriteString("    '*::arg:->args'\n\n")
	b.WriteString("  case $state in\n")
	b.WriteString("    command)\n")
	b.WriteString("      _describe -t commands 'notifier command' _notifier_root_commands\n")
	b.WriteString("      return\n")
	b.WriteString("      ;;\n")
	b.WriteString("    args)\n")
	b.WriteString("      case $words[2] in\n")
	b.WriteString("        serve)\n")
	b.WriteString("          _arguments \\\n")
	writeArgumentSpecs(&b, commandSpec("serve").Flags, "  ")
	b.WriteString("          return\n")
	b.WriteString("          ;;\n")
	b.WriteString("        hash)\n")
	b.WriteString("          _arguments \\\n")
	writeArgumentSpecs(&b, commandSpec("hash").Flags, "  ")
	b.WriteString("          return\n")
	b.WriteString("          ;;\n")
	b.WriteString("        curve)\n")
	b.WriteString("          if (( CURRENT == 3 )); then\n")
	b.WriteString("            _describe -t curve-commands 'curve command' _notifier_curve_commands\n")
	b.WriteString("            return\n")
	b.WriteString("          fi\n")
	b.WriteString("          case $words[3] in\n")
	b.WriteString("            gen)\n")
	b.WriteString("              _arguments \\\n")
	writeArgumentSpecs(&b, commandSpec("curve", "gen").Flags, "  ")
	b.WriteString("              return\n")
	b.WriteString("              ;;\n")
	b.WriteString("          esac\n")
	b.WriteString("          ;;\n")
	b.WriteString("        completion)\n")
	b.WriteString("          if (( CURRENT == 3 )); then\n")
	b.WriteString("            _describe -t completion-commands 'completion command' _notifier_completion_commands\n")
	b.WriteString("            return\n")
	b.WriteString("          fi\n")
	b.WriteString("          ;;\n")
	b.WriteString("      esac\n")
	b.WriteString("      ;;\n")
	b.WriteString("  esac\n")
	b.WriteString("}\n\n")
	b.WriteString("_notifier_go_dispatch() {\n")
	b.WriteString("  local -a _notifier_root_commands\n")
	b.WriteString("  local -a _notifier_curve_commands\n")
	b.WriteString("  local -a _notifier_completion_commands\n\n")
	b.WriteString("  _notifier_root_commands=(\n")
	writeDescribeArray(&b, notifierCompletionSpec.Subcommands)
	b.WriteString("  )\n\n")
	b.WriteString("  _notifier_curve_commands=(\n")
	writeDescribeArray(&b, commandSpec("curve").Subcommands)
	b.WriteString("  )\n\n")
	b.WriteString("  _notifier_completion_commands=(\n")
	writeDescribeArray(&b, commandSpec("completion").Subcommands)
	b.WriteString("  )\n\n")
	b.WriteString("  if [[ ${words[2]-} == run ]]; then\n")
	b.WriteString("    case ${words[3]-} in\n")
	b.WriteString("      ./cmd/notifier|cmd/notifier)\n")
	b.WriteString("        if (( CURRENT == 4 )); then\n")
	b.WriteString("          _describe -t commands 'notifier command' _notifier_root_commands\n")
	b.WriteString("          return\n")
	b.WriteString("        fi\n")
	b.WriteString("        case ${words[4]-} in\n")
	b.WriteString("          serve)\n")
	b.WriteString("            if [[ ${words[CURRENT-1]-} == -config ]]; then\n")
	b.WriteString("              _files\n")
	b.WriteString("              return\n")
	b.WriteString("            fi\n")
	b.WriteString("            compadd -h --help -config\n")
	b.WriteString("            return\n")
	b.WriteString("            ;;\n")
	b.WriteString("          hash)\n")
	b.WriteString("            compadd -h --help -password -stdin\n")
	b.WriteString("            return\n")
	b.WriteString("            ;;\n")
	b.WriteString("          curve)\n")
	b.WriteString("            if (( CURRENT == 5 )); then\n")
	b.WriteString("              _describe -t curve-commands 'curve command' _notifier_curve_commands\n")
	b.WriteString("              return\n")
	b.WriteString("            fi\n")
	b.WriteString("            case ${words[5]-} in\n")
	b.WriteString("              gen)\n")
	b.WriteString("                compadd -h --help\n")
	b.WriteString("                return\n")
	b.WriteString("                ;;\n")
	b.WriteString("            esac\n")
	b.WriteString("            ;;\n")
	b.WriteString("          completion)\n")
	b.WriteString("            if (( CURRENT == 5 )); then\n")
	b.WriteString("              _describe -t completion-commands 'completion command' _notifier_completion_commands\n")
	b.WriteString("              return\n")
	b.WriteString("            fi\n")
	b.WriteString("            ;;\n")
	b.WriteString("        esac\n")
	b.WriteString("        return\n")
	b.WriteString("        ;;\n")
	b.WriteString("    esac\n")
	b.WriteString("  fi\n")
	b.WriteString("  if (( $+functions[_go] )); then\n")
	b.WriteString("    _go\n")
	b.WriteString("  fi\n")
	b.WriteString("}\n\n")
	b.WriteString("if (( $+functions[compdef] )); then\n")
	b.WriteString("  compdef _notifier notifier\n")
	b.WriteString("  compdef _notifier_go_dispatch go\n")
	b.WriteString("fi\n")
	return b.String()
}

func commandSpec(path ...string) completionCommand {
	cmd := notifierCompletionSpec
	for _, part := range path {
		found := false
		for _, sub := range cmd.Subcommands {
			if sub.Name == part {
				cmd = sub
				found = true
				break
			}
		}
		if !found {
			return completionCommand{}
		}
	}
	return cmd
}

func writeDescribeArray(b *strings.Builder, commands []completionCommand) {
	for _, cmd := range commands {
		fmt.Fprintf(b, "    %s\n", zshQuote(cmd.Name+":"+cmd.Description))
	}
}

func writeArgumentSpecs(b *strings.Builder, flags []completionFlag, indent string) {
	if len(flags) == 0 {
		return
	}
	for _, fl := range flags {
		fmt.Fprintf(b, "%s  %s \\\n", indent, zshQuote(zshArgumentSpec(fl)))
	}
}

func zshArgumentSpec(fl completionFlag) string {
	if fl.ValueName == "" {
		return fl.Name + "[" + fl.Description + "]"
	}
	if fl.IsPath {
		return fl.Name + "[" + fl.Description + "]:" + fl.ValueName + ":_files"
	}
	return fl.Name + "[" + fl.Description + "]:" + fl.ValueName + ":"
}

func zshQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
