package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestUsageTextMentionsCompletionZsh(t *testing.T) {
	t.Parallel()

	if !strings.Contains(usageText(), "notifier completion zsh") {
		t.Fatalf("expected usage text to mention completion zsh, got %q", usageText())
	}
}

func TestCompletionWithoutArgsPrintsUsage(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"completion"}, &stdout); err != nil {
		t.Fatalf("run completion: %v", err)
	}
	if !strings.Contains(stdout.String(), "notifier completion zsh") {
		t.Fatalf("expected completion usage to mention zsh, got %q", stdout.String())
	}
}

func TestCompletionHelpPrintsUsage(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"completion", "help"}, &stdout); err != nil {
		t.Fatalf("run completion help: %v", err)
	}
	if !strings.Contains(stdout.String(), "notifier completion zsh") {
		t.Fatalf("expected completion help to mention zsh, got %q", stdout.String())
	}
}

func TestCompletionZshPrintsScript(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"completion", "zsh"}, &stdout); err != nil {
		t.Fatalf("run completion zsh: %v", err)
	}

	output := stdout.String()
	for _, snippet := range []string{
		"#compdef notifier",
		"_notifier() {",
		"_notifier_go_dispatch() {",
		"compdef _notifier notifier",
		"compdef _notifier_go_dispatch go",
		"case $words[2] in",
		"if [[ ${words[2]-} == run ]]; then",
		"./cmd/notifier|cmd/notifier",
		"serve:start notifier service",
		"hash:generate password hash",
		"curve:manage ZeroMQ CURVE helpers",
		"completion:generate shell completion scripts",
		"-config[path to TOML config file]:config file:_files",
		"_notifier_curve_commands",
		"_notifier_completion_commands",
	} {
		if !strings.Contains(output, snippet) {
			t.Fatalf("expected output to contain %q, got %q", snippet, output)
		}
	}
}

func TestCompletionRejectsUnknownShell(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"completion", "bash"}, &stdout)
	if err == nil || !strings.Contains(err.Error(), `unknown completion shell "bash"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}
