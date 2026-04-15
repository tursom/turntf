package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRootHelpMentionsCompletion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"--help"}, &stdout); err != nil {
		t.Fatalf("run root help: %v", err)
	}
	if !strings.Contains(stdout.String(), "completion") {
		t.Fatalf("expected root help to mention completion, got %q", stdout.String())
	}
}

func TestCompletionWithoutArgsRejectsMissingShell(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"completion"}, &stdout)
	if err == nil || !strings.Contains(err.Error(), "accepts 1 arg") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompletionHelpPrintsUsage(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"completion", "--help"}, &stdout); err != nil {
		t.Fatalf("run completion help: %v", err)
	}
	for _, snippet := range []string{"bash", "zsh", "fish", "powershell"} {
		if !strings.Contains(stdout.String(), snippet) {
			t.Fatalf("expected completion help to mention %q, got %q", snippet, stdout.String())
		}
	}
}

func TestCompletionPrintsScripts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		shell   string
		snippet string
	}{
		{shell: "bash", snippet: "bash completion for turntf"},
		{shell: "zsh", snippet: "#compdef turntf"},
		{shell: "fish", snippet: "fish completion for turntf"},
		{shell: "powershell", snippet: "powershell completion for turntf"},
	}
	for _, tt := range tests {
		t.Run(tt.shell, func(t *testing.T) {
			var stdout bytes.Buffer
			if err := run([]string{"completion", tt.shell}, &stdout); err != nil {
				t.Fatalf("run completion %s: %v", tt.shell, err)
			}
			output := stdout.String()
			if !strings.Contains(output, tt.snippet) {
				t.Fatalf("expected output to contain %q, got %q", tt.snippet, output)
			}
		})
	}
}

func TestCompletionZshUsesCobraScript(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"completion", "zsh"}, &stdout); err != nil {
		t.Fatalf("run completion zsh: %v", err)
	}
	output := stdout.String()
	for _, snippet := range []string{
		"#compdef turntf ./turntf",
		"_turntf()",
		"__complete",
		"compdef _turntf ./turntf",
		"compdef -p _turntf '*/turntf'",
		"_turntf_go_dispatch()",
		"./cmd/turntf|cmd/turntf",
		"--config",
		"-c",
		"--password",
		"--stdin",
		"bash:Generate bash completion",
	} {
		if !strings.Contains(output, snippet) {
			t.Fatalf("expected output to contain %q, got %q", snippet, output)
		}
	}
}

func TestCompletionRejectsUnknownShell(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"completion", "elvish"}, &stdout)
	if err == nil || !strings.Contains(err.Error(), `unsupported completion shell "elvish"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}
