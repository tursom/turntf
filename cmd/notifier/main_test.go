package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunWithoutArgsPrintsHelp(t *testing.T) {
	var stdout bytes.Buffer

	if err := run(nil, &stdout); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	output := stdout.String()
	if !strings.Contains(output, "usage:") {
		t.Fatalf("expected usage in output, got %q", output)
	}
	if !strings.Contains(output, "notifier serve") {
		t.Fatalf("expected serve command in output, got %q", output)
	}
}

func TestRunHelpCommandPrintsHelp(t *testing.T) {
	var stdout bytes.Buffer

	if err := run([]string{"help"}, &stdout); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	output := stdout.String()
	if !strings.Contains(output, "notifier apikey") {
		t.Fatalf("expected apikey command in output, got %q", output)
	}
}
