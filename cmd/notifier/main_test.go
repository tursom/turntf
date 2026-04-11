package main

import (
	"bytes"
	"os"
	"path/filepath"
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
	if !strings.Contains(output, "notifier init-store") {
		t.Fatalf("expected init-store command in output, got %q", output)
	}
}

func TestInitStoreCommandCreatesDatabase(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "phase1.db")
	var stdout bytes.Buffer

	err := run([]string{"init-store", "-db", dbPath, "-node-id", "node-test", "-node-slot", "7"}, &stdout)
	if err != nil {
		t.Fatalf("run init-store: %v", err)
	}

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected db file to exist: %v", err)
	}
	if !strings.Contains(stdout.String(), "initialized store") {
		t.Fatalf("unexpected output: %q", stdout.String())
	}
}

func TestServeRejectsIncompleteClusterConfig(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cluster.db")
	var stdout bytes.Buffer

	err := run([]string{
		"serve",
		"-db", dbPath,
		"-node-id", "node-a",
		"-node-slot", "1",
		"-cluster-secret", "secret",
		"-peer", "node-b=ws://127.0.0.1:9081/internal/cluster/ws",
	}, &stdout)
	if err == nil {
		t.Fatalf("expected incomplete cluster config to fail")
	}
	if !strings.Contains(err.Error(), "cluster listen addr cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}
