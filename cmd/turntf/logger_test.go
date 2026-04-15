package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog/log"
)

func TestConfigureLoggerWritesConsoleAndJSONFile(t *testing.T) {
	originalLogger := log.Logger
	t.Cleanup(func() {
		log.Logger = originalLogger
	})

	var console bytes.Buffer
	logPath := filepath.Join(t.TempDir(), "logs", "turntf.log")
	closeLogger, err := configureLogger(runtimeLoggingConfig{
		Level:    "info",
		FilePath: logPath,
	}, &console)
	if err != nil {
		t.Fatalf("configure logger: %v", err)
	}

	log.Info().
		Str("component", "turntf").
		Str("event", "test_event").
		Msg("test message")
	if err := closeLogger(); err != nil {
		t.Fatalf("close logger: %v", err)
	}

	consoleOutput := console.String()
	if !strings.Contains(consoleOutput, "test message") {
		t.Fatalf("expected console message, got %q", consoleOutput)
	}
	if !strings.Contains(consoleOutput, "component=turntf") {
		t.Fatalf("expected console component field, got %q", consoleOutput)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one json log line, got %d: %q", len(lines), string(data))
	}

	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("unmarshal json log: %v line=%q", err, lines[0])
	}
	if event["level"] != "info" {
		t.Fatalf("unexpected log level: %#v", event["level"])
	}
	if timestamp, ok := event["time"].(string); !ok || timestamp == "" {
		t.Fatalf("expected time field in json log: %#v", event)
	}
	if event["component"] != "turntf" || event["event"] != "test_event" {
		t.Fatalf("unexpected structured fields: %#v", event)
	}
}

func TestConfigureLoggerRejectsInvalidLevel(t *testing.T) {
	if _, err := configureLogger(runtimeLoggingConfig{Level: "trace"}, io.Discard); err == nil {
		t.Fatalf("expected invalid level to fail")
	}
}
