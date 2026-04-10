package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
addr = ":9090"
db_path = "./data/test.db"
user_session_ttl_hours = 12
`

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Addr != ":9090" {
		t.Fatalf("unexpected addr: %s", cfg.Addr)
	}
	if cfg.DBPath != "./data/test.db" {
		t.Fatalf("unexpected db path: %s", cfg.DBPath)
	}
	if cfg.UserSessionTTLHours != 12 {
		t.Fatalf("unexpected ttl hours: %d", cfg.UserSessionTTLHours)
	}
}

func TestLoadWithComments(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
# service config
addr = ":8081" # listen addr
db_path = './data/notifier.db'
user_session_ttl_hours = 48
`

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Addr != ":8081" || cfg.DBPath != "./data/notifier.db" || cfg.UserSessionTTLHours != 48 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
