package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"notifier/internal/auth"
	"notifier/internal/cluster"
	"notifier/internal/store"
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
	if !strings.Contains(output, "notifier serve [-config ./config.toml]") {
		t.Fatalf("expected serve config usage in output, got %q", output)
	}
	if strings.Contains(output, "init-store") {
		t.Fatalf("did not expect init-store in output, got %q", output)
	}
}

func TestInitStoreCommandRemoved(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"init-store"}, &stdout)
	if err == nil {
		t.Fatalf("expected init-store to be removed")
	}
	if !strings.Contains(err.Error(), `unknown command "init-store"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServeRejectsLegacyFlags(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"serve", "-db", "legacy.db"}, &stdout)
	if err == nil {
		t.Fatalf("expected legacy flag to be rejected")
	}
	if !strings.Contains(err.Error(), "flag provided but not defined: -db") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServeRejectsMissingDefaultConfig(t *testing.T) {
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir tempdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(originalWD)
	})

	var stdout bytes.Buffer
	err = run([]string{"serve"}, &stdout)
	if err == nil {
		t.Fatalf("expected missing default config to fail")
	}
	if !strings.Contains(err.Error(), "read config ./config.toml") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigFromExplicitPath(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "notifier.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"
message_window_size = 250

[cluster]
advertise_path = "/internal/cluster/ws"
secret = "secret"

[[cluster.peers]]
node_id = "node-b"
url = "ws://127.0.0.1:9081/internal/cluster/ws"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ConfigPath != configPath {
		t.Fatalf("unexpected config path: got=%q want=%q", cfg.ConfigPath, configPath)
	}
	if cfg.APIAddr != ":8080" {
		t.Fatalf("unexpected api addr: %q", cfg.APIAddr)
	}
	if cfg.DBPath != filepath.Clean("./data/node-a.db") {
		t.Fatalf("unexpected db path: %q", cfg.DBPath)
	}
	if cfg.StoreOptions.NodeID != "node-a" || cfg.StoreOptions.NodeSlot != 1 {
		t.Fatalf("unexpected store options: %+v", cfg.StoreOptions)
	}
	if cfg.StoreOptions.MessageWindowSize != 250 {
		t.Fatalf("unexpected message window size: %d", cfg.StoreOptions.MessageWindowSize)
	}
	if cfg.Cluster.AdvertisePath != "/internal/cluster/ws" || len(cfg.Cluster.Peers) != 1 {
		t.Fatalf("unexpected cluster config: %+v", cfg.Cluster)
	}
	if cfg.Cluster.MaxClockSkewMs != cluster.DefaultMaxClockSkewMs {
		t.Fatalf("unexpected default max clock skew: got=%d want=%d", cfg.Cluster.MaxClockSkewMs, cluster.DefaultMaxClockSkewMs)
	}
}

func TestLoadServeRuntimeConfigUsesDefaultPathAndDefaultMessageWindow(t *testing.T) {
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir tempdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(originalWD)
	})

	writeTestConfig(t, filepath.Join(tempDir, "config.toml"), `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"
`)

	cfg, err := loadServeRuntimeConfig("")
	if err != nil {
		t.Fatalf("load default-path config: %v", err)
	}

	if cfg.ConfigPath != defaultConfigPath {
		t.Fatalf("unexpected default config path: %q", cfg.ConfigPath)
	}
	if cfg.StoreOptions.MessageWindowSize != store.DefaultMessageWindowSize {
		t.Fatalf("unexpected default message window size: got=%d want=%d", cfg.StoreOptions.MessageWindowSize, store.DefaultMessageWindowSize)
	}
}

func TestLoadServeRuntimeConfigRejectsMissingRequiredFields(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "broken.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[store]
db_path = "./data/node-a.db"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil {
		t.Fatalf("expected missing required field to fail")
	}
	if !strings.Contains(err.Error(), "api.listen_addr cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsMissingAuthTokenSecret(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "missing-auth.toml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"

[auth.bootstrap_admin]
username = "root"
password_hash = "hash-root"
`)+"\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "auth.token_secret cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsMissingBootstrapAdminConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "missing-bootstrap.toml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"

[auth]
token_secret = "token-secret"
`)+"\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "auth.bootstrap_admin.username cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsMatchingAuthAndClusterSecrets(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "matching-secrets.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"

[auth]
token_secret = "shared-secret"

[auth.bootstrap_admin]
username = "root"
password_hash = "hash-root"

[cluster]
advertise_path = "/internal/cluster/ws"
secret = "shared-secret"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "auth.token_secret must differ from cluster.secret") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHashCommandPrintsBCryptHashForPasswordFlag(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := runHash([]string{"-password", "secret"}, &stdout, strings.NewReader("")); err != nil {
		t.Fatalf("run hash: %v", err)
	}

	hash := strings.TrimSpace(stdout.String())
	if hash == "" {
		t.Fatalf("expected hash output")
	}
	if err := auth.VerifyPassword(hash, "secret"); err != nil {
		t.Fatalf("verify printed hash: %v", err)
	}
}

func TestHashCommandPrintsBCryptHashForStdin(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := runHash([]string{"-stdin"}, &stdout, strings.NewReader("secret\n")); err != nil {
		t.Fatalf("run hash stdin: %v", err)
	}

	hash := strings.TrimSpace(stdout.String())
	if hash == "" {
		t.Fatalf("expected hash output")
	}
	if err := auth.VerifyPassword(hash, "secret"); err != nil {
		t.Fatalf("verify printed stdin hash: %v", err)
	}
}

func TestHashCommandRejectsEmptyPassword(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := runHash([]string{"-stdin"}, &stdout, strings.NewReader("\n"))
	if err == nil || !strings.Contains(err.Error(), "password cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigUsesDefaultAuthTokenTTL(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "default-auth-ttl.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Auth.TokenTTLMinutes != 1440 {
		t.Fatalf("unexpected default auth ttl: %d", cfg.Auth.TokenTTLMinutes)
	}
}

func TestLoadServeRuntimeConfigAllowsDisablingMaxClockSkew(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "notifier.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"

[cluster]
advertise_path = "/internal/cluster/ws"
secret = "secret"
max_clock_skew_ms = 0

[[cluster.peers]]
node_id = "node-b"
url = "ws://127.0.0.1:9081/internal/cluster/ws"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cluster.MaxClockSkewMs != 0 {
		t.Fatalf("expected disabled max clock skew, got %d", cfg.Cluster.MaxClockSkewMs)
	}
}

func TestLoadServeRuntimeConfigRejectsNegativeMaxClockSkew(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "notifier.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"

[cluster]
advertise_path = "/internal/cluster/ws"
secret = "secret"
max_clock_skew_ms = -1

[[cluster.peers]]
node_id = "node-b"
url = "ws://127.0.0.1:9081/internal/cluster/ws"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil {
		t.Fatalf("expected negative max clock skew to fail")
	}
	if !strings.Contains(err.Error(), "cluster max clock skew must be non-negative") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsRemovedClusterListenAddr(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "legacy.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"

[cluster]
listen_addr = ":9080"
advertise_path = "/internal/cluster/ws"
secret = "secret"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil {
		t.Fatalf("expected legacy cluster.listen_addr to fail")
	}
	if !strings.Contains(err.Error(), "unknown fields cluster.listen_addr") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsRemovedClusterAdvertiseAddr(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "legacy-advertise.toml")
	writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"

[cluster]
advertise_addr = "ws://127.0.0.1:9080/internal/cluster/ws"
secret = "secret"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil {
		t.Fatalf("expected legacy cluster.advertise_addr to fail")
	}
	if !strings.Contains(err.Error(), "unknown fields cluster.advertise_addr") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsIncompleteClusterConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterBody string
		wantErr     string
	}{
		{
			name: "missing advertise path",
			clusterBody: `
[cluster]
secret = "secret"
`,
			wantErr: "cluster advertise path cannot be empty when cluster mode is enabled",
		},
		{
			name: "missing secret",
			clusterBody: `
[cluster]
advertise_path = "/internal/cluster/ws"
`,
			wantErr: "cluster secret cannot be empty",
		},
		{
			name: "invalid advertise path",
			clusterBody: `
[cluster]
advertise_path = "internal/cluster/ws"
secret = "secret"
`,
			wantErr: "cluster advertise path must start with /",
		},
		{
			name: "peers without cluster settings",
			clusterBody: `
[cluster]

[[cluster.peers]]
node_id = "node-b"
url = "ws://127.0.0.1:9081/internal/cluster/ws"
`,
			wantErr: "cluster secret cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "broken.toml")
			writeTestConfig(t, configPath, `
[node]
id = "node-a"
slot = 1

[api]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"
`+tt.clusterBody)

			_, err := loadServeRuntimeConfig(configPath)
			if err == nil {
				t.Fatalf("expected invalid cluster config")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestServeHandlerLeavesClusterRouteHiddenWhenDisabled(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/internal/cluster/ws", nil)
	rr := httptest.NewRecorder()

	serveHandler(http.NewServeMux(), nil, "").ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got=%d want=%d", rr.Code, http.StatusNotFound)
	}
}

func TestServeHandlerMountsClusterRouteOnAPIListener(t *testing.T) {
	t.Parallel()

	manager, err := cluster.NewManager(cluster.Config{
		NodeID:            "node-a",
		NodeSlot:          1,
		AdvertisePath:     "/internal/cluster/ws",
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    cluster.DefaultMaxClockSkewMs,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/cluster/ws", nil)
	rr := httptest.NewRecorder()

	serveHandler(http.NewServeMux(), manager, "/internal/cluster/ws").ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got=%d want=%d", rr.Code, http.StatusServiceUnavailable)
	}
}

func writeTestConfig(t *testing.T, path, body string) {
	t.Helper()
	config := strings.TrimSpace(body)
	if !strings.Contains(config, "[auth]") {
		config += `

[auth]
token_secret = "token-secret"

[auth.bootstrap_admin]
username = "root"
password_hash = "hash-root"
`
	}
	if err := os.WriteFile(path, []byte(config+"\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
