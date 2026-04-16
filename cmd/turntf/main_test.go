package main

import (
	"bytes"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"

	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/store"
)

func TestRunWithoutArgsDefaultsToServe(t *testing.T) {
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
	err = run(nil, &stdout)
	if err == nil {
		t.Fatalf("expected missing default config to fail")
	}
	if !strings.Contains(err.Error(), "read config ./config.toml") {
		t.Fatalf("unexpected error: %v", err)
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

func TestServeRejectsLegacyConfigFlag(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"serve", "-config", "legacy.toml"}, &stdout)
	if err == nil {
		t.Fatalf("expected legacy config flag to be rejected")
	}
	if !strings.Contains(err.Error(), `unknown command "legacy.toml"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServeLongConfigFlag(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "missing.toml")
	var stdout bytes.Buffer
	err := run([]string{"serve", "--config", configPath}, &stdout)
	if err == nil {
		t.Fatalf("expected missing explicit config to fail")
	}
	if !strings.Contains(err.Error(), "read config "+configPath) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServeShortConfigFlag(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "missing.toml")
	var stdout bytes.Buffer
	err := run([]string{"serve", "-c", configPath}, &stdout)
	if err == nil {
		t.Fatalf("expected missing explicit config to fail")
	}
	if !strings.Contains(err.Error(), "read config "+configPath) {
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

	configPath := filepath.Join(t.TempDir(), "turntf.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store]
message_window_size = 250

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "ws://127.0.0.1:9081/internal/cluster/ws"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ConfigPath != configPath {
		t.Fatalf("unexpected config path: got=%q want=%q", cfg.ConfigPath, configPath)
	}
	if cfg.Services.HTTP.ListenAddr != ":8080" {
		t.Fatalf("unexpected http listen addr: %q", cfg.Services.HTTP.ListenAddr)
	}
	if cfg.SQLitePath != filepath.Clean("./data/node-a.db") {
		t.Fatalf("unexpected db path: %q", cfg.SQLitePath)
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

func TestLoadServeRuntimeConfigReadsZeroMQCurveConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "curve.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[services.zeromq]
enabled = true
bind_url = "tcp://127.0.0.1:9090"
security = "curve"

[services.zeromq.curve]
server_public_key = "S111111111111111111111111111111111111111"
server_secret_key = "s111111111111111111111111111111111111111"
client_public_key = "C111111111111111111111111111111111111111"
client_secret_key = "c111111111111111111111111111111111111111"
allowed_client_public_keys = ["A111111111111111111111111111111111111111"]

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "zmq+tcp://127.0.0.1:9091"
zeromq = { curve_server_public_key = "P111111111111111111111111111111111111111" }
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cluster.ZeroMQ.Security != cluster.ZeroMQSecurityCurve {
		t.Fatalf("unexpected zeromq security: %+v", cfg.Cluster.ZeroMQ)
	}
	if cfg.Cluster.ZeroMQ.Curve.ServerPublicKey != "S111111111111111111111111111111111111111" {
		t.Fatalf("unexpected curve config: %+v", cfg.Cluster.ZeroMQ.Curve)
	}
	if len(cfg.Cluster.Peers) != 1 || cfg.Cluster.Peers[0].ZeroMQCurveServerPublicKey != "P111111111111111111111111111111111111111" {
		t.Fatalf("unexpected peer curve key: %+v", cfg.Cluster.Peers)
	}
}

func TestLoadServeRuntimeConfigKeepsConfiguredPeerPath(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "peer-path.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "ws://127.0.0.1:9081/custom/cluster/path"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cluster.AdvertisePath != cluster.WebSocketPath {
		t.Fatalf("unexpected local cluster websocket path: %q", cfg.Cluster.AdvertisePath)
	}
	if len(cfg.Cluster.Peers) != 1 || cfg.Cluster.Peers[0].URL != "ws://127.0.0.1:9081/custom/cluster/path" {
		t.Fatalf("unexpected peer config: %+v", cfg.Cluster.Peers)
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
[services.http]
listen_addr = ":8080"

[store]
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
	if cfg.StoreOptions.Engine != store.EngineSQLite {
		t.Fatalf("unexpected default store engine: %q", cfg.StoreOptions.Engine)
	}
	if cfg.SQLitePath != filepath.Clean(defaultSQLitePath) {
		t.Fatalf("unexpected default sqlite path: %q", cfg.SQLitePath)
	}
	if cfg.PebblePath != filepath.Clean(defaultPebblePath) {
		t.Fatalf("unexpected default pebble path: %q", cfg.PebblePath)
	}
}

func TestLoadServeRuntimeConfigReadsSplitStoreConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "pebble.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store]
engine = "pebble"
message_window_size = 250

[store.sqlite]
db_path = "./data/state.db"

[store.pebble]
path = "./data/projections.pebble"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.SQLitePath != filepath.Clean("./data/state.db") {
		t.Fatalf("unexpected sqlite path: %q", cfg.SQLitePath)
	}
	if cfg.PebblePath != filepath.Clean("./data/projections.pebble") {
		t.Fatalf("unexpected pebble path: %q", cfg.PebblePath)
	}
	if cfg.StoreOptions.Engine != store.EnginePebble {
		t.Fatalf("unexpected store engine: %q", cfg.StoreOptions.Engine)
	}
	if cfg.StoreOptions.PebblePath != filepath.Clean("./data/projections.pebble") {
		t.Fatalf("unexpected store options pebble path: %q", cfg.StoreOptions.PebblePath)
	}
}

func TestLoadServeRuntimeConfigRejectsUnknownStoreEngine(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "bad-engine.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store]
engine = "badger"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "store.engine must be sqlite or pebble") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsMissingRequiredFields(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "broken.toml")
	writeTestConfig(t, configPath, `
[store]
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil {
		t.Fatalf("expected missing required field to fail")
	}
	if !strings.Contains(err.Error(), "services.http.listen_addr cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigRejectsMissingAuthTokenSecret(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "missing-auth.toml")
	if err := os.WriteFile(configPath, []byte(strings.TrimSpace(`
[services.http]
listen_addr = ":8080"

[store.sqlite]
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
[services.http]
listen_addr = ":8080"

[store.sqlite]
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
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[auth]
token_secret = "shared-secret"

[auth.bootstrap_admin]
username = "root"
password_hash = "hash-root"

[cluster]
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
	if err := runWithIO([]string{"hash", "--password", "secret"}, commandIO{
		Stdout: &stdout,
		Stdin:  strings.NewReader(""),
	}); err != nil {
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
	if err := runWithIO([]string{"hash", "--stdin"}, commandIO{
		Stdout: &stdout,
		Stdin:  strings.NewReader("secret\n"),
	}); err != nil {
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
	err := runWithIO([]string{"hash", "--stdin"}, commandIO{
		Stdout: &stdout,
		Stdin:  strings.NewReader("\n"),
	})
	if err == nil || !strings.Contains(err.Error(), "password cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHashCommandRejectsPasswordAndStdinTogether(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := runWithIO([]string{"hash", "--password", "secret", "--stdin"}, commandIO{
		Stdout: &stdout,
		Stdin:  strings.NewReader("secret\n"),
	})
	if err == nil || !strings.Contains(err.Error(), "--password and --stdin cannot be used together") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolvePasswordInputUsesHiddenPromptForTerminal(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp(t.TempDir(), "tty")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer file.Close()

	originalIsTerminal := isTerminalFile
	originalReadPassword := readPasswordLineFile
	t.Cleanup(func() {
		isTerminalFile = originalIsTerminal
		readPasswordLineFile = originalReadPassword
	})

	isTerminalFile = func(*os.File) bool { return true }
	var reads int
	readPasswordLineFile = func(*os.File) (string, error) {
		reads++
		return "secret\n", nil
	}

	var stdout bytes.Buffer
	password, err := resolvePasswordInput(&stdout, file, "", false)
	if err != nil {
		t.Fatalf("resolve password input: %v", err)
	}
	if password != "secret" {
		t.Fatalf("unexpected password: %q", password)
	}
	if reads != 2 {
		t.Fatalf("expected two hidden reads, got %d", reads)
	}
	if got := stdout.String(); got != "Password: \nConfirm password: \n" {
		t.Fatalf("unexpected prompt output: %q", got)
	}
}

func TestResolvePasswordInputRejectsMismatchedTerminalPasswords(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp(t.TempDir(), "tty")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer file.Close()

	originalIsTerminal := isTerminalFile
	originalReadPassword := readPasswordLineFile
	t.Cleanup(func() {
		isTerminalFile = originalIsTerminal
		readPasswordLineFile = originalReadPassword
	})

	isTerminalFile = func(*os.File) bool { return true }
	passwords := []string{"secret\n", "different\n"}
	readPasswordLineFile = func(*os.File) (string, error) {
		next := passwords[0]
		passwords = passwords[1:]
		return next, nil
	}

	var stdout bytes.Buffer
	_, err = resolvePasswordInput(&stdout, file, "", false)
	if err == nil || !strings.Contains(err.Error(), "passwords do not match") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigUsesDefaultAuthTokenTTL(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "default-auth-ttl.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
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

func TestLoadServeRuntimeConfigUsesDefaultLogging(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "default-logging.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Logging.Level != "info" {
		t.Fatalf("unexpected default log level: %q", cfg.Logging.Level)
	}
	if cfg.Logging.FilePath != "" {
		t.Fatalf("unexpected default log file path: %q", cfg.Logging.FilePath)
	}
}

func TestLoadServeRuntimeConfigReadsLogging(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "logging.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[logging]
level = "warn"
file_path = "./logs/turntf.log"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Logging.Level != "warn" {
		t.Fatalf("unexpected log level: %q", cfg.Logging.Level)
	}
	if cfg.Logging.FilePath != filepath.Clean("./logs/turntf.log") {
		t.Fatalf("unexpected log file path: %q", cfg.Logging.FilePath)
	}
}

func TestLoadServeRuntimeConfigRejectsInvalidLoggingLevel(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "bad-logging.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[logging]
level = "trace"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), `logging.level "trace" is invalid`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigAllowsDisablingMaxClockSkew(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "turntf.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[cluster.clock]
max_skew_ms = 0

[[cluster.peers]]
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

	configPath := filepath.Join(t.TempDir(), "turntf.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[cluster.clock]
max_skew_ms = -1

[[cluster.peers]]
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
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
listen_addr = ":9080"
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
[services.http]
listen_addr = ":8080"

[store.sqlite]
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

func TestLoadServeRuntimeConfigRejectsRemovedConfigFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "api listen addr moved to services",
			body: `
[api]
listen_addr = ":8080"
`,
			wantErr: "api.listen_addr",
		},
		{
			name: "store db path moved to store sqlite",
			body: `
[services.http]
listen_addr = ":8080"

[store]
db_path = "./data/node-a.db"
`,
			wantErr: "unknown fields store.db_path",
		},
		{
			name: "cluster advertise path removed",
			body: `
[services.http]
listen_addr = ":8080"

[cluster]
advertise_path = "/custom/cluster/ws"
secret = "secret"
`,
			wantErr: "unknown fields cluster.advertise_path",
		},
		{
			name: "cluster zeromq moved to services",
			body: `
[services.http]
listen_addr = ":8080"

[cluster.zeromq]
enabled = true
`,
			wantErr: "cluster.zeromq.enabled",
		},
		{
			name: "cluster max clock skew moved to clock section",
			body: `
[services.http]
listen_addr = ":8080"

[cluster]
max_clock_skew_ms = 1000
secret = "secret"
`,
			wantErr: "unknown fields cluster.max_clock_skew_ms",
		},
		{
			name: "cluster clock fields renamed",
			body: `
[services.http]
listen_addr = ":8080"

[cluster]
clock_sync_timeout_ms = 8000
secret = "secret"
`,
			wantErr: "unknown fields cluster.clock_sync_timeout_ms",
		},
		{
			name: "peer zeromq curve key moved to peer zeromq table",
			body: `
[services.http]
listen_addr = ":8080"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "zmq+tcp://127.0.0.1:9091"
zeromq_curve_server_public_key = "P111111111111111111111111111111111111111"
`,
			wantErr: "unknown fields cluster.peers.zeromq_curve_server_public_key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "removed.toml")
			writeTestConfig(t, configPath, tt.body)

			_, err := loadServeRuntimeConfig(configPath)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
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
			name: "peers without cluster settings",
			clusterBody: `
[cluster]

[[cluster.peers]]
url = "ws://127.0.0.1:9081/internal/cluster/ws"
`,
			wantErr: "cluster secret cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "broken.toml")
			writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
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

func TestLoadServeRuntimeConfigRejectsRemovedPeerNodeID(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "peer-node-id.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
node_id = 8192
url = "ws://127.0.0.1:9081/internal/cluster/ws"
`)

	_, err := loadServeRuntimeConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "unknown fields cluster.peers.node_id") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadServeRuntimeConfigSupportsZeroMQConfigAndPeerValidation(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "zeromq.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[services.zeromq]
enabled = true
bind_url = "tcp://0.0.0.0:9090"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "WS://Example.COM/internal/cluster/ws"

[[cluster.peers]]
url = "zmq+tcp://Example.COM:9091"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Cluster.ZeroMQ.Enabled {
		t.Fatalf("expected zeromq to be enabled")
	}
	if cfg.Cluster.ZeroMQ.BindURL != "tcp://0.0.0.0:9090" {
		t.Fatalf("unexpected zeromq bind url: %q", cfg.Cluster.ZeroMQ.BindURL)
	}
	if cfg.Cluster.Peers[0].URL != "ws://example.com/internal/cluster/ws" {
		t.Fatalf("unexpected normalized websocket peer url: %q", cfg.Cluster.Peers[0].URL)
	}
	if cfg.Cluster.Peers[1].URL != "zmq+tcp://example.com:9091" {
		t.Fatalf("unexpected normalized zeromq peer url: %q", cfg.Cluster.Peers[1].URL)
	}
}

func TestLoadServeRuntimeConfigZeroMQDefaultsDisabled(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "zeromq-default.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[services.zeromq]
bind_url = "tcp://0.0.0.0:9090"

[store.sqlite]
db_path = "./data/node-a.db"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cluster.Enabled() {
		t.Fatalf("expected cluster mode to stay disabled when zeromq is only preconfigured")
	}
	if cfg.Cluster.ZeroMQ.Enabled {
		t.Fatalf("expected zeromq to default to disabled")
	}
	if cfg.Cluster.ZeroMQ.BindURL != "tcp://0.0.0.0:9090" {
		t.Fatalf("unexpected preserved zeromq bind url: %q", cfg.Cluster.ZeroMQ.BindURL)
	}
	if cfg.Services.ZeroMQ.BindURL != "tcp://0.0.0.0:9090" {
		t.Fatalf("unexpected preserved service zeromq bind url: %q", cfg.Services.ZeroMQ.BindURL)
	}
}

func TestLoadServeRuntimeConfigSupportsStandaloneZeroMQService(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "zeromq-standalone.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[services.zeromq]
enabled = true
bind_url = "tcp://127.0.0.1:9090"

[store.sqlite]
db_path = "./data/node-a.db"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cluster.Enabled() {
		t.Fatalf("expected cluster mode to stay disabled for standalone zeromq service")
	}
	if !cfg.Services.ZeroMQ.Enabled || cfg.Services.ZeroMQ.BindURL != "tcp://127.0.0.1:9090" {
		t.Fatalf("unexpected service zeromq config: %+v", cfg.Services.ZeroMQ)
	}
	if !cfg.Cluster.ZeroMQ.Enabled || cfg.Cluster.ZeroMQ.BindURL != "tcp://127.0.0.1:9090" {
		t.Fatalf("unexpected cluster zeromq config: %+v", cfg.Cluster.ZeroMQ)
	}
}

func TestLoadServeRuntimeConfigRejectsInvalidZeroMQConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "invalid bind url scheme",
			body: `
[services.http]
listen_addr = ":8080"

[services.zeromq]
enabled = true
bind_url = "ws://127.0.0.1:9090"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"
`,
			wantErr: "zeromq bind url scheme must be tcp",
		},
		{
			name: "zeromq peer requires zeromq enabled",
			body: `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "zmq+tcp://127.0.0.1:9091"
`,
			wantErr: `zeromq peer url "zmq+tcp://127.0.0.1:9091" requires services.zeromq.enabled`,
		},
		{
			name: "invalid zeromq peer wildcard",
			body: `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "zmq+tcp://0.0.0.0:9091"
`,
			wantErr: "peer url host cannot be a wildcard address",
		},
		{
			name: "duplicate normalized peer urls",
			body: `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "WS://Example.COM/internal/cluster/ws"

[[cluster.peers]]
url = "ws://example.com/internal/cluster/ws"
`,
			wantErr: `duplicate peer url "ws://example.com/internal/cluster/ws"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "zeromq-invalid.toml")
			writeTestConfig(t, configPath, tt.body)

			_, err := loadServeRuntimeConfig(configPath)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoadServeRuntimeConfigSupportsOutboundOnlyZeroMQ(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "zeromq-outbound.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[services.zeromq]
enabled = true

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "zmq+tcp://Example.COM:9091"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Cluster.ZeroMQ.Enabled {
		t.Fatalf("expected zeromq to be enabled")
	}
	if cfg.Cluster.ZeroMQ.BindURL != "" {
		t.Fatalf("expected empty zeromq bind url in outbound-only mode, got %q", cfg.Cluster.ZeroMQ.BindURL)
	}
	if cfg.Cluster.Peers[0].URL != "zmq+tcp://example.com:9091" {
		t.Fatalf("unexpected normalized zeromq peer url: %q", cfg.Cluster.Peers[0].URL)
	}
}

func TestLoadServeRuntimeConfigSupportsLibP2PConfig(t *testing.T) {
	t.Parallel()

	peerAddr := testConfigLibP2PPeerAddr(t, "/ip4/127.0.0.1/tcp/4001")
	configPath := filepath.Join(t.TempDir(), "libp2p.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[services.libp2p]
enabled = true
private_key_path = "./data/node-a-libp2p.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/4001"]
bootstrap_peers = ["`+peerAddr+`"]
enable_dht = false
enable_mdns = true
relay_peers = ["`+peerAddr+`"]
enable_hole_punching = false
gossipsub_enabled = false

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "`+peerAddr+`"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	libp2p := cfg.Cluster.LibP2P
	if !libp2p.Enabled {
		t.Fatalf("expected libp2p to be enabled")
	}
	if libp2p.PrivateKeyPath != "./data/node-a-libp2p.key" {
		t.Fatalf("unexpected private key path: %q", libp2p.PrivateKeyPath)
	}
	if len(libp2p.ListenAddrs) != 1 || libp2p.ListenAddrs[0] != "/ip4/0.0.0.0/tcp/4001" {
		t.Fatalf("unexpected listen addrs: %+v", libp2p.ListenAddrs)
	}
	if len(libp2p.BootstrapPeers) != 1 || libp2p.BootstrapPeers[0] != peerAddr {
		t.Fatalf("unexpected bootstrap peers: %+v", libp2p.BootstrapPeers)
	}
	if libp2p.EnableDHT || !libp2p.EnableMDNS || libp2p.EnableHolePunching || libp2p.GossipSubEnabled {
		t.Fatalf("unexpected libp2p feature flags: %+v", libp2p)
	}
	if len(cfg.Cluster.Peers) != 1 || cfg.Cluster.Peers[0].URL != peerAddr {
		t.Fatalf("unexpected libp2p peer: %+v", cfg.Cluster.Peers)
	}
	if cfg.Services.LibP2P.PrivateKeyPath != "./data/node-a-libp2p.key" {
		t.Fatalf("unexpected service libp2p config: %+v", cfg.Services.LibP2P)
	}
}

func TestLoadServeRuntimeConfigLibP2PDefaultsDisabled(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "libp2p-default.toml")
	writeTestConfig(t, configPath, `
[services.http]
listen_addr = ":8080"

[services.libp2p]
private_key_path = "./data/libp2p.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/4001"]

[store.sqlite]
db_path = "./data/node-a.db"
`)

	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cluster.Enabled() {
		t.Fatalf("expected cluster mode to stay disabled when libp2p is only preconfigured")
	}
	if cfg.Cluster.LibP2P.Enabled {
		t.Fatalf("expected libp2p to default to disabled")
	}
	if cfg.Services.LibP2P.PrivateKeyPath != "./data/libp2p.key" {
		t.Fatalf("unexpected service libp2p config: %+v", cfg.Services.LibP2P)
	}
}

func TestLoadServeRuntimeConfigRejectsInvalidLibP2PConfig(t *testing.T) {
	t.Parallel()

	peerAddr := testConfigLibP2PPeerAddr(t, "/ip4/127.0.0.1/tcp/4001")
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "enabled requires cluster secret",
			body: `
[services.http]
listen_addr = ":8080"

[services.libp2p]
enabled = true

[store.sqlite]
db_path = "./data/node-a.db"
`,
			wantErr: "cluster secret cannot be empty",
		},
		{
			name: "peer requires enabled",
			body: `
[services.http]
listen_addr = ":8080"

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "` + peerAddr + `"
`,
			wantErr: `libp2p peer url "` + peerAddr + `" requires services.libp2p.enabled`,
		},
		{
			name: "static peer missing p2p",
			body: `
[services.http]
listen_addr = ":8080"

[services.libp2p]
enabled = true

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"

[[cluster.peers]]
url = "/ip4/127.0.0.1/tcp/4001"
`,
			wantErr: "libp2p peer addr must include /p2p peer id",
		},
		{
			name: "listen addr includes p2p",
			body: `
[services.http]
listen_addr = ":8080"

[services.libp2p]
enabled = true
listen_addrs = ["` + peerAddr + `"]

[store.sqlite]
db_path = "./data/node-a.db"

[cluster]
secret = "secret"
`,
			wantErr: "libp2p listen addr must not include /p2p",
		},
		{
			name: "unknown libp2p field",
			body: `
[services.http]
listen_addr = ":8080"

[services.libp2p]
enabled = true
advertise_addrs = ["/ip4/127.0.0.1/tcp/4001"]

[store.sqlite]
db_path = "./data/node-a.db"
`,
			wantErr: "unknown fields services.libp2p.advertise_addrs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "libp2p-invalid.toml")
			writeTestConfig(t, configPath, tt.body)

			_, err := loadServeRuntimeConfig(configPath)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestServeHandlerLeavesClusterRouteHiddenWhenDisabled(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/internal/cluster/ws", nil)
	rr := httptest.NewRecorder()

	serveHandler(http.NewServeMux(), nil).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got=%d want=%d", rr.Code, http.StatusNotFound)
	}
}

func TestServeHandlerMountsClusterRouteOnAPIListener(t *testing.T) {
	t.Parallel()

	manager, err := cluster.NewManager(cluster.Config{
		NodeID:            int64(4096),
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

	serveHandler(http.NewServeMux(), manager).ServeHTTP(rr, req)

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

func testConfigLibP2PPeerAddr(t *testing.T, transportAddr string) string {
	t.Helper()
	_, pub, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate libp2p key: %v", err)
	}
	id, err := libp2ppeer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("derive libp2p peer id: %v", err)
	}
	return transportAddr + "/p2p/" + id.String()
}
