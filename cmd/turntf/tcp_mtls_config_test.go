package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadServeTCPMTLSConfig(t *testing.T) {
	const settings = `
[services.tcp_mtls]
enabled = true
listen_addr = "127.0.0.1:9443"
advertised_endpoints = ["tcp+tls://kiwi.example:9443/4096"]
ca_file = "ca.pem"
cert_file = "node.pem"
key_file = "node.key"
allowed_node_ids = [8192, 12288]
handshake_timeout_ms = 1200
max_frame_bytes = 1048576
[cluster]
secret = "cluster-test-secret"
[[cluster.peers]]
url = "tcp+tls://cc.example:9443/8192"
[[cluster.peers]]
url = "wss://cc.example/internal/cluster/ws"
`
	for _, tc := range []struct{ name, body, want string }{
		{"enabled", settings, ""},
		{"disabled with TCP peer", strings.Replace(settings, "enabled = true", "enabled = false", 1), "requires services.tcp_mtls.enabled"},
		{"missing certificate", strings.Replace(settings, `cert_file = "node.pem"`, `cert_file = ""`, 1), "requires ca_file"},
		{"missing allow list", strings.Replace(settings, `allowed_node_ids = [8192, 12288]`, `allowed_node_ids = []`, 1), "requires allowed_node_ids"},
		{"unallowed target", strings.Replace(settings, `allowed_node_ids = [8192, 12288]`, `allowed_node_ids = [12288]`, 1), "must be in allowed_node_ids"},
		{"negative timeout", strings.Replace(settings, "handshake_timeout_ms = 1200", "handshake_timeout_ms = -1", 1), "requires timeout"},
		{"oversized frame limit", strings.Replace(settings, "max_frame_bytes = 1048576", "max_frame_bytes = 67108865", 1), "frame limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			writeTestConfig(t, path, "[services.http]\nlisten_addr = \":8080\"\n"+tc.body)
			cfg, err := loadServeRuntimeConfig(path)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("want %q got %v", tc.want, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tcp := cfg.Cluster.TCPMTLS
			if !tcp.Enabled || tcp.ListenAddr != "127.0.0.1:9443" || tcp.HandshakeTimeoutMs != 1200 || tcp.MaxFrameBytes != 1048576 || len(tcp.AllowedNodeIDs) != 2 || len(tcp.AdvertisedEndpoints) != 1 || len(cfg.Cluster.Peers) != 2 {
				t.Fatalf("config mapping: %+v", cfg.Cluster)
			}
		})
	}
	t.Run("default disabled does not load certificates", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.toml")
		writeTestConfig(t, path, "[services.http]\nlisten_addr = \":8080\"\n[services.tcp_mtls]\ncert_file = \"/does/not/exist\"\n")
		cfg, err := loadServeRuntimeConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Cluster.TCPMTLS.Enabled || cfg.Cluster.Enabled() {
			t.Fatal("TCP enabled by certificate setting")
		}
		if cfg.Cluster.TCPMTLS.MaxFrameBytes != 8<<20 || cfg.Cluster.TCPMTLS.HandshakeTimeoutMs != 5000 {
			t.Fatal("incorrect TCP defaults")
		}
	})
}
