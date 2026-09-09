//go:build integration

package integration

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type status struct {
	NodeID int64  `json:"node_id"`
	Ready  bool   `json:"write_gate_ready"`
	State  string `json:"clock_state"`
	Reason string `json:"clock_reason"`
	Last   string `json:"last_trusted_clock_sync"`
	Peers  []struct {
		NodeID  int64  `json:"node_id"`
		State   string `json:"clock_state"`
		Last    string `json:"last_credible_clock_sync"`
		Trusted bool   `json:"trusted_for_offset"`
	} `json:"peers"`
	Mesh struct {
		Enabled bool `json:"enabled"`
		Routes  []struct {
			Reachable bool   `json:"reachable"`
			Transport string `json:"outbound_transport"`
		} `json:"routes"`
	} `json:"mesh"`
}

type node struct {
	base, token string
	cmd         *exec.Cmd
	running     bool
}

var client = &http.Client{Timeout: 3 * time.Second}

func secret(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func request(n *node, method, path string, body any, out any) int {
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	req, err := http.NewRequest(method, n.base+path, bytes.NewReader(data))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	if n.token != "" {
		req.Header.Set("Authorization", "Bearer "+n.token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if out != nil {
		if json.NewDecoder(resp.Body).Decode(out) != nil {
			return 0
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode
}

func eventually(t *testing.T, timeout time.Duration, label string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("timeout: " + label)
}

// TestServeWSClockAndReplication uses unmodified serve startup, authentication,
// real loopback WebSockets, default clock protection and isolated SQLite stores.
// No clock injection, direct store write or gate override is used.
func TestServeWSClockAndReplication(t *testing.T) {
	binary := os.Getenv("TURNTF_PREFLIGHT_BINARY")
	if binary == "" {
		t.Fatal("set TURNTF_PREFLIGHT_BINARY to an absolute serve binary path")
	}
	dir := os.Getenv("TURNTF_PREFLIGHT_ARTIFACTS")
	if dir == "" {
		dir = t.TempDir()
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Logf("artifacts=%s", dir)
	auth, cluster := secret(t), secret(t)
	listeners := make([]net.Listener, 2)
	addresses := make([]string, 2)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		addresses[i] = l.Addr().String()
		defer l.Close()
	}
	nodes := make([]*node, 2)
	stop := func(n *node) {
		if !n.running {
			return
		}
		_ = n.cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- n.cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = n.cmd.Process.Kill()
			<-done
		}
		n.running = false
		t.Logf("stopped pid=%d", n.cmd.Process.Pid)
	}
	passwords := []string{secret(t), secret(t)}
	for i := range nodes {
		hashCmd := exec.Command(binary, "hash", "--stdin")
		hashCmd.Stdin = strings.NewReader(passwords[i] + "\n")
		hash, err := hashCmd.Output()
		if err != nil {
			t.Fatal("hash command failed")
		}
		if !strings.HasPrefix(strings.TrimSpace(string(hash)), "$2") {
			t.Fatal("unexpected hash format")
		}
		config := fmt.Sprintf(`[services.http]
listen_addr = %q
[services.zeromq]
enabled = false
[services.libp2p]
enabled = false
[store.sqlite]
db_path = %q
[auth]
token_secret = %q
[auth.bootstrap_admin]
username = "root"
login_name = "preflight-%d"
password_hash = %q
[logging]
level = "info"
file_path = ""
[cluster]
secret = %q
[cluster.forwarding]
enabled = true
bridge_enabled = false
node_fee_weight = 1
[[cluster.peers]]
url = %q
`, addresses[i], filepath.Join(dir, fmt.Sprintf("node%d.db", i)), auth, i, strings.TrimSpace(string(hash)), cluster, "ws://"+addresses[1-i]+"/internal/cluster/ws")
		configPath := filepath.Join(dir, fmt.Sprintf("node%d.toml", i))
		if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		log, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("node%d.log", i)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			t.Fatal(err)
		}
		n := &node{base: "http://" + addresses[i]}
		nodes[i] = n
		n.cmd = exec.Command(binary, "serve", "--config", configPath)
		n.cmd.Stdout = log
		n.cmd.Stderr = log
		_ = listeners[i].Close()
		if err := n.cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		n.running = true
		t.Logf("node%d pid=%d address=%s", i, n.cmd.Process.Pid, addresses[i])
		t.Cleanup(func() {
			stop(n)
			log.Close()
		})
		if i == 0 {
			// The configured peer has not started yet: a healthy HTTP listener
			// must not admit writes before its first trusted clock observation.
			eventually(t, 15*time.Second, "first node health", func() bool {
				return request(n, "GET", "/healthz", nil, nil) == 200
			})
			var login struct {
				Token string `json:"token"`
			}
			if code := request(n, "POST", "/auth/login", map[string]string{"login_name": "preflight-0", "password": passwords[0]}, &login); code != 200 || login.Token == "" {
				t.Fatalf("first node login HTTP %d", code)
			}
			n.token = login.Token
			if code := request(n, "POST", "/users", map[string]string{"username": "must-not-exist-before-sync", "password": secret(t)}, nil); code != http.StatusServiceUnavailable {
				t.Fatalf("write before first trusted sample HTTP %d, want 503", code)
			}
			t.Log("write before first trusted sample rejected with HTTP 503")
		}
	}
	for i, n := range nodes {
		eventually(t, 15*time.Second, "health", func() bool { return request(n, "GET", "/healthz", nil, nil) == 200 })
		var login struct {
			Token string `json:"token"`
		}
		eventually(t, 15*time.Second, "bootstrap login", func() bool {
			return request(n, "POST", "/auth/login", map[string]any{"login_name": fmt.Sprintf("preflight-%d", i), "password": passwords[i]}, &login) == 200 && login.Token != ""
		})
		n.token = login.Token
	}
	ownerToken := ""
	readStatus := func(i int) status {
		var s status
		code := request(nodes[i], "GET", "/ops/status", nil, &s)
		// Bootstrap reconciliation deliberately keeps only the smallest node's
		// bootstrap as super_admin. Use that real authenticated identity after sync.
		if code == 403 && ownerToken != "" && nodes[i].token != ownerToken {
			nodes[i].token = ownerToken
			code = request(nodes[i], "GET", "/ops/status", nil, &s)
			t.Logf("node%d switched to replicated cluster bootstrap owner after role reconciliation", i)
		}
		if code != 200 {
			t.Fatalf("node%d ops HTTP %d", i, code)
		}
		data, _ := json.MarshalIndent(s, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("status%d.json", i)), data, 0600); err != nil {
			t.Fatal(err)
		}
		return s
	}
	deadline := time.Now().Add(15 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		a, b := readStatus(0), readStatus(1)
		if a.Ready && b.Ready && a.State == "trusted" && b.State == "trusted" {
			ready = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ready {
		for i, n := range nodes {
			s := readStatus(i)
			code := request(n, "POST", "/users", map[string]string{"username": "blocked-probe", "password": secret(t)}, nil)
			t.Logf("node%d state=%s reason=%s ready=%t last=%q peers=%+v mesh=%+v write_HTTP=%d", i, s.State, s.Reason, s.Ready, s.Last, s.Peers, s.Mesh, code)
		}
		t.Fatal("real WS nodes failed to become automatically trusted and writable")
	}
	first := []status{readStatus(0), readStatus(1)}
	if first[0].NodeID == first[1].NodeID {
		t.Fatal("node IDs must differ")
	}
	owner := 0
	if first[1].NodeID < first[0].NodeID {
		owner = 1
	}
	ownerToken = nodes[owner].token
	check := func(i int) status {
		s := readStatus(i)
		if !s.Ready || s.State != "trusted" || s.Last == "" || !s.Mesh.Enabled {
			t.Fatalf("node%d unhealthy clock state=%s ready=%t", i, s.State, s.Ready)
		}
		reachable := false
		for _, r := range s.Mesh.Routes {
			if r.Reachable && r.Transport == "websocket" {
				reachable = true
			}
		}
		if !reachable {
			t.Fatalf("node%d has no reachable websocket route", i)
		}
		trusted := false
		for _, p := range s.Peers {
			if p.Trusted && p.State == "trusted" && p.Last != "" {
				trusted = true
			}
		}
		if !trusted {
			t.Fatalf("node%d has no trusted peer", i)
		}
		return s
	}
	replicate := func(round int) {
		for i, n := range nodes {
			username := fmt.Sprintf("preflight-%d-%d", round, i)
			var user struct {
				NodeID   int64  `json:"node_id"`
				UserID   int64  `json:"user_id"`
				Username string `json:"username"`
			}
			if code := request(n, "POST", "/users", map[string]string{"username": username, "password": secret(t)}, &user); code != 201 && code != 200 {
				t.Fatalf("create node%d HTTP %d", i, code)
			}
			if user.NodeID != first[i].NodeID || user.UserID <= 0 {
				t.Fatal("invalid created user identity")
			}
			path := fmt.Sprintf("/nodes/%d/users/%d", user.NodeID, user.UserID)
			eventually(t, 15*time.Second, "replicated user", func() bool {
				var got struct {
					Username string `json:"username"`
				}
				return request(nodes[1-i], "GET", path, nil, &got) == 200 && got.Username == username
			})
			body := base64.StdEncoding.EncodeToString([]byte(username))
			if code := request(n, "POST", path+"/messages", map[string]string{"body": body}, nil); code != 201 && code != 200 {
				t.Fatalf("message node%d HTTP %d", i, code)
			}
			eventually(t, 15*time.Second, "replicated persistent message", func() bool {
				var got struct {
					Items []struct {
						Body string `json:"body"`
					} `json:"items"`
				}
				if request(nodes[1-i], "GET", path+"/messages?limit=10", nil, &got) != 200 {
					return false
				}
				for _, m := range got.Items {
					if m.Body == body {
						return true
					}
				}
				return false
			})
			t.Logf("round%d node%d -> node%d user=(%d,%d) and persistent message replicated", round, i, 1-i, user.NodeID, user.UserID)
		}
	}
	start := time.Now()
	replicate(0)
	updates := []int{0, 0}
	previous := []string{first[0].Last, first[1].Last}
	for time.Since(start) < 65*time.Second {
		for i := range nodes {
			s := check(i)
			if s.Last != previous[i] {
				updates[i]++
				previous[i] = s.Last
			}
		}
		time.Sleep(time.Second)
	}
	replicate(1)
	for i := range nodes {
		s := check(i)
		if updates[i] < 10 {
			t.Fatalf("node%d insufficient continued time sync updates: %d", i, updates[i])
		}
		t.Logf("node%d trusted duration=%s updates=%d first=%s last=%s", i, time.Since(start).Round(time.Millisecond), updates[i], first[i].Last, s.Last)
	}

	// Exercise the unchanged default 180-second observation grace using real
	// process loss. Keep the cluster bootstrap owner alive for authorization.
	peer := nodes[1-owner]
	stop(peer)
	eventually(t, 210*time.Second, "write gate closes after all peers disappear", func() bool {
		s := readStatus(owner)
		return !s.Ready && (s.State == "degraded" || s.State == "unwritable")
	})
	if code := request(nodes[owner], "POST", "/users", map[string]string{"username": "must-not-exist-after-peer-loss", "password": secret(t)}, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("write after peer-loss grace HTTP %d, want 503", code)
	}
	t.Log("write after peer-loss grace rejected with HTTP 503")

	old := peer.cmd
	peer.cmd = exec.Command(binary, old.Args[1:]...)
	peer.cmd.Stdout, peer.cmd.Stderr = old.Stdout, old.Stderr
	if err := peer.cmd.Start(); err != nil {
		t.Fatal("restart peer failed")
	}
	peer.running = true
	eventually(t, 15*time.Second, "restarted peer health", func() bool {
		return request(peer, "GET", "/healthz", nil, nil) == 200
	})
	eventually(t, 20*time.Second, "clock trust recovers after reconnect", func() bool {
		a, b := readStatus(0), readStatus(1)
		return a.Ready && b.Ready && a.State == "trusted" && b.State == "trusted"
	})
	for i := range nodes {
		if check(i).NodeID != first[i].NodeID {
			t.Fatalf("node%d identity changed after restart", i)
		}
	}
	replicate(2)
	t.Log("peer restart preserved identity and restored trusted bilateral replication")
}
