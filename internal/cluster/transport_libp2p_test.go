package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestLoadOrCreateLibP2PPrivateKeyStableAcrossRestart(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "node.key")
	keyA, err := loadOrCreateLibP2PPrivateKey(path)
	if err != nil {
		t.Fatalf("create libp2p key: %v", err)
	}
	keyB, err := loadOrCreateLibP2PPrivateKey(path)
	if err != nil {
		t.Fatalf("reload libp2p key: %v", err)
	}

	idA, err := libp2ppeer.IDFromPrivateKey(keyA)
	if err != nil {
		t.Fatalf("derive peer id from first key: %v", err)
	}
	idB, err := libp2ppeer.IDFromPrivateKey(keyB)
	if err != nil {
		t.Fatalf("derive peer id from second key: %v", err)
	}
	if idA != idB {
		t.Fatalf("expected stable peer id, got %s and %s", idA, idB)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("unexpected key file permissions: %o", got)
	}
}

func TestLoadOrCreateLibP2PPrivateKeyRejectsBrokenFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "broken.key")
	if err := os.WriteFile(path, []byte("not-a-private-key"), 0o600); err != nil {
		t.Fatalf("write broken key: %v", err)
	}

	_, err := loadOrCreateLibP2PPrivateKey(path)
	if err == nil || !strings.Contains(err.Error(), "unmarshal libp2p private key") {
		t.Fatalf("unexpected error: %v", err)
	}
}
