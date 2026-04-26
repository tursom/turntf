//go:build linux

package benchroot

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestIsMemoryFSType(t *testing.T) {
	if !isMemoryFSType(int64(unix.TMPFS_MAGIC)) {
		t.Fatalf("expected tmpfs to be memory-backed")
	}
	if !isMemoryFSType(int64(unix.RAMFS_MAGIC)) {
		t.Fatalf("expected ramfs to be memory-backed")
	}
	if isMemoryFSType(int64(unix.EXT4_SUPER_MAGIC)) {
		t.Fatalf("expected ext4 to be disk-backed")
	}
}
