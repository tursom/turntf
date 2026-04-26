//go:build linux

package benchroot

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func statFilesystem(path string) (filesystemInfo, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return filesystemInfo{}, err
	}
	return filesystemInfo{
		name:     filesystemTypeName(int64(stat.Type)),
		isMemory: isMemoryFSType(int64(stat.Type)),
	}, nil
}

func isMemoryFSType(fsType int64) bool {
	switch fsType {
	case int64(unix.TMPFS_MAGIC), int64(unix.RAMFS_MAGIC):
		return true
	default:
		return false
	}
}

func filesystemTypeName(fsType int64) string {
	switch fsType {
	case int64(unix.TMPFS_MAGIC):
		return "tmpfs"
	case int64(unix.RAMFS_MAGIC):
		return "ramfs"
	default:
		return fmt.Sprintf("0x%x", fsType)
	}
}
