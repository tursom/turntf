//go:build linux

// Package benchroot 提供基准测试的存储模式检测。
// 本文件是 Linux 平台实现，使用 unix.Statfs 系统调用来获取
// 文件系统类型信息，以区分内存文件系统（tmpfs/ramfs）和磁盘文件系统。
package benchroot

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// statFilesystem 通过 unix.Statfs 系统调用获取指定路径所在文件系统的信息。
// 返回文件系统名称和是否是内存文件系统的标记。
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

// isMemoryFSType 判断给定的文件系统类型 ID 是否为内存文件系统。
// 目前支持的 ID 包括 TMPFS_MAGIC 和 RAMFS_MAGIC。
func isMemoryFSType(fsType int64) bool {
	switch fsType {
	case int64(unix.TMPFS_MAGIC), int64(unix.RAMFS_MAGIC):
		return true
	default:
		return false
	}
}

// filesystemTypeName 将文件系统类型 ID 转换为人类可读的名称。
// 已知类型返回 "tmpfs" 或 "ramfs"，未知类型以十六进制格式返回。
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
