//go:build !linux

// Package benchroot 提供基准测试的存储模式检测。
// 本文件是非 Linux 平台的降级实现，不依赖 unix.Statfs 系统调用。
// 由于无法获取文件系统类型信息，始终报告为"unknown"且非内存文件系统，
// 这会导致 selectModes 仅返回 ModeTmp（临时目录模式），不提供磁盘模式。
package benchroot

// statFilesystem 是非 Linux 平台的降级实现。
// 不执行实际的系统调用，始终返回"unknown"类型和 isMemory=false。
// 这意味着在非 Linux 平台上基准测试始终只使用临时目录模式。
func statFilesystem(path string) (filesystemInfo, error) {
	return filesystemInfo{
		name:     "unknown",
		isMemory: false,
	}, nil
}
