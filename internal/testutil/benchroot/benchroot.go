// Package benchroot 提供基准测试（benchmark）的临时目录管理工具。
// 自动检测临时目录所在的文件系统类型（内存盘 vs 磁盘），并在内存盘时额外
// 提供基于项目仓库的磁盘模式，使基准测试能区分内存和磁盘两种存储性能。
package benchroot

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// ModeTmp 表示使用系统临时目录（os.TempDir()）的模式名。
const ModeTmp = "tmp"

// ModeDisk 表示使用项目仓库下 .benchdata 目录的模式名，
// 仅在临时目录位于内存文件系统时才会启用此模式。
const ModeDisk = "disk"

// diskRootName 是磁盘模式下在项目根目录下创建的数据目录名称。
const diskRootName = ".benchdata"

// Mode 描述一种基准测试存储模式，包含模式名称和根目录路径。
type Mode struct {
	// name 模式名称，如 "tmp" 或 "disk"
	name string
	// root 该模式对应的根目录路径
	root string
	// useDefaultTmp 是否使用系统默认临时目录（此时 root 仅用于标识）
	useDefaultTmp bool
}

// Name 返回模式名称。
func (m Mode) Name() string {
	return m.name
}

// Root 返回模式根目录路径。
func (m Mode) Root() string {
	return m.root
}

// MkdirTemp 在该模式下创建一个临时目录，并返回清理函数。
// tb 用于记录测试失败信息。
// pattern 传递给 os.MkdirTemp 作为目录名模板。
// 返回创建的临时目录路径和清理函数（调用者应在基准测试结束时执行清理）。
func (m Mode) MkdirTemp(tb testing.TB, pattern string) (string, func()) {
	tb.Helper()

	base := ""
	if !m.useDefaultTmp {
		// 磁盘模式：确保根目录存在
		base = filepath.Clean(m.root)
		if err := os.MkdirAll(base, 0o755); err != nil {
			tb.Fatalf("create benchmark root %q: %v", base, err)
		}
	}

	dir, err := os.MkdirTemp(base, pattern)
	if err != nil {
		tb.Fatalf("create benchmark temp dir in mode %q: %v", m.name, err)
	}
	return dir, func() {
		_ = os.RemoveAll(dir)
	}
}

var (
	cachedModesOnce sync.Once
	cachedModes     []Mode
	cachedModesErr  error
)

// Modes 返回当前系统可用的基准测试存储模式列表。
// 结果被缓存（sync.Once），多次调用返回同一份数据的副本。
// 至少返回包含 ModeTmp 的模式，当临时目录位于内存文件系统时，
// 额外返回 ModeDisk 以提供真实的磁盘 I/O 性能测试。
func Modes(tb testing.TB) []Mode {
	tb.Helper()

	cachedModesOnce.Do(func() {
		cachedModes, cachedModesErr = detectModes()
	})
	if cachedModesErr != nil {
		tb.Fatalf("detect benchmark storage modes: %v", cachedModesErr)
	}
	return append([]Mode(nil), cachedModes...)
}

// filesystemInfo 描述一个路径所在文件系统的信息。
type filesystemInfo struct {
	// path 被检查的路径
	path string
	// name 文件系统类型名称（如 "tmpfs"、"ext4"）
	name string
	// isMemory 该文件系统是否为内存文件系统
	isMemory bool
}

// detectModes 探测系统临时目录和项目仓库目录的文件系统类型，
// 然后通过 selectModes 决定可用的存储模式。
func detectModes() ([]Mode, error) {
	tmpRoot := os.TempDir()
	tmpFS, err := statFilesystem(tmpRoot)
	if err != nil {
		return nil, fmt.Errorf("stat temp dir filesystem %q: %w", tmpRoot, err)
	}
	tmpFS.path = tmpRoot

	repoRoot, err := moduleRoot()
	if err != nil {
		return nil, err
	}
	diskRoot := filepath.Join(repoRoot, diskRootName)
	diskFS, err := statFilesystem(repoRoot)
	if err != nil {
		return nil, fmt.Errorf("stat repo filesystem %q: %w", repoRoot, err)
	}
	diskFS.path = diskRoot

	return selectModes(tmpFS, diskFS)
}

// selectModes 根据临时目录和磁盘根目录的文件系统信息选择可用模式。
// 决策逻辑：
//   - 始终包含 ModeTmp（临时目录模式）
//   - 如果临时目录不在内存文件系统上，则磁盘 I/O 性能已接近真实，
//     只返回 ModeTmp
//   - 如果临时目录在内存上，且磁盘也在内存上，返回错误（无法提供磁盘测试）
//   - 如果临时目录在内存上，且磁盘在真实文件系统上，额外添加 ModeDisk
func selectModes(tmpFS, diskFS filesystemInfo) ([]Mode, error) {
	modes := []Mode{{
		name:          ModeTmp,
		root:          tmpFS.path,
		useDefaultTmp: true,
	}}
	if !tmpFS.isMemory {
		return modes, nil
	}
	if diskFS.isMemory {
		return nil, fmt.Errorf("temp dir %q and disk root %q are both on memory filesystems (%s, %s)", tmpFS.path, diskFS.path, tmpFS.name, diskFS.name)
	}
	modes = append(modes, Mode{
		name: ModeDisk,
		root: diskFS.path,
	})
	return modes, nil
}

// moduleRoot 通过运行时调用栈信息找到当前模块的根目录（包含 go.mod 的目录）。
func moduleRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve current file")
	}
	return findModuleRoot(file)
}

// findModuleRoot 从 startFile 所在目录开始逐级向上查找 go.mod 文件，
// 返回包含 go.mod 的目录绝对路径。
func findModuleRoot(startFile string) (string, error) {
	dir := filepath.Dir(startFile)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		} else if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat go.mod in %q: %w", dir, err)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find go.mod above %q", startFile)
		}
		dir = parent
	}
}
