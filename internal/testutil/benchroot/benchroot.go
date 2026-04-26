package benchroot

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

const (
	ModeTmp  = "tmp"
	ModeDisk = "disk"
)

const diskRootName = ".benchdata"

type Mode struct {
	name          string
	root          string
	useDefaultTmp bool
}

func (m Mode) Name() string {
	return m.name
}

func (m Mode) Root() string {
	return m.root
}

func (m Mode) MkdirTemp(tb testing.TB, pattern string) (string, func()) {
	tb.Helper()

	base := ""
	if !m.useDefaultTmp {
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

type filesystemInfo struct {
	path     string
	name     string
	isMemory bool
}

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

func moduleRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve current file")
	}
	return findModuleRoot(file)
}

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
