package benchroot

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestFindModuleRoot(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file")
	}

	root, err := findModuleRoot(file)
	if err != nil {
		t.Fatalf("find module root: %v", err)
	}
	if got, want := filepath.Base(root), "turntf"; got != want {
		t.Fatalf("unexpected module root basename: got=%q want=%q", got, want)
	}
}

func TestSelectModesUsesTmpOnlyOnDiskBackedTempDir(t *testing.T) {
	modes, err := selectModes(
		filesystemInfo{path: "/tmp", name: "ext4", isMemory: false},
		filesystemInfo{path: "/repo/.benchdata", name: "ext4", isMemory: false},
	)
	if err != nil {
		t.Fatalf("select modes: %v", err)
	}
	if len(modes) != 1 {
		t.Fatalf("unexpected mode count: got=%d want=1", len(modes))
	}
	if got := modes[0].Name(); got != ModeTmp {
		t.Fatalf("unexpected tmp-only mode: got=%q want=%q", got, ModeTmp)
	}
}

func TestSelectModesAddsDiskWhenTempDirIsMemoryBacked(t *testing.T) {
	modes, err := selectModes(
		filesystemInfo{path: "/tmp", name: "tmpfs", isMemory: true},
		filesystemInfo{path: "/repo/.benchdata", name: "ext4", isMemory: false},
	)
	if err != nil {
		t.Fatalf("select modes: %v", err)
	}
	if len(modes) != 2 {
		t.Fatalf("unexpected mode count: got=%d want=2", len(modes))
	}
	if got := modes[0].Name(); got != ModeTmp {
		t.Fatalf("unexpected first mode: got=%q want=%q", got, ModeTmp)
	}
	if got := modes[1].Name(); got != ModeDisk {
		t.Fatalf("unexpected second mode: got=%q want=%q", got, ModeDisk)
	}
}

func TestSelectModesRejectsMemoryBackedDiskRoot(t *testing.T) {
	_, err := selectModes(
		filesystemInfo{path: "/tmp", name: "tmpfs", isMemory: true},
		filesystemInfo{path: "/repo/.benchdata", name: "tmpfs", isMemory: true},
	)
	if err == nil {
		t.Fatal("expected error for memory-backed disk root")
	}
}
