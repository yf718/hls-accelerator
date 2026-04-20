package task

import (
	"os"
	"path/filepath"
	"testing"

	"hls-accelerator/internal/config"
)

func TestPrepareTaskDeletionPathsDetachesActiveDirAndKeepsStaleDirs(t *testing.T) {
	oldCacheDir := config.GlobalConfig.CacheDir
	tempDir := t.TempDir()
	config.GlobalConfig.CacheDir = tempDir
	t.Cleanup(func() {
		config.GlobalConfig.CacheDir = oldCacheDir
	})

	const taskID = "task-123"

	activeDir := filepath.Join(tempDir, taskID)
	if err := os.MkdirAll(activeDir, 0755); err != nil {
		t.Fatalf("MkdirAll active dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(activeDir, "seg.ts"), []byte("data"), 0644); err != nil {
		t.Fatalf("WriteFile active file: %v", err)
	}

	staleDir := filepath.Join(tempDir, ".deleting", taskID+"-stale")
	if err := os.MkdirAll(staleDir, 0755); err != nil {
		t.Fatalf("MkdirAll stale dir: %v", err)
	}

	m := &Manager{}
	paths, err := m.prepareTaskDeletionPaths(taskID)
	if err != nil {
		t.Fatalf("prepareTaskDeletionPaths: %v", err)
	}

	if _, err := os.Stat(activeDir); !os.IsNotExist(err) {
		t.Fatalf("expected active dir to be detached, stat err=%v", err)
	}

	if len(paths) != 2 {
		t.Fatalf("prepareTaskDeletionPaths returned %d paths, want 2", len(paths))
	}

	foundStale := false
	foundDetached := false
	for _, path := range paths {
		if path == staleDir {
			foundStale = true
		}
		if filepath.Dir(path) == filepath.Join(tempDir, ".deleting") && filepath.Base(path) != filepath.Base(staleDir) {
			foundDetached = true
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected path %s to exist: %v", path, err)
		}
	}

	if !foundStale {
		t.Fatal("stale detached dir was not included")
	}
	if !foundDetached {
		t.Fatal("newly detached dir was not included")
	}
}

func TestUniquePathsDeduplicatesEntries(t *testing.T) {
	got := uniquePaths([]string{"a", "", "a", "b"})
	want := []string{"a", "b"}

	if len(got) != len(want) {
		t.Fatalf("uniquePaths length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("uniquePaths[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
