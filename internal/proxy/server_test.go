package proxy

import (
	"os"
	"path/filepath"
	"testing"

	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/config"
)

func TestCleanupResumeArtifactsRemovesPartialAndControlFiles(t *testing.T) {
	oldCacheDir := config.GlobalConfig.CacheDir
	tempDir := t.TempDir()
	config.GlobalConfig.CacheDir = tempDir
	t.Cleanup(func() {
		config.GlobalConfig.CacheDir = oldCacheDir
	})

	const taskID = "task-cleanup"
	const filename = "00001.ts"

	if err := cache.EnsureTaskDir(taskID); err != nil {
		t.Fatalf("EnsureTaskDir: %v", err)
	}

	taskDir := cache.GetTaskDir(taskID)
	filePath := filepath.Join(taskDir, filename)
	controlPath := filepath.Join(taskDir, filename+".aria2")

	if err := os.WriteFile(filePath, []byte("partial"), 0644); err != nil {
		t.Fatalf("WriteFile partial: %v", err)
	}
	if err := os.WriteFile(controlPath, []byte("control"), 0644); err != nil {
		t.Fatalf("WriteFile control: %v", err)
	}

	if err := cleanupResumeArtifacts(taskID, filename); err != nil {
		t.Fatalf("cleanupResumeArtifacts: %v", err)
	}

	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("partial file still exists, stat err=%v", err)
	}
	if _, err := os.Stat(controlPath); !os.IsNotExist(err) {
		t.Fatalf("control file still exists, stat err=%v", err)
	}
}
