package task

import (
	"os"
	"path/filepath"
	"testing"

	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/config"
	playlist "hls-accelerator/internal/m3u8"
)

func TestBuildManifestDeduplicatesByFilenameAndPreservesLastEntry(t *testing.T) {
	taskID := "task-build-manifest"
	items := []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/old.ts", Type: "segment"},
		{Filename: "00001.ts", URL: "https://example.com/new.ts", Type: "segment"},
		{Filename: "enc.key", URL: "https://example.com/enc.key", Type: "key"},
	}

	manifest := buildManifest(taskID, "https://example.com/test.m3u8", items, 1)
	if len(manifest.Items) != 2 {
		t.Fatalf("len(manifest.Items) = %d, want 2", len(manifest.Items))
	}
	if manifest.Items[0].URL != "https://example.com/old.ts" {
		t.Fatalf("first duplicate entry should keep first-seen position, got %q", manifest.Items[0].URL)
	}
	if manifest.Items[0].Path != cache.GetFilePath(taskID, "00001.ts") {
		t.Fatalf("path = %q, want %q", manifest.Items[0].Path, cache.GetFilePath(taskID, "00001.ts"))
	}
	if manifest.Items[1].Type != "key" {
		t.Fatalf("type = %q, want key", manifest.Items[1].Type)
	}
}

func TestTaskRuntimeSnapshotCountsKeysOutsideSegmentProgress(t *testing.T) {
	manifest := TaskManifest{
		TaskID:        "task-runtime",
		TotalSegments: 1,
		Items: []ManifestItem{
			{Filename: "enc.key", Type: "key"},
			{Filename: "00001.ts", Type: "segment"},
		},
	}
	progress := buildInitialProgress(manifest)
	rt := newTaskRuntime(manifest, progress, false)

	if !rt.markCompleted("enc.key") {
		t.Fatal("expected key completion to change runtime state")
	}

	_, snapshot := rt.snapshot()
	if snapshot.DoneItems != 1 {
		t.Fatalf("done_items = %d, want 1", snapshot.DoneItems)
	}
	if snapshot.DownloadedSegments != 0 {
		t.Fatalf("downloaded_segments = %d, want 0", snapshot.DownloadedSegments)
	}
	if snapshot.Status != TaskStatusDownloading {
		t.Fatalf("status = %q, want %q", snapshot.Status, TaskStatusDownloading)
	}

	if !rt.markCompleted("00001.ts") {
		t.Fatal("expected segment completion to change runtime state")
	}
	_, snapshot = rt.snapshot()
	if snapshot.DownloadedSegments != 1 {
		t.Fatalf("downloaded_segments = %d, want 1", snapshot.DownloadedSegments)
	}
	if snapshot.Status != TaskStatusCompleted {
		t.Fatalf("status = %q, want %q", snapshot.Status, TaskStatusCompleted)
	}
}

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

	filePath := filepath.Join(cache.GetTaskDir(taskID), filename)
	controlPath := filepath.Join(cache.GetTaskDir(taskID), filename+".aria2")
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
