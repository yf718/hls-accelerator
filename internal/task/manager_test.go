package task

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/config"
	playlist "hls-accelerator/internal/m3u8"
	_ "modernc.org/sqlite"
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

func TestCleanupInactiveRuntimesEvictsPausedButKeepsDownloading(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	m, err := NewManager(nil, db)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	oldCacheDir := config.GlobalConfig.CacheDir
	tempDir := t.TempDir()
	config.GlobalConfig.CacheDir = tempDir
	t.Cleanup(func() {
		config.GlobalConfig.CacheDir = oldCacheDir
	})

	pausedMeta := TaskMetadata{
		ID:            "paused-task",
		Name:          "paused-task",
		OriginalURL:   "https://example.com/paused.m3u8",
		CreatedTime:   time.Now(),
		UpdatedTime:   time.Now(),
		TotalItems:    1,
		TotalSegments: 1,
		Status:        TaskStatusPaused,
	}
	downloadingMeta := TaskMetadata{
		ID:            "downloading-task",
		Name:          "downloading-task",
		OriginalURL:   "https://example.com/downloading.m3u8",
		CreatedTime:   time.Now(),
		UpdatedTime:   time.Now(),
		TotalItems:    1,
		TotalSegments: 1,
		Status:        TaskStatusDownloading,
	}
	if err := m.CreateTask(pausedMeta); err != nil {
		t.Fatalf("CreateTask paused: %v", err)
	}
	if err := m.CreateTask(downloadingMeta); err != nil {
		t.Fatalf("CreateTask downloading: %v", err)
	}

	pausedManifest := buildManifest(pausedMeta.ID, pausedMeta.OriginalURL, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/paused-1.ts", Type: "segment"},
	}, 1)
	pausedProgress := buildInitialProgress(pausedManifest)
	if err := m.SaveTaskManifest(pausedManifest); err != nil {
		t.Fatalf("save paused manifest: %v", err)
	}
	if err := writeJSONAtomic(taskProgressPath(pausedMeta.ID), pausedProgress); err != nil {
		t.Fatalf("write paused progress: %v", err)
	}

	downloadingManifest := buildManifest(downloadingMeta.ID, downloadingMeta.OriginalURL, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/downloading-1.ts", Type: "segment"},
	}, 1)
	downloadingProgress := buildInitialProgress(downloadingManifest)
	if err := m.SaveTaskManifest(downloadingManifest); err != nil {
		t.Fatalf("save downloading manifest: %v", err)
	}
	if err := writeJSONAtomic(taskProgressPath(downloadingMeta.ID), downloadingProgress); err != nil {
		t.Fatalf("write downloading progress: %v", err)
	}

	pausedRT, err := m.loadRuntime(pausedMeta.ID)
	if err != nil {
		t.Fatalf("loadRuntime paused: %v", err)
	}
	downloadingRT, err := m.loadRuntime(downloadingMeta.ID)
	if err != nil {
		t.Fatalf("loadRuntime downloading: %v", err)
	}

	pausedRT.mu.Lock()
	pausedRT.lastAccessAt = time.Now().Add(-pausedRuntimeTTL - time.Minute)
	pausedRT.mu.Unlock()
	downloadingRT.mu.Lock()
	downloadingRT.lastAccessAt = time.Now().Add(-pausedRuntimeTTL - time.Minute)
	downloadingRT.mu.Unlock()

	m.cleanupInactiveRuntimes()

	m.runtimeMu.Lock()
	_, pausedExists := m.runtimes[pausedMeta.ID]
	_, downloadingExists := m.runtimes[downloadingMeta.ID]
	m.runtimeMu.Unlock()

	if pausedExists {
		t.Fatal("paused runtime should be evicted after ttl")
	}
	if !downloadingExists {
		t.Fatal("downloading runtime should stay resident")
	}
}

func TestProgressSnapshotUsesCompactFailedOnlyFormat(t *testing.T) {
	manifest := TaskManifest{
		TaskID:        "task-progress",
		TotalSegments: 2,
		Items: []ManifestItem{
			{Filename: "00001.ts", Type: "segment"},
			{Filename: "00002.ts", Type: "segment"},
		},
	}
	rt := newTaskRuntime(manifest, buildInitialProgress(manifest), false)

	if !rt.markCompleted("00001.ts") {
		t.Fatal("expected first item to complete")
	}
	if !rt.markFailed("00002.ts", "boom") {
		t.Fatal("expected second item to fail")
	}

	progress, snapshot := rt.snapshot()
	if progress.TaskID != manifest.TaskID {
		t.Fatalf("task id = %q, want %q", progress.TaskID, manifest.TaskID)
	}
	if len(progress.Failed) != 1 || progress.Failed[0] != "00002.ts" {
		t.Fatalf("failed list = %#v, want only failed item name", progress.Failed)
	}
	if progress.DoneItems != 1 {
		t.Fatalf("done_items = %d, want 1", progress.DoneItems)
	}
	if progress.DownloadedSegments != 1 {
		t.Fatalf("downloaded_segments = %d, want 1", progress.DownloadedSegments)
	}
	if snapshot.Status != TaskStatusFailed {
		t.Fatalf("status = %q, want %q", snapshot.Status, TaskStatusFailed)
	}
}

func TestNewTaskRuntimeStoresRemainingAsManifestIndexes(t *testing.T) {
	manifest := TaskManifest{
		TaskID: "task-index-map",
		Items: []ManifestItem{
			{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "segment"},
			{Filename: "00002.ts", URL: "https://example.com/2.ts", Type: "segment"},
		},
	}

	rt := newTaskRuntime(manifest, buildInitialProgress(manifest), false)

	index, ok := rt.remaining["00002.ts"]
	if !ok {
		t.Fatal("expected remaining entry for 00002.ts")
	}
	if got := rt.manifest.Items[index].URL; got != "https://example.com/2.ts" {
		t.Fatalf("manifest lookup url = %q, want %q", got, "https://example.com/2.ts")
	}
}
