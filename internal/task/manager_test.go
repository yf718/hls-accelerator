package task

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/config"
	"hls-accelerator/internal/downloader"
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
	rt := newTaskRuntime("task-runtime", 2, 1, []ManifestIndexItem{
		{Seq: 0, Filename: "enc.key", IsSegment: false},
		{Seq: 1, Filename: "00001.ts", IsSegment: true},
	}, TaskProgressFile{TaskID: "task-runtime", Failed: []string{}}, false)

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
	rt := newTaskRuntime("task-progress", 2, 2, []ManifestIndexItem{
		{Seq: 0, Filename: "00001.ts", IsSegment: true},
		{Seq: 1, Filename: "00002.ts", IsSegment: true},
	}, TaskProgressFile{TaskID: "task-progress", Failed: []string{}}, false)

	if !rt.markCompleted("00001.ts") {
		t.Fatal("expected first item to complete")
	}
	if !rt.markFailed("00002.ts", "boom") {
		t.Fatal("expected second item to fail")
	}

	progress, snapshot := rt.snapshot()
	if progress.TaskID != "task-progress" {
		t.Fatalf("task id = %q, want %q", progress.TaskID, "task-progress")
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
	rt := newTaskRuntime("task-index-map", 2, 2, []ManifestIndexItem{
		{Seq: 0, Filename: "00001.ts", IsSegment: true},
		{Seq: 1, Filename: "00002.ts", IsSegment: true},
	}, TaskProgressFile{TaskID: "task-index-map", Failed: []string{}}, false)

	index, ok := rt.remaining["00002.ts"]
	if !ok {
		t.Fatal("expected remaining entry for 00002.ts")
	}
	if index != 1 {
		t.Fatalf("remaining index = %d, want 1", index)
	}
	if !rt.isSegment(index) {
		t.Fatal("expected sequence 1 to be marked as segment")
	}
}

func TestCompletedTaskRejectsPauseResumeRetryWithoutLoadingRuntime(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	m, err := NewManager(nil, db)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	meta := TaskMetadata{
		ID:                 "completed-task",
		Name:               "completed-task",
		OriginalURL:        "https://example.com/completed.m3u8",
		CreatedTime:        time.Now(),
		UpdatedTime:        time.Now(),
		TotalItems:         1,
		TotalSegments:      1,
		DoneItems:          1,
		DownloadedSegments: 1,
		Status:             TaskStatusCompleted,
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	if _, err := m.PauseTask(meta.ID); err == nil {
		t.Fatal("PauseTask should reject completed task")
	}
	if _, err := m.ResumeTask(meta.ID); err == nil {
		t.Fatal("ResumeTask should reject completed task")
	}
	if _, err := m.RetryTask(meta.ID); err == nil {
		t.Fatal("RetryTask should reject completed task")
	}

	m.runtimeMu.Lock()
	_, loaded := m.runtimes[meta.ID]
	m.runtimeMu.Unlock()
	if loaded {
		t.Fatal("completed task should not load runtime for pause/resume/retry")
	}
}

func TestRuntimeMetricsReportsRuntimeDirtyAndFlushStats(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	m, err := NewManager(nil, db)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	rt := newTaskRuntime("metrics-task", 1, 1, []ManifestIndexItem{
		{Seq: 0, Filename: "00001.ts", IsSegment: true},
	}, TaskProgressFile{TaskID: "metrics-task", Failed: []string{}}, false)
	rt.mu.Lock()
	rt.markDirtyLocked()
	rt.mu.Unlock()

	m.runtimeMu.Lock()
	m.runtimes["metrics-task"] = rt
	m.runtimeMu.Unlock()

	m.dispatchMu.Lock()
	m.dispatches["metrics-task"] = func() {}
	m.dispatchMu.Unlock()

	m.recordFlushCost(12 * time.Millisecond)
	m.recordFlushCost(8 * time.Millisecond)

	metrics := m.RuntimeMetrics()
	if metrics.RuntimeCount != 1 {
		t.Fatalf("runtime_count = %d, want 1", metrics.RuntimeCount)
	}
	if metrics.DirtyRuntimeCount != 1 {
		t.Fatalf("dirty_runtime_count = %d, want 1", metrics.DirtyRuntimeCount)
	}
	if metrics.ActiveDispatches != 1 {
		t.Fatalf("active_dispatches = %d, want 1", metrics.ActiveDispatches)
	}
	if metrics.LastFlushCostMs != 8 {
		t.Fatalf("last_flush_cost_ms = %d, want 8", metrics.LastFlushCostMs)
	}
	if metrics.AverageFlushCostMs != 10 {
		t.Fatalf("average_flush_cost_ms = %d, want 10", metrics.AverageFlushCostMs)
	}
}

func TestNextPurgeTimeTargetsThreeAM(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 4, 23, 2, 15, 0, 0, loc)
	next := nextPurgeTime(now)
	want := time.Date(2026, 4, 23, 3, 0, 0, 0, loc)
	if !next.Equal(want) {
		t.Fatalf("next purge = %v, want %v", next, want)
	}

	now = time.Date(2026, 4, 23, 3, 15, 0, 0, loc)
	next = nextPurgeTime(now)
	want = time.Date(2026, 4, 24, 3, 0, 0, 0, loc)
	if !next.Equal(want) {
		t.Fatalf("next purge after 3am = %v, want %v", next, want)
	}
}

func TestDeleteTaskAsyncCleansAria2ByDirImmediately(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var multicallCount int
	var dirExistsDuringCleanup bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req downloader.JsonRpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		resp := downloader.JsonRpcResponse{ID: req.ID}
		switch req.Method {
		case "aria2.tellActive":
			if _, err := os.Stat(cache.GetTaskDir("delete-task")); err == nil {
				dirExistsDuringCleanup = true
			}
			resp.Result = []map[string]string{
				{"gid": "active-1", "dir": cache.GetTaskDir("delete-task")},
			}
		case "aria2.tellWaiting":
			resp.Result = []map[string]string{}
		case "aria2.tellStopped":
			resp.Result = []map[string]string{
				{"gid": "stopped-1", "dir": cache.GetTaskDir("delete-task")},
			}
		case "system.multicall":
			multicallCount++
			resp.Result = []interface{}{
				[]interface{}{"OK"},
				[]interface{}{"OK"},
				[]interface{}{"OK"},
				[]interface{}{"OK"},
			}
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	oldCacheDir := config.GlobalConfig.CacheDir
	tempDir := t.TempDir()
	config.GlobalConfig.CacheDir = tempDir
	t.Cleanup(func() {
		config.GlobalConfig.CacheDir = oldCacheDir
	})

	m := &Manager{
		aria2: &downloader.Aria2Client{
			RPCUrl: srv.URL,
			Client: &http.Client{Timeout: time.Second},
		},
		db:        db,
		deleteSem: make(chan struct{}, 1),
		runtimes:  make(map[string]*taskRuntime),
	}
	if err := m.InitTable(); err != nil {
		t.Fatalf("InitTable: %v", err)
	}

	meta := TaskMetadata{
		ID:            "delete-task",
		Name:          "delete-task",
		OriginalURL:   "https://example.com/delete.m3u8",
		CreatedTime:   time.Now(),
		UpdatedTime:   time.Now(),
		TotalItems:    1,
		TotalSegments: 1,
		Status:        TaskStatusDeleted,
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := cache.EnsureTaskDir(meta.ID); err != nil {
		t.Fatalf("EnsureTaskDir: %v", err)
	}
	if err := os.WriteFile(taskProgressPath(meta.ID), []byte("{}"), 0644); err != nil {
		t.Fatalf("WriteFile progress: %v", err)
	}

	m.deleteTaskAsync(meta.ID)

	if multicallCount != 1 {
		t.Fatalf("system.multicall count = %d, want 1", multicallCount)
	}
	if !dirExistsDuringCleanup {
		t.Fatal("task dir should still exist when aria2 cleanup starts")
	}
	if _, err := os.Stat(taskProgressPath(meta.ID)); !os.IsNotExist(err) {
		t.Fatalf("progress file still exists, stat err=%v", err)
	}
	if _, err := os.Stat(cache.GetTaskDir(meta.ID)); !os.IsNotExist(err) {
		t.Fatalf("task dir still exists, stat err=%v", err)
	}
	if _, err := m.GetTask(meta.ID); err == nil {
		t.Fatal("task should be removed from db")
	}
}
