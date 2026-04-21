package task

import (
	"database/sql"
	playlist "hls-accelerator/internal/m3u8"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	m := &Manager{db: db}
	if err := m.InitTable(); err != nil {
		t.Fatalf("InitTable: %v", err)
	}
	return m
}

func TestMarkTaskItemCompletedByGIDIsIdempotent(t *testing.T) {
	m := newTestManager(t)

	meta := TaskMetadata{
		ID:                 "task-1",
		Name:               "test-task",
		OriginalURL:        "https://example.com/test.m3u8",
		TotalSegments:      2,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItem(meta.ID, "seg-1.ts", "gid-1", "https://example.com/seg-1.ts", "ts"); err != nil {
		t.Fatalf("CreateTaskItem seg-1: %v", err)
	}
	if err := m.CreateTaskItem(meta.ID, "seg-2.ts", "gid-2", "https://example.com/seg-2.ts", "ts"); err != nil {
		t.Fatalf("CreateTaskItem seg-2: %v", err)
	}

	taskID, updated, err := m.MarkTaskItemCompletedByGID("gid-1")
	if err != nil {
		t.Fatalf("MarkTaskItemCompletedByGID first call: %v", err)
	}
	if !updated || taskID != meta.ID {
		t.Fatalf("first completion = (%q, %v), want (%q, true)", taskID, updated, meta.ID)
	}

	taskAfterFirst, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask after first completion: %v", err)
	}
	if taskAfterFirst.DownloadedSegments != 1 {
		t.Fatalf("downloaded after first completion = %d, want 1", taskAfterFirst.DownloadedSegments)
	}
	if taskAfterFirst.Status != "downloading" {
		t.Fatalf("status after first completion = %q, want downloading", taskAfterFirst.Status)
	}

	_, updated, err = m.MarkTaskItemCompletedByGID("gid-1")
	if err != nil {
		t.Fatalf("MarkTaskItemCompletedByGID second call: %v", err)
	}
	if updated {
		t.Fatal("second completion should be ignored")
	}

	if _, updated, err = m.MarkTaskItemCompletedByGID("gid-2"); err != nil {
		t.Fatalf("MarkTaskItemCompletedByGID final call: %v", err)
	} else if !updated {
		t.Fatal("expected second segment completion to update progress")
	}

	taskAfterSecond, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask after second completion: %v", err)
	}
	if taskAfterSecond.DownloadedSegments != 2 {
		t.Fatalf("downloaded after second completion = %d, want 2", taskAfterSecond.DownloadedSegments)
	}
	if taskAfterSecond.Status != "completed" {
		t.Fatalf("status after second completion = %q, want completed", taskAfterSecond.Status)
	}
}

func TestTryCreateTaskUsesUniqueIndexAsArbitration(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-unique",
		Name:               "unique-task",
		OriginalURL:        "https://example.com/unique.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}

	created, err := m.TryCreateTask(meta)
	if err != nil {
		t.Fatalf("TryCreateTask first: %v", err)
	}
	if !created {
		t.Fatal("expected first TryCreateTask to create task")
	}

	created, err = m.TryCreateTask(meta)
	if err != nil {
		t.Fatalf("TryCreateTask second: %v", err)
	}
	if created {
		t.Fatal("expected duplicate TryCreateTask to lose arbitration")
	}
}

func TestEnqueueProgressNotificationDedupesInflightAndRecent(t *testing.T) {
	m := newTestManager(t)
	m.progressNotifyCh = make(chan string, 2)
	m.progressInflight = make(map[string]struct{})
	m.progressRecent = make(map[string]time.Time)

	if !m.enqueueProgressNotification("gid-1") {
		t.Fatal("expected first enqueue to succeed")
	}
	if m.enqueueProgressNotification("gid-1") {
		t.Fatal("expected inflight gid to be deduped")
	}

	m.finishProgressNotification("gid-1")
	if m.enqueueProgressNotification("gid-1") {
		t.Fatal("expected recent gid to be deduped")
	}

	m.progressDeduperMu.Lock()
	m.progressRecent["gid-1"] = time.Now().Add(-time.Second)
	m.progressDeduperMu.Unlock()

	if !m.enqueueProgressNotification("gid-1") {
		t.Fatal("expected expired recent gid to enqueue again")
	}
}

func TestMarkTaskItemCompletedByGIDDoesNotCountKeysTowardCompletion(t *testing.T) {
	m := newTestManager(t)

	meta := TaskMetadata{
		ID:                 "task-with-key",
		Name:               "test-task-with-key",
		OriginalURL:        "https://example.com/test-key.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItem(meta.ID, "enc.key", "gid-key", "https://example.com/enc.key", "key"); err != nil {
		t.Fatalf("CreateTaskItem key: %v", err)
	}
	if err := m.CreateTaskItem(meta.ID, "seg-1.ts", "gid-seg", "https://example.com/seg-1.ts", "ts"); err != nil {
		t.Fatalf("CreateTaskItem seg-1: %v", err)
	}

	if _, updated, err := m.MarkTaskItemCompletedByGID("gid-key"); err != nil {
		t.Fatalf("MarkTaskItemCompletedByGID key: %v", err)
	} else if !updated {
		t.Fatal("expected key completion to update item state")
	}

	taskAfterKey, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask after key completion: %v", err)
	}
	if taskAfterKey.DownloadedSegments != 0 {
		t.Fatalf("downloaded after key completion = %d, want 0", taskAfterKey.DownloadedSegments)
	}
	if taskAfterKey.Status != "downloading" {
		t.Fatalf("status after key completion = %q, want downloading", taskAfterKey.Status)
	}

	if _, updated, err := m.MarkTaskItemCompletedByGID("gid-seg"); err != nil {
		t.Fatalf("MarkTaskItemCompletedByGID segment: %v", err)
	} else if !updated {
		t.Fatal("expected segment completion to update progress")
	}

	taskAfterSegment, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask after segment completion: %v", err)
	}
	if taskAfterSegment.DownloadedSegments != 1 {
		t.Fatalf("downloaded after segment completion = %d, want 1", taskAfterSegment.DownloadedSegments)
	}
	if taskAfterSegment.Status != "completed" {
		t.Fatalf("status after segment completion = %q, want completed", taskAfterSegment.Status)
	}
}

func TestMarkTaskItemCompletedByGIDPreservesStoppingStatus(t *testing.T) {
	m := newTestManager(t)

	meta := TaskMetadata{
		ID:                 "task-stopping-gid",
		Name:               "stopping-gid",
		OriginalURL:        "https://example.com/stopping-gid.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "stopping",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItem(meta.ID, "seg-1.ts", "gid-1", "https://example.com/seg-1.ts", "ts"); err != nil {
		t.Fatalf("CreateTaskItem: %v", err)
	}

	if _, updated, err := m.MarkTaskItemCompletedByGID("gid-1"); err != nil {
		t.Fatalf("MarkTaskItemCompletedByGID: %v", err)
	} else if !updated {
		t.Fatal("expected completion to update progress")
	}

	taskAfter, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if taskAfter.DownloadedSegments != 1 {
		t.Fatalf("downloaded = %d, want 1", taskAfter.DownloadedSegments)
	}
	if taskAfter.Status != "stopping" {
		t.Fatalf("status = %q, want stopping", taskAfter.Status)
	}
}

func TestCreateTaskItemsPlaceholdersMarkSubmittingAndBind(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-placeholders",
		Name:               "placeholder-task",
		OriginalURL:        "https://example.com/test.m3u8",
		TotalSegments:      2,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	items := []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
		{Filename: "00002.ts", URL: "https://example.com/2.ts", Type: "ts"},
	}
	if err := m.CreateTaskItemsPlaceholders(meta.ID, items); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders: %v", err)
	}

	taskItems, err := m.GetTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("GetTaskItems: %v", err)
	}
	if len(taskItems) != 2 {
		t.Fatalf("len(taskItems) = %d, want 2", len(taskItems))
	}
	for _, item := range taskItems {
		if item.Status != taskItemStatusPending {
			t.Fatalf("placeholder status = %q, want %q", item.Status, taskItemStatusPending)
		}
		if item.Aria2GID != "" {
			t.Fatalf("placeholder gid = %q, want empty", item.Aria2GID)
		}
	}

	pending, err := m.ListPendingTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("ListPendingTaskItems: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("len(pending) = %d, want 2", len(pending))
	}

	marked, err := m.MarkTaskItemSubmitting(meta.ID, pending[0].Filename)
	if err != nil {
		t.Fatalf("MarkTaskItemSubmitting: %v", err)
	}
	if !marked {
		t.Fatal("expected first pending item to become submitting")
	}

	taskItems, err = m.GetTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("GetTaskItems after mark: %v", err)
	}
	var submitting TaskItem
	foundSubmitting := false
	for _, item := range taskItems {
		if item.Status == taskItemStatusSubmitting {
			submitting = item
			foundSubmitting = true
			break
		}
	}
	if !foundSubmitting {
		t.Fatal("expected one task item in submitting state")
	}

	if err := m.BindTaskItemToAria2(meta.ID, submitting.Filename, "gid-claimed"); err != nil {
		t.Fatalf("BindTaskItemToAria2: %v", err)
	}

	var status string
	var gid sql.NullString
	err = m.db.QueryRow(`SELECT status, aria2_gid FROM task_item WHERE task_id = ? AND filename = ?`, meta.ID, submitting.Filename).Scan(&status, &gid)
	if err != nil {
		t.Fatalf("query bound item: %v", err)
	}
	if status != taskItemStatusQueued {
		t.Fatalf("bound status = %q, want %q", status, taskItemStatusQueued)
	}
	if !gid.Valid || gid.String != "gid-claimed" {
		t.Fatalf("bound gid = (%v, %q), want (true, gid-claimed)", gid.Valid, gid.String)
	}
}

func TestMarkTaskItemCompletedByFilenamePreservesStoppingStatus(t *testing.T) {
	m := newTestManager(t)

	meta := TaskMetadata{
		ID:                 "task-stopping-file",
		Name:               "stopping-file",
		OriginalURL:        "https://example.com/stopping-file.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "stopping",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItemsPlaceholders(meta.ID, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
	}); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders: %v", err)
	}

	if updated, err := m.MarkTaskItemCompletedByFilename(meta.ID, "00001.ts"); err != nil {
		t.Fatalf("MarkTaskItemCompletedByFilename: %v", err)
	} else if !updated {
		t.Fatal("expected completion to update progress")
	}

	taskAfter, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if taskAfter.DownloadedSegments != 1 {
		t.Fatalf("downloaded = %d, want 1", taskAfter.DownloadedSegments)
	}
	if taskAfter.Status != "stopping" {
		t.Fatalf("status = %q, want stopping", taskAfter.Status)
	}
}

func TestCreateTaskItemsPlaceholdersResetsFailedItemsForRetry(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-retry",
		Name:               "retry-task",
		OriginalURL:        "https://example.com/retry.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	items := []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
	}
	if err := m.CreateTaskItemsPlaceholders(meta.ID, items); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders first: %v", err)
	}

	pending, err := m.ListPendingTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("ListPendingTaskItems first: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending first) = %d, want 1", len(pending))
	}

	marked, err := m.MarkTaskItemSubmitting(meta.ID, pending[0].Filename)
	if err != nil {
		t.Fatalf("MarkTaskItemSubmitting first: %v", err)
	}
	if !marked {
		t.Fatal("expected pending item to become submitting before failure")
	}

	if err := m.MarkTaskItemSubmitFailed(meta.ID, pending[0].Filename, "aria2 timeout"); err != nil {
		t.Fatalf("MarkTaskItemSubmitFailed: %v", err)
	}

	if err := m.CreateTaskItemsPlaceholders(meta.ID, items); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders second: %v", err)
	}

	var status string
	var gid sql.NullString
	var retryCount int
	var lastErr string
	err = m.db.QueryRow(`
SELECT status, aria2_gid, retry_count, last_error
FROM task_item
WHERE task_id = ? AND filename = ?
`, meta.ID, pending[0].Filename).Scan(&status, &gid, &retryCount, &lastErr)
	if err != nil {
		t.Fatalf("query retried item: %v", err)
	}
	if status != taskItemStatusPending {
		t.Fatalf("retry status = %q, want %q", status, taskItemStatusPending)
	}
	if gid.Valid {
		t.Fatalf("retry gid valid = %v, want false", gid.Valid)
	}
	if retryCount != 1 {
		t.Fatalf("retryCount = %d, want 1", retryCount)
	}
	if lastErr != "" {
		t.Fatalf("lastErr = %q, want empty", lastErr)
	}

	reclaimed, err := m.ListPendingTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("ListPendingTaskItems second: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("len(reclaimed) = %d, want 1", len(reclaimed))
	}
}

func TestResetFailedTaskItemsToPending(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-reset-failed",
		Name:               "reset-failed",
		OriginalURL:        "https://example.com/reset.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItemsPlaceholders(meta.ID, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
	}); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders: %v", err)
	}

	marked, err := m.MarkTaskItemSubmitting(meta.ID, "00001.ts")
	if err != nil {
		t.Fatalf("MarkTaskItemSubmitting: %v", err)
	}
	if !marked {
		t.Fatal("expected task item to enter submitting")
	}
	if err := m.MarkTaskItemSubmitFailed(meta.ID, "00001.ts", "temporary error"); err != nil {
		t.Fatalf("MarkTaskItemSubmitFailed: %v", err)
	}

	reset, err := m.ResetFailedTaskItemsToPending(meta.ID)
	if err != nil {
		t.Fatalf("ResetFailedTaskItemsToPending: %v", err)
	}
	if reset != 1 {
		t.Fatalf("reset = %d, want 1", reset)
	}

	pending, err := m.ListPendingTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("ListPendingTaskItems: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending) = %d, want 1", len(pending))
	}
}

func TestResetQueuedTaskItemsToPending(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-reset-queued",
		Name:               "reset-queued",
		OriginalURL:        "https://example.com/reset-queued.m3u8",
		TotalSegments:      2,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "stopped",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItemsPlaceholders(meta.ID, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
		{Filename: "00002.ts", URL: "https://example.com/2.ts", Type: "ts"},
	}); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders: %v", err)
	}

	marked, err := m.MarkTaskItemSubmitting(meta.ID, "00001.ts")
	if err != nil {
		t.Fatalf("MarkTaskItemSubmitting first: %v", err)
	}
	if !marked {
		t.Fatal("expected first task item to enter submitting")
	}
	if err := m.BindTaskItemToAria2(meta.ID, "00001.ts", "gid-1"); err != nil {
		t.Fatalf("BindTaskItemToAria2: %v", err)
	}

	marked, err = m.MarkTaskItemSubmitting(meta.ID, "00002.ts")
	if err != nil {
		t.Fatalf("MarkTaskItemSubmitting second: %v", err)
	}
	if !marked {
		t.Fatal("expected second task item to enter submitting")
	}

	reset, err := m.ResetQueuedTaskItemsToPending(meta.ID)
	if err != nil {
		t.Fatalf("ResetQueuedTaskItemsToPending: %v", err)
	}
	if reset != 2 {
		t.Fatalf("reset = %d, want 2", reset)
	}

	items, err := m.GetTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("GetTaskItems: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}
	for _, item := range items {
		if item.Status != taskItemStatusPending {
			t.Fatalf("item %s status = %q, want %q", item.Filename, item.Status, taskItemStatusPending)
		}
		if item.Aria2GID != "" {
			t.Fatalf("item %s gid = %q, want empty", item.Filename, item.Aria2GID)
		}
	}
}

func TestStopTaskResetsQueuedItemsToPending(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-stop-reset",
		Name:               "stop-reset",
		OriginalURL:        "https://example.com/stop-reset.m3u8",
		TotalSegments:      2,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItemsPlaceholders(meta.ID, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
		{Filename: "00002.ts", URL: "https://example.com/2.ts", Type: "ts"},
	}); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders: %v", err)
	}

	marked, err := m.MarkTaskItemSubmitting(meta.ID, "00001.ts")
	if err != nil {
		t.Fatalf("MarkTaskItemSubmitting first: %v", err)
	}
	if !marked {
		t.Fatal("expected first task item to enter submitting")
	}
	if err := m.BindTaskItemToAria2(meta.ID, "00001.ts", "gid-1"); err != nil {
		t.Fatalf("BindTaskItemToAria2: %v", err)
	}

	marked, err = m.MarkTaskItemSubmitting(meta.ID, "00002.ts")
	if err != nil {
		t.Fatalf("MarkTaskItemSubmitting second: %v", err)
	}
	if !marked {
		t.Fatal("expected second task item to enter submitting")
	}

	if err := m.StopTask(meta.ID); err != nil {
		t.Fatalf("StopTask: %v", err)
	}

	stoppedMeta, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if stoppedMeta.Status != "stopped" {
		t.Fatalf("task status = %q, want stopped", stoppedMeta.Status)
	}

	items, err := m.GetTaskItems(meta.ID)
	if err != nil {
		t.Fatalf("GetTaskItems: %v", err)
	}
	for _, item := range items {
		if item.Status != taskItemStatusPending {
			t.Fatalf("item %s status = %q, want %q", item.Filename, item.Status, taskItemStatusPending)
		}
		if item.Aria2GID != "" {
			t.Fatalf("item %s gid = %q, want empty", item.Filename, item.Aria2GID)
		}
	}
}

func TestGetTaskItemGIDsSkipsEmptyValues(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-gids",
		Name:               "gid-task",
		OriginalURL:        "https://example.com/gid.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	if err := m.CreateTaskItemsPlaceholders(meta.ID, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
	}); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders: %v", err)
	}
	if err := m.CreateTaskItem(meta.ID, "00002.ts", "gid-2", "https://example.com/2.ts", "ts"); err != nil {
		t.Fatalf("CreateTaskItem: %v", err)
	}

	gids, err := m.GetTaskItemGIDs(meta.ID)
	if err != nil {
		t.Fatalf("GetTaskItemGIDs: %v", err)
	}
	if len(gids) != 1 || gids[0] != "gid-2" {
		t.Fatalf("gids = %#v, want [gid-2]", gids)
	}
}

func TestMarkTaskItemCompletedByFilenameCountsProgressWithoutGID(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:                 "task-filename-complete",
		Name:               "filename-complete",
		OriginalURL:        "https://example.com/file.m3u8",
		TotalSegments:      1,
		DownloadedSegments: 0,
		CreatedTime:        time.Now(),
		Status:             "downloading",
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := m.CreateTaskItemsPlaceholders(meta.ID, []playlist.DownloadItem{
		{Filename: "00001.ts", URL: "https://example.com/1.ts", Type: "ts"},
	}); err != nil {
		t.Fatalf("CreateTaskItemsPlaceholders: %v", err)
	}

	updated, err := m.MarkTaskItemCompletedByFilename(meta.ID, "00001.ts")
	if err != nil {
		t.Fatalf("MarkTaskItemCompletedByFilename: %v", err)
	}
	if !updated {
		t.Fatal("expected filename completion to update progress")
	}

	taskAfter, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if taskAfter.DownloadedSegments != 1 {
		t.Fatalf("downloaded = %d, want 1", taskAfter.DownloadedSegments)
	}
	if taskAfter.Status != "completed" {
		t.Fatalf("status = %q, want completed", taskAfter.Status)
	}
}
