package task

import (
	"database/sql"
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
