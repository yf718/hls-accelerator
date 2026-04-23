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
	t.Cleanup(func() { _ = db.Close() })

	m := &Manager{db: db}
	if err := m.InitTable(); err != nil {
		t.Fatalf("InitTable: %v", err)
	}
	return m
}

func TestTryCreateTaskUsesUniqueConstraintAsArbitration(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:           "task-unique",
		Name:         "unique-task",
		OriginalURL:  "https://example.com/unique.m3u8",
		CreatedTime:  time.Now(),
		UpdatedTime:  time.Now(),
		TotalItems:   3,
		TotalSegments: 2,
		Status:       TaskStatusDownloading,
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

func TestUpdateTaskSnapshotMarksCompletion(t *testing.T) {
	m := newTestManager(t)
	meta := TaskMetadata{
		ID:            "task-complete",
		Name:          "complete-task",
		OriginalURL:   "https://example.com/complete.m3u8",
		CreatedTime:   time.Now(),
		UpdatedTime:   time.Now(),
		TotalItems:    3,
		TotalSegments: 2,
		Status:        TaskStatusDownloading,
	}
	if err := m.CreateTask(meta); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	if err := m.UpdateTaskSnapshot(meta.ID, TaskStatusCompleted, 3, 2, 0); err != nil {
		t.Fatalf("UpdateTaskSnapshot: %v", err)
	}

	got, err := m.GetTask(meta.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != TaskStatusCompleted {
		t.Fatalf("status = %q, want %q", got.Status, TaskStatusCompleted)
	}
	if got.DoneItems != 3 {
		t.Fatalf("done_items = %d, want 3", got.DoneItems)
	}
	if got.DownloadedSegments != 2 {
		t.Fatalf("downloaded_segments = %d, want 2", got.DownloadedSegments)
	}
	if got.FinishedTime == nil {
		t.Fatal("finished_time should be set for completed task")
	}
}
