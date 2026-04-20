package task

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/downloader"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	mu        sync.Mutex
	aria2     *downloader.Aria2Client
	db        *sql.DB
	deleteSem chan struct{}
}

func NewManager(aria2 *downloader.Aria2Client, db *sql.DB) (*Manager, error) {
	m := &Manager{
		aria2:     aria2,
		db:        db,
		deleteSem: make(chan struct{}, 1),
	}
	if err := m.InitTable(); err != nil {
		return nil, err
	}
	return m, nil
}

// GetTasks returns list of tasks from DB with calculated progress
func (m *Manager) GetTasks() ([]map[string]interface{}, error) {
	dbTasks, err := m.ListTasksDB()
	if err != nil {
		return nil, err
	}

	tasks := []map[string]interface{}{}

	for _, meta := range dbTasks {
		var downloaded int

		// Optimization: If task is completed, we trust the DB and skip filesystem check
		if meta.Status == "completed" {
			downloaded = meta.TotalSegments
		} else if meta.Status == "deleting" || meta.Status == "delete_failed" {
			// Deleting tasks may already have their cache directory detached, so probing
			// the filesystem would be both slow and misleading.
			downloaded = 0
		} else {
			// Calculate progress by checking task_item records
			downloaded = m.countDownloadedSegments(meta.ID)

			// Check if completed (update DB if changed)
			if meta.TotalSegments > 0 && downloaded >= meta.TotalSegments {
				meta.Status = "completed"
				downloaded = meta.TotalSegments // Ensure consistency
				// Update DB
				go m.UpdateTaskStatus(meta.ID, "completed")
			}
		}

		tasks = append(tasks, map[string]interface{}{
			"id":                  meta.ID,
			"original_url":        meta.OriginalURL,
			"total_segments":      meta.TotalSegments,
			"downloaded_segments": downloaded,
			"created_time":        meta.CreatedTime,
			"status":              meta.Status,
		})
	}
	return tasks, nil
}

func (m *Manager) StopTask(id string) error {
	// 1. Mark as Stopped
	if err := m.UpdateTaskStatus(id, "stopped"); err != nil {
		return err
	}

	// 2. Get all GIDs for this task from database
	gids, err := m.GetTaskItemGIDs(id)
	if err != nil {
		return fmt.Errorf("failed to get task item GIDs: %v", err)
	}

	// 3. Stop all downloads by GID (batched RPC — many segments would otherwise stall here)
	if m.aria2 != nil {
		m.aria2.ForceRemoveMany(gids)
	}

	return nil
}

func (m *Manager) DeleteTask(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check status first
	meta, err := m.GetTask(id)
	if err != nil {
		return err // Task not found
	}

	switch meta.Status {
	case "downloading":
		return fmt.Errorf("cannot delete running task, please stop it first")
	case "deleting":
		return nil
	}

	if err := m.UpdateTaskStatus(id, "deleting"); err != nil {
		return fmt.Errorf("failed to mark task as deleting: %v", err)
	}

	go m.deleteTaskAsync(*meta)

	return nil
}

func (m *Manager) deleteTaskAsync(meta TaskMetadata) {
	m.acquireDeleteSlot()
	defer m.releaseDeleteSlot()

	start := time.Now()
	fmt.Printf("Delete task %s: started (previous_status=%s)\n", meta.ID, meta.Status)

	if err := m.deleteTaskResources(meta.ID); err != nil {
		fmt.Printf("Delete task %s: failed after %s: %v\n", meta.ID, time.Since(start), err)
		_ = m.UpdateTaskStatus(meta.ID, "delete_failed")
		return
	}

	fmt.Printf("Delete task %s: completed in %s\n", meta.ID, time.Since(start))
}

func (m *Manager) acquireDeleteSlot() {
	if m == nil || m.deleteSem == nil {
		return
	}
	m.deleteSem <- struct{}{}
}

func (m *Manager) releaseDeleteSlot() {
	if m == nil || m.deleteSem == nil {
		return
	}
	<-m.deleteSem
}

func (m *Manager) deleteTaskResources(id string) error {
	gids, err := m.GetTaskItemGIDs(id)
	if err != nil {
		return fmt.Errorf("failed to get task item GIDs: %v", err)
	}

	paths, err := m.prepareTaskDeletionPaths(id)
	if err != nil {
		return fmt.Errorf("failed to prepare delete paths: %v", err)
	}

	rpcStart := time.Now()
	if m.aria2 != nil && len(gids) > 0 {
		m.aria2.CleanupTaskDownloads(gids)
	}
	fmt.Printf("Delete task %s: aria2 cleanup finished in %s (%d gids)\n", id, time.Since(rpcStart), len(gids))

	fsStart := time.Now()
	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove files at %s: %v", path, err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return fmt.Errorf("directory still exists: %s", path)
		}
	}
	fmt.Printf("Delete task %s: file cleanup finished in %s (%d paths)\n", id, time.Since(fsStart), len(paths))

	dbStart := time.Now()
	if err := m.DeleteTaskItems(id); err != nil {
		return fmt.Errorf("failed to delete task items: %v", err)
	}
	if err := m.DeleteTaskDB(id); err != nil {
		return fmt.Errorf("failed to delete from db: %v", err)
	}
	fmt.Printf("Delete task %s: db cleanup finished in %s\n", id, time.Since(dbStart))

	return nil
}

func (m *Manager) prepareTaskDeletionPaths(id string) ([]string, error) {
	paths := make([]string, 0, 2)

	dir := cache.GetTaskDir(id)
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		detachedDir, detachErr := detachTaskDir(id)
		if detachErr != nil {
			return nil, detachErr
		}
		paths = append(paths, detachedDir)
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	stalePaths, err := findDetachedTaskDirs(id)
	if err != nil {
		return nil, err
	}
	paths = append(paths, stalePaths...)

	return uniquePaths(paths), nil
}

func detachTaskDir(taskID string) (string, error) {
	dir := cache.GetTaskDir(taskID)
	trashRoot := filepath.Join(filepath.Dir(dir), ".deleting")
	if err := os.MkdirAll(trashRoot, 0755); err != nil {
		return "", err
	}

	detachedDir := filepath.Join(trashRoot, fmt.Sprintf("%s-%d", taskID, time.Now().UnixNano()))
	if err := os.Rename(dir, detachedDir); err != nil {
		return "", err
	}
	return detachedDir, nil
}

func findDetachedTaskDirs(taskID string) ([]string, error) {
	dir := cache.GetTaskDir(taskID)
	trashRoot := filepath.Join(filepath.Dir(dir), ".deleting")
	entries, err := os.ReadDir(trashRoot)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	prefix := taskID + "-"
	paths := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), prefix) {
			paths = append(paths, filepath.Join(trashRoot, entry.Name()))
		}
	}
	return paths, nil
}

func uniquePaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}

	out := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	return out
}

// countDownloadedSegments counts completed downloads by checking task_item records
func (m *Manager) countDownloadedSegments(taskID string) int {
	// Get all task items from database
	items, err := m.GetTaskItems(taskID)
	if err != nil {
		// Fallback to 0 if query fails
		return 0
	}

	count := 0
	for _, item := range items {
		// Check if file exists and is complete (no .aria2 file)
		if cache.FileExists(taskID, item.Filename) {
			aria2File := item.Filename + ".aria2"
			if !cache.FileExists(taskID, aria2File) {
				count++
			}
		}
	}
	return count
}

// API Handlers
func (m *Manager) HandleList(w http.ResponseWriter, r *http.Request) {
	tasks, err := m.GetTasks()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(tasks)
}

func (m *Manager) HandleStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "Invalid ID", 400)
		return
	}
	if err := m.StopTask(id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(200)
}

func (m *Manager) HandleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "Invalid ID", 400)
		return
	}
	if err := m.DeleteTask(id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(200)
}

func (m *Manager) HandleAdd(w http.ResponseWriter, r *http.Request, triggerFunc func(string) error) {
	var body struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Validate URL
	if _, err := url.Parse(body.URL); err != nil {
		http.Error(w, "Invalid URL", 400)
		return
	}

	// Check if task exists
	taskID := cache.GetTaskID(body.URL)
	exists, status, err := m.CheckTaskExists(taskID)
	if err != nil {
		http.Error(w, "DB Error", 500)
		return
	}

	if exists {
		if status == "deleting" { // Legacy check just in case
			http.Error(w, "Task is currently being deleted, please try again later", 409)
			return
		}
		// If task exists in any active/stopped state, reject
		http.Error(w, fmt.Sprintf("Task already exists with status: %s", status), 409)
		return
	}

	// Trigger the download asynchronously
	go func() {
		if err := triggerFunc(body.URL); err != nil {
			fmt.Printf("Error starting task %s: %v\n", body.URL, err)
		}
	}()

	w.WriteHeader(200)
}

// DeleteCompletedTasks deletes all tasks with status "completed"
func (m *Manager) DeleteCompletedTasks() error {
	// Get all completed tasks
	completedTasks, err := m.GetTasksByStatus("completed")
	if err != nil {
		return fmt.Errorf("failed to get completed tasks: %v", err)
	}

	var errors []string
	for _, task := range completedTasks {
		if err := m.DeleteTask(task.ID); err != nil {
			errors = append(errors, fmt.Sprintf("failed to delete task %s: %v", task.ID, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors during cleanup: %v", errors)
	}

	return nil
}
