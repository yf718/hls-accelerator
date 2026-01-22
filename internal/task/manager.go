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
	"sync"
)

type Manager struct {
	mu    sync.Mutex
	aria2 *downloader.Aria2Client
	db    *sql.DB
}

func NewManager(aria2 *downloader.Aria2Client, db *sql.DB) (*Manager, error) {
	m := &Manager{
		aria2: aria2,
		db:    db,
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

	// 3. Stop all downloads by GID
	if m.aria2 != nil {
		for _, gid := range gids {
			// Try to remove from aria2, ignore errors (task might already be completed/removed)
			m.aria2.ForceRemove(gid)
		}
	}

	return nil
}

func (m *Manager) DeleteTask(id string) error {
	// Check status first
	meta, err := m.GetTask(id)
	if err != nil {
		return err // Task not found
	}

	if meta.Status == "downloading" {
		return fmt.Errorf("cannot delete running task, please stop it first")
	}

	// Get all GIDs before deleting from DB
	gids, err := m.GetTaskItemGIDs(id)
	if err == nil && m.aria2 != nil {
		// Remove all GIDs from aria2 (including completed ones)
		for _, gid := range gids {
			// Use ForceRemove to remove even completed downloads
			m.aria2.ForceRemove(gid)
			// Also try to remove from download results (for completed tasks)
			m.aria2.RemoveDownloadResult(gid)
		}
	}

	dir := cache.GetTaskDir(id)

	// Remove files
	// We try to remove. If it fails, we return error.
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove files: %v", err)
	}

	// Double check
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		return fmt.Errorf("directory still exists")
	}

	// Remove task items from DB (must be done before deleting task)
	if err := m.DeleteTaskItems(id); err != nil {
		return fmt.Errorf("failed to delete task items: %v", err)
	}

	// Remove task from DB
	if err := m.DeleteTaskDB(id); err != nil {
		return fmt.Errorf("failed to delete from db: %v", err)
	}

	return nil
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
