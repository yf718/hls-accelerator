package task

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/downloader"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	mu                sync.Mutex
	aria2             *downloader.Aria2Client
	db                *sql.DB
	deleteSem         chan struct{}
	progressNotifyCh  chan string
	progressDeduperMu sync.Mutex
	progressInflight  map[string]struct{}
	progressRecent    map[string]time.Time
}

type AddTaskRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

func NewManager(aria2 *downloader.Aria2Client, db *sql.DB) (*Manager, error) {
	m := &Manager{
		aria2:            aria2,
		db:               db,
		deleteSem:        make(chan struct{}, 1),
		progressNotifyCh: make(chan string, 2048),
		progressInflight: make(map[string]struct{}),
		progressRecent:   make(map[string]time.Time),
	}
	if err := m.InitTable(); err != nil {
		return nil, err
	}
	if err := m.RecoverSubmittingTaskItems(); err != nil {
		return nil, err
	}
	m.startProgressTracking()
	return m, nil
}

// GetTasks returns list of tasks using DB-backed progress snapshots.
func (m *Manager) GetTasks() ([]map[string]interface{}, error) {
	dbTasks, err := m.ListTasksDB()
	if err != nil {
		return nil, err
	}

	tasks := []map[string]interface{}{}

	for _, meta := range dbTasks {
		downloaded := meta.DownloadedSegments
		if meta.Status == "completed" && downloaded < meta.TotalSegments {
			downloaded = meta.TotalSegments
			go m.UpdateTaskDownloadedSegments(meta.ID, downloaded)
		}

		tasks = append(tasks, map[string]interface{}{
			"id":                  meta.ID,
			"name":                meta.Name,
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
	status, _, err := m.MarkTaskStopping(id)
	if err != nil {
		return err
	}
	if status == "stopped" {
		return nil
	}

	// Stop only downloads that are still active/waiting in aria2. Historical
	// completed GIDs do not consume network resources and make stop needlessly slow.
	if m.aria2 != nil {
		dir := cache.GetTaskDir(id)
		if _, err := m.aria2.ForceRemoveTaskDownloads(dir); err != nil {
			// Fall back to the previous DB-driven GID sweep if queue introspection fails.
			gids, gidErr := m.GetTaskItemGIDs(id)
			if gidErr != nil {
				return fmt.Errorf("failed to inspect aria2 queue: %v (and failed to get task gids: %v)", err, gidErr)
			}
			m.aria2.ForceRemoveMany(gids)
		}
	}

	if _, err := m.ResetQueuedTaskItemsToPending(id); err != nil {
		return fmt.Errorf("failed to reset queued task items: %w", err)
	}

	return m.UpdateTaskStatus(id, "stopped")
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
	aria2Mode := "disabled"
	aria2Touched := 0
	if m.aria2 != nil {
		dir := cache.GetTaskDir(id)
		removed, err := m.aria2.ForceRemoveTaskDownloads(dir)
		if err != nil {
			aria2Mode = "gid_fallback"
			if len(gids) > 0 {
				m.aria2.CleanupTaskDownloads(gids)
				aria2Touched = len(gids)
			}
		} else {
			aria2Mode = "queue_only+purge"
			aria2Touched = removed
			if err := m.aria2.PurgeDownloadResult(); err != nil && len(gids) > 0 {
				aria2Mode = "queue_only+gid_result_fallback"
				m.aria2.CleanupTaskDownloads(gids)
				aria2Touched = len(gids)
			}
		}
	}
	fmt.Printf("Delete task %s: aria2 cleanup finished in %s (%s, %d gids)\n", id, time.Since(rpcStart), aria2Mode, aria2Touched)

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

func (m *Manager) HandleAdd(w http.ResponseWriter, r *http.Request, triggerFunc func(AddTaskRequest) error) {
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "Content-Type must be application/json", 400)
		return
	}

	var body AddTaskRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "Request body must contain a single JSON object", 400)
		return
	}

	body.Name = strings.TrimSpace(body.Name)
	body.URL = strings.TrimSpace(body.URL)
	if body.URL == "" {
		http.Error(w, "url is required", 400)
		return
	}
	if body.Name == "" {
		http.Error(w, "name is required", 400)
		return
	}

	// Validate URL
	parsedURL, err := url.Parse(body.URL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
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
		if err := triggerFunc(body); err != nil {
			fmt.Printf("Error starting task %s: %v\n", body.URL, err)
		}
	}()

	w.WriteHeader(200)
}

func DeriveTaskName(rawURL string) string {
	parsedURL, err := url.Parse(rawURL)
	if err == nil {
		base := strings.TrimSpace(path.Base(parsedURL.Path))
		if base != "" && base != "." && base != "/" {
			return base
		}
		if host := strings.TrimSpace(parsedURL.Host); host != "" {
			return host
		}
	}

	return "Untitled Task"
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

func (m *Manager) startProgressTracking() {
	go m.progressNotificationWorker()
	go m.progressDeduperCleanupLoop()
	go m.progressNotificationLoop()
}

func (m *Manager) progressNotificationLoop() {
	if m.aria2 == nil {
		return
	}

	for {
		err := m.aria2.ListenNotifications(context.Background(), func(method, gid string) {
			if method != "aria2.onDownloadComplete" || gid == "" {
				return
			}
			if !m.enqueueProgressNotification(gid) {
				log.Printf("progress notify dropped or deduped for gid=%s", gid)
			}
		})
		if err != nil {
			log.Printf("aria2 notification loop disconnected: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (m *Manager) progressNotificationWorker() {
	for gid := range m.progressNotifyCh {
		taskID, updated, err := m.MarkTaskItemCompletedByGID(gid)
		m.finishProgressNotification(gid)
		if err != nil {
			log.Printf("progress notify failed for gid=%s: %v", gid, err)
			continue
		}
		if updated {
			log.Printf("Progress updated from aria2 notification: task=%s gid=%s", taskID, gid)
		}
	}
}

func (m *Manager) enqueueProgressNotification(gid string) bool {
	if gid == "" {
		return false
	}

	now := time.Now()

	m.progressDeduperMu.Lock()
	if _, exists := m.progressInflight[gid]; exists {
		m.progressDeduperMu.Unlock()
		return false
	}
	if until, exists := m.progressRecent[gid]; exists {
		if now.Before(until) {
			m.progressDeduperMu.Unlock()
			return false
		}
		delete(m.progressRecent, gid)
	}
	m.progressInflight[gid] = struct{}{}
	m.progressDeduperMu.Unlock()

	select {
	case m.progressNotifyCh <- gid:
		return true
	default:
		m.finishProgressNotification(gid)
		return false
	}
}

func (m *Manager) finishProgressNotification(gid string) {
	if gid == "" {
		return
	}

	m.progressDeduperMu.Lock()
	delete(m.progressInflight, gid)
	m.progressRecent[gid] = time.Now().Add(2 * time.Minute)
	m.progressDeduperMu.Unlock()
}

func (m *Manager) progressDeduperCleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		m.progressDeduperMu.Lock()
		for gid, until := range m.progressRecent {
			if !now.Before(until) {
				delete(m.progressRecent, gid)
			}
		}
		m.progressDeduperMu.Unlock()
	}
}

func (m *Manager) SyncTaskProgress() (int, error) {
	tasks, err := m.GetTasksByStatuses("downloading", "stopped")
	if err != nil {
		return 0, err
	}

	updatedCount := 0
	for _, meta := range tasks {
		n, err := m.syncTaskItemsFromFileSet(meta.ID)
		if err != nil {
			return updatedCount, err
		}
		updatedCount += n
	}

	return updatedCount, nil
}

func (m *Manager) SyncTaskProgressForTask(taskID string) (int, error) {
	if _, err := m.GetTask(taskID); err != nil {
		return 0, err
	}
	return m.syncTaskItemsFromFileSet(taskID)
}

// syncTaskItemsFromFileSet checks the filesystem for task items that are
// already downloaded but not yet marked completed in the database.
func (m *Manager) syncTaskItemsFromFileSet(taskID string) (int, error) {
	items, err := m.GetIncompleteTaskItems(taskID)
	if err != nil {
		return 0, fmt.Errorf("failed to list items for task=%s: %w", taskID, err)
	}
	fileSet, err := m.taskFileSet(taskID)
	if err != nil {
		return 0, fmt.Errorf("failed to inspect files for task=%s: %w", taskID, err)
	}

	updatedCount := 0
	for _, item := range items {
		if _, ok := fileSet[item.Filename]; !ok {
			continue
		}
		if _, ok := fileSet[item.Filename+".aria2"]; ok {
			continue
		}
		if item.Aria2GID == "" {
			updated, err := m.MarkTaskItemCompletedByFilename(taskID, item.Filename)
			if err != nil {
				return updatedCount, fmt.Errorf("failed to sync task=%s file=%s: %w", taskID, item.Filename, err)
			}
			if updated {
				updatedCount++
			}
			continue
		}
		_, updated, err := m.MarkTaskItemCompletedByGID(item.Aria2GID)
		if err != nil {
			return updatedCount, fmt.Errorf("failed to sync task=%s gid=%s: %w", taskID, item.Aria2GID, err)
		}
		if updated {
			updatedCount++
		}
	}

	return updatedCount, nil
}

func (m *Manager) taskFileSet(taskID string) (map[string]struct{}, error) {
	entries, err := os.ReadDir(cache.GetTaskDir(taskID))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]struct{}{}, nil
		}
		return nil, err
	}

	fileSet := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileSet[entry.Name()] = struct{}{}
	}
	return fileSet, nil
}

func (m *Manager) HandleSyncProgress(w http.ResponseWriter, r *http.Request) {
	updated, err := m.SyncTaskProgress()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"updated": updated,
	})
}
