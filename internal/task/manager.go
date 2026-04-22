package task

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"strconv"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	mu                sync.Mutex
	aria2             *downloader.Aria2Client
	db                *sql.DB
	deleteSem         chan struct{}
	progressNotifyCh  chan aria2NotificationEvent
	progressDeduperMu sync.Mutex
	progressInflight  map[string]struct{}
	progressRecent    map[string]time.Time
}

type aria2NotificationEvent struct {
	Method string
	GID    string
}

type AddTaskRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type RetryTaskRequest struct {
	ItemIDs []int64 `json:"item_ids"`
}

type TaskSummary struct {
	ID                 string     `json:"id"`
	Name               string     `json:"name"`
	OriginalURL        string     `json:"original_url"`
	Status             string     `json:"status"`
	TotalSegments      int        `json:"total_segments"`
	DownloadedSegments int        `json:"downloaded_segments"`
	TotalItems         int        `json:"total_items"`
	DoneItems          int        `json:"done_items"`
	FailedItems        int        `json:"failed_items"`
	CreatedTime        time.Time  `json:"created_time"`
	UpdatedTime        time.Time  `json:"updated_time"`
	FinishedTime       *time.Time `json:"finished_time,omitempty"`
	OutputDir          string     `json:"output_dir"`
	Progress           float64    `json:"progress"`
}

type TaskDetail struct {
	TaskSummary
	Extra string `json:"extra"`
}

func NewManager(aria2 *downloader.Aria2Client, db *sql.DB) (*Manager, error) {
	m := &Manager{
		aria2:            aria2,
		db:               db,
		deleteSem:        make(chan struct{}, 1),
		progressNotifyCh: make(chan aria2NotificationEvent, 2048),
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

func (m *Manager) GetTasks() ([]TaskSummary, error) {
	dbTasks, err := m.ListTasksDB()
	if err != nil {
		return nil, err
	}
	out := make([]TaskSummary, 0, len(dbTasks))
	for _, meta := range dbTasks {
		out = append(out, summarizeTask(meta))
	}
	return out, nil
}

func summarizeTask(meta TaskMetadata) TaskSummary {
	totalForProgress := meta.TotalSegments
	doneForProgress := meta.DownloadedSegments
	if totalForProgress <= 0 {
		totalForProgress = meta.TotalItems
		doneForProgress = meta.DoneItems
	}
	progress := 0.0
	if totalForProgress > 0 {
		progress = float64(doneForProgress) / float64(totalForProgress)
		if progress > 1 {
			progress = 1
		}
	}
	return TaskSummary{
		ID:                 meta.ID,
		Name:               meta.Name,
		OriginalURL:        meta.OriginalURL,
		Status:             meta.Status,
		TotalSegments:      meta.TotalSegments,
		DownloadedSegments: meta.DownloadedSegments,
		TotalItems:         meta.TotalItems,
		DoneItems:          meta.DoneItems,
		FailedItems:        meta.FailedItems,
		CreatedTime:        meta.CreatedTime,
		UpdatedTime:        meta.UpdatedTime,
		FinishedTime:       meta.FinishedTime,
		OutputDir:          meta.OutputDir,
		Progress:           progress,
	}
}

func (m *Manager) GetTaskDetail(taskID string) (*TaskDetail, error) {
	meta, err := m.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	detail := &TaskDetail{
		TaskSummary: summarizeTask(*meta),
		Extra:       meta.Extra,
	}
	return detail, nil
}

func (m *Manager) PauseTask(id string) (int, error) {
	items, err := m.ListTaskItemsByTask(id, "")
	if err != nil {
		return 0, err
	}
	var gids []string
	for _, item := range items {
		switch item.Status {
		case taskItemStatusQueued, taskItemStatusDownloading:
			if item.Aria2GID != "" {
				gids = append(gids, item.Aria2GID)
			}
		}
	}
	if m.aria2 != nil && len(gids) > 0 {
		_ = m.aria2.BatchPause(gids)
	}
	res, err := m.db.Exec(`
	UPDATE task_item
	SET status = ?, updated_time = datetime('now')
	WHERE task_id = ? AND status IN (?, ?, ?, ?)
	`, taskItemStatusPaused, id, taskItemStatusPending, taskItemStatusQueued, taskItemStatusDownloading, taskItemStatusSubmitting)
	if err != nil {
		return 0, err
	}
	if err := m.AggregateTask(id); err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

func (m *Manager) StopTask(id string) error {
	_, err := m.PauseTask(id)
	return err
}

func (m *Manager) ResumeTask(id string) (int, error) {
	items, err := m.ListTaskItemsByTask(id, taskItemStatusPaused)
	if err != nil {
		return 0, err
	}
	var gids []string
	for _, item := range items {
		if item.Aria2GID != "" {
			gids = append(gids, item.Aria2GID)
		}
	}
	if m.aria2 != nil && len(gids) > 0 {
		_ = m.aria2.BatchUnpause(gids)
	}
	if _, err := m.db.Exec(`
	UPDATE task_item
	SET status = CASE WHEN aria2_gid IS NOT NULL AND aria2_gid != '' THEN ? ELSE ? END,
		updated_time = datetime('now')
	WHERE task_id = ? AND status = ?
	`, taskItemStatusQueued, taskItemStatusPending, id, taskItemStatusPaused); err != nil {
		return 0, err
	}
	if err := m.AggregateTask(id); err != nil {
		return 0, err
	}
	return len(items), nil
}

func (m *Manager) RetryTask(id string, itemIDs []int64) (int, error) {
	if len(itemIDs) == 0 {
		return m.ResetFailedTaskItemsToPending(id)
	}
	placeholders := make([]string, len(itemIDs))
	args := make([]interface{}, 0, len(itemIDs)+2)
	args = append(args, id)
	for i, itemID := range itemIDs {
		placeholders[i] = "?"
		args = append(args, itemID)
	}
	args = append(args, taskItemStatusFailed)
	query := fmt.Sprintf(`
	UPDATE task_item
	SET status = ?, aria2_gid = NULL, last_error = '', updated_time = datetime('now')
	WHERE task_id = ? AND id IN (%s) AND status = ?
	`, strings.Join(placeholders, ","))
	execArgs := make([]interface{}, 0, len(args)+1)
	execArgs = append(execArgs, taskItemStatusPending)
	execArgs = append(execArgs, args...)
	res, err := m.db.Exec(query, execArgs...)
	if err != nil {
		return 0, err
	}
	if err := m.AggregateTask(id); err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

func (m *Manager) DeleteTask(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	meta, err := m.GetTask(id)
	if err != nil {
		return err
	}
	if meta.Status == TaskStatusDownloading || meta.Status == TaskStatusParsing {
		return fmt.Errorf("cannot delete running task, please pause it first")
	}
	if meta.Status == TaskStatusDeleted {
		return nil
	}
	if err := m.UpdateTaskStatus(id, TaskStatusDeleted); err != nil {
		return err
	}
	go m.deleteTaskAsync(*meta)
	return nil
}

func (m *Manager) deleteTaskAsync(meta TaskMetadata) {
	m.acquireDeleteSlot()
	defer m.releaseDeleteSlot()

	start := time.Now()
	if err := m.deleteTaskResources(meta.ID); err != nil {
		log.Printf("Delete task %s failed after %s: %v", meta.ID, time.Since(start), err)
		return
	}
	log.Printf("Delete task %s completed in %s", meta.ID, time.Since(start))
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

	if m.aria2 != nil {
		dir := cache.GetTaskDir(id)
		if _, err := m.aria2.ForceRemoveTaskDownloads(dir); err != nil && len(gids) > 0 {
			m.aria2.CleanupTaskDownloads(gids)
		} else {
			_ = m.aria2.PurgeDownloadResult()
		}
	}

	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove files at %s: %v", path, err)
		}
	}
	if err := m.DeleteTaskItems(id); err != nil {
		return err
	}
	return m.DeleteTaskDB(id)
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
	var paths []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
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
	for _, current := range paths {
		if current == "" {
			continue
		}
		if _, ok := seen[current]; ok {
			continue
		}
		seen[current] = struct{}{}
		out = append(out, current)
	}
	return out
}

func (m *Manager) HandleLegacyList(w http.ResponseWriter, r *http.Request) {
	tasks, err := m.GetTasks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func (m *Manager) HandleListV1(w http.ResponseWriter, r *http.Request) {
	tasks, err := m.GetTasks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" {
		filtered := tasks[:0]
		for _, item := range tasks {
			if item.Status == status {
				filtered = append(filtered, item)
			}
		}
		tasks = filtered
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total": len(tasks),
		"items": tasks,
	})
}

func (m *Manager) HandleGetTaskV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	detail, err := m.GetTaskDetail(taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "task not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(detail)
}

func (m *Manager) HandleListTaskItemsV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	items, err := m.ListTaskItemsByTask(taskID, status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total": len(items),
		"items": items,
	})
}

func (m *Manager) HandleGetTaskItemV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	itemID, err := strconv.ParseInt(r.PathValue("itemId"), 10, 64)
	if err != nil {
		http.Error(w, "invalid item id", http.StatusBadRequest)
		return
	}
	item, err := m.GetTaskItem(taskID, itemID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "task item not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}

func (m *Manager) HandlePauseV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	paused, err := m.PauseTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"paused_items": paused})
}

func (m *Manager) HandleResumeV1(w http.ResponseWriter, r *http.Request, onResume func(string)) {
	taskID := r.PathValue("id")
	resumed, err := m.ResumeTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if onResume != nil {
		onResume(taskID)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"resumed_items": resumed})
}

func (m *Manager) HandleRetryV1(w http.ResponseWriter, r *http.Request, onResume func(string)) {
	taskID := r.PathValue("id")
	var body RetryTaskRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	retried, err := m.RetryTask(taskID, body.ItemIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if onResume != nil && retried > 0 {
		onResume(taskID)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"retried_items": retried})
}

func (m *Manager) HandleDeleteV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if err := m.DeleteTask(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Manager) HandleAdd(w http.ResponseWriter, r *http.Request, triggerFunc func(AddTaskRequest) error) {
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "Content-Type must be application/json", http.StatusBadRequest)
		return
	}
	var body AddTaskRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "Request body must contain a single JSON object", http.StatusBadRequest)
		return
	}

	body.Name = strings.TrimSpace(body.Name)
	body.URL = strings.TrimSpace(body.URL)
	if body.URL == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}
	if body.Name == "" {
		body.Name = DeriveTaskName(body.URL)
	}

	parsedURL, err := url.Parse(body.URL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	taskID := cache.GetTaskID(body.URL)
	exists, status, err := m.CheckTaskExists(taskID)
	if err != nil {
		http.Error(w, "DB Error", http.StatusInternalServerError)
		return
	}
	if exists && status != TaskStatusDeleted {
		http.Error(w, fmt.Sprintf("Task already exists with status: %s", status), http.StatusConflict)
		return
	}

	go func() {
		if err := triggerFunc(body); err != nil {
			log.Printf("Error starting task %s: %v", body.URL, err)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":     taskID,
		"name":   body.Name,
		"url":    body.URL,
		"status": TaskStatusPending,
	})
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

func (m *Manager) DeleteCompletedTasks() error {
	completedTasks, err := m.GetTasksByStatus(TaskStatusCompleted)
	if err != nil {
		return err
	}
	var errs []string
	for _, current := range completedTasks {
		if err := m.DeleteTask(current.ID); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (m *Manager) startProgressTracking() {
	go m.progressNotificationWorker()
	go m.progressDeduperCleanupLoop()
	go m.progressNotificationLoop()
	go m.reconcileLoop()
}

func (m *Manager) progressNotificationLoop() {
	if m.aria2 == nil {
		return
	}
	for {
		err := m.aria2.ListenNotifications(context.Background(), func(method, gid string) {
			if gid == "" || method == "" {
				return
			}
			if !m.enqueueProgressNotification(method, gid) {
				return
			}
		})
		if err != nil {
			log.Printf("aria2 notification loop disconnected: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (m *Manager) progressNotificationWorker() {
	for event := range m.progressNotifyCh {
		switch event.Method {
		case "aria2.onDownloadStart":
			_, _, _ = m.UpdateTaskItemStateByGID(event.GID, taskItemStatusDownloading, "", "", 0)
		case "aria2.onDownloadPause":
			_, _, _ = m.UpdateTaskItemStateByGID(event.GID, taskItemStatusPaused, "", "", 0)
		case "aria2.onDownloadStop":
			_, _, _ = m.UpdateTaskItemStateByGID(event.GID, taskItemStatusPaused, "", "", 0)
		case "aria2.onDownloadError":
			_, _, _ = m.UpdateTaskItemStateByGID(event.GID, taskItemStatusFailed, "aria2 download error", "", 0)
		case "aria2.onDownloadComplete":
			_, _, _ = m.MarkTaskItemCompletedByGID(event.GID)
		}
		m.finishProgressNotification(event.GID)
	}
}

func (m *Manager) enqueueProgressNotification(method, gid string) bool {
	now := time.Now()
	m.progressDeduperMu.Lock()
	if _, exists := m.progressInflight[gid]; exists {
		m.progressDeduperMu.Unlock()
		return false
	}
	if until, exists := m.progressRecent[gid]; exists && now.Before(until) && method == "aria2.onDownloadComplete" {
		m.progressDeduperMu.Unlock()
		return false
	}
	delete(m.progressRecent, gid)
	m.progressInflight[gid] = struct{}{}
	m.progressDeduperMu.Unlock()

	select {
	case m.progressNotifyCh <- aria2NotificationEvent{Method: method, GID: gid}:
		return true
	default:
		m.finishProgressNotification(gid)
		return false
	}
}

func (m *Manager) finishProgressNotification(gid string) {
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

func (m *Manager) reconcileLoop() {
	if m.aria2 == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if updated, err := m.ReconcileActiveTaskItems(); err != nil {
			log.Printf("reconcile task items failed: %v", err)
		} else if updated > 0 {
			log.Printf("reconcile updated %d task items", updated)
		}
	}
}

func (m *Manager) ReconcileActiveTaskItems() (int, error) {
	items, err := m.ListActiveTaskItemsWithGID()
	if err != nil {
		return 0, err
	}
	if len(items) == 0 || m.aria2 == nil {
		return 0, nil
	}
	statuses, err := m.aria2.BatchTellStatus(extractGIDs(items))
	if err != nil {
		return 0, err
	}
	updated := 0
	for _, item := range items {
		status, ok := statuses[item.Aria2GID]
		if !ok {
			continue
		}
		target, errMsg := mapAria2Status(status.Status)
		filePath := status.FirstFilePath()
		if taskID, changed, err := m.UpdateTaskItemStateByGID(item.Aria2GID, target, firstNonEmpty(status.ErrorMessage, errMsg), filePath, status.CompletedLengthInt64()); err != nil {
			return updated, err
		} else if changed {
			updated++
			_ = taskID
		}
	}
	return updated, nil
}

func (m *Manager) ReconcileTaskItems(taskID string) (int, error) {
	items, err := m.ListTaskItemsByTask(taskID, "")
	if err != nil {
		return 0, err
	}
	if len(items) == 0 || m.aria2 == nil {
		return 0, nil
	}

	filtered := make([]TaskItem, 0, len(items))
	for _, item := range items {
		if item.Aria2GID == "" {
			continue
		}
		switch item.Status {
		case taskItemStatusQueued, taskItemStatusDownloading, taskItemStatusPaused, taskItemStatusSubmitting:
			filtered = append(filtered, item)
		}
	}
	if len(filtered) == 0 {
		return 0, nil
	}

	statuses, err := m.aria2.BatchTellStatus(extractGIDs(filtered))
	if err != nil {
		return 0, err
	}

	updated := 0
	for _, item := range filtered {
		status, ok := statuses[item.Aria2GID]
		if !ok {
			continue
		}
		target, errMsg := mapAria2Status(status.Status)
		filePath := status.FirstFilePath()
		if _, changed, err := m.UpdateTaskItemStateByGID(item.Aria2GID, target, firstNonEmpty(status.ErrorMessage, errMsg), filePath, status.CompletedLengthInt64()); err != nil {
			return updated, err
		} else if changed {
			updated++
		}
	}
	return updated, nil
}

func mapAria2Status(status string) (string, string) {
	switch status {
	case "active":
		return taskItemStatusDownloading, ""
	case "waiting":
		return taskItemStatusQueued, ""
	case "paused":
		return taskItemStatusPaused, ""
	case "complete":
		return taskItemStatusCompleted, ""
	case "error":
		return taskItemStatusFailed, "aria2 reported error"
	case "removed":
		return taskItemStatusRemoved, ""
	default:
		return taskItemStatusQueued, ""
	}
}

func extractGIDs(items []TaskItem) []string {
	gids := make([]string, 0, len(items))
	for _, item := range items {
		if item.Aria2GID != "" {
			gids = append(gids, item.Aria2GID)
		}
	}
	return gids
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (m *Manager) SyncTaskProgress() (int, error) {
	tasks, err := m.GetTasksByStatuses(TaskStatusDownloading, TaskStatusPaused, TaskStatusFailed)
	if err != nil {
		return 0, err
	}
	updated := 0
	for _, meta := range tasks {
		count, err := m.syncTaskItemsFromFileSet(meta.ID)
		if err != nil {
			return updated, err
		}
		updated += count
	}
	reconciled, err := m.ReconcileActiveTaskItems()
	if err != nil {
		return updated, err
	}
	return updated + reconciled, nil
}

func (m *Manager) SyncTaskProgressForTask(taskID string) (int, error) {
	if _, err := m.GetTask(taskID); err != nil {
		return 0, err
	}
	updated, err := m.syncTaskItemsFromFileSet(taskID)
	if err != nil {
		return 0, err
	}
	reconciled, err := m.ReconcileActiveTaskItems()
	if err != nil {
		return updated, err
	}
	return updated + reconciled, nil
}

func (m *Manager) syncTaskItemsFromFileSet(taskID string) (int, error) {
	items, err := m.GetIncompleteTaskItems(taskID)
	if err != nil {
		return 0, err
	}
	fileSet, err := m.taskFileSet(taskID)
	if err != nil {
		return 0, err
	}
	updated := 0
	for _, item := range items {
		if _, ok := fileSet[item.Filename]; !ok {
			continue
		}
		if _, ok := fileSet[item.Filename+".aria2"]; ok {
			continue
		}
		if item.Aria2GID == "" {
			changed, err := m.MarkTaskItemCompletedByFilename(taskID, item.Filename)
			if err != nil {
				return updated, err
			}
			if changed {
				updated++
			}
			continue
		}
		_, changed, err := m.MarkTaskItemCompletedByGID(item.Aria2GID)
		if err != nil {
			return updated, err
		}
		if changed {
			updated++
		}
	}
	return updated, nil
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
		if !entry.IsDir() {
			fileSet[entry.Name()] = struct{}{}
		}
	}
	return fileSet, nil
}

func (m *Manager) HandleSyncProgress(w http.ResponseWriter, r *http.Request) {
	updated, err := m.SyncTaskProgress()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"updated": updated})
}
