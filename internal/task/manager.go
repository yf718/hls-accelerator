package task

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/config"
	"hls-accelerator/internal/downloader"
	playlist "hls-accelerator/internal/m3u8"
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

const pausedRuntimeTTL = 10 * time.Minute

const (
	dirtyFlushTick            = 500 * time.Millisecond
	downloadingFlushInterval  = 2 * time.Second
	pausedFlushInterval       = 400 * time.Millisecond
	terminalFlushInterval     = 150 * time.Millisecond
)

type Manager struct {
	mu               sync.Mutex
	aria2            *downloader.Aria2Client
	db               *sql.DB
	deleteSem        chan struct{}
	progressNotifyCh chan aria2NotificationEvent

	runtimeMu sync.Mutex
	runtimes  map[string]*taskRuntime

	dispatchMu sync.Mutex
	dispatches map[string]context.CancelFunc

	metricsMu       sync.Mutex
	lastFlushCost   time.Duration
	totalFlushCost  time.Duration
	totalFlushCount int64
}

type aria2NotificationEvent struct {
	Method string
	GID    string
}

type taskRuntime struct {
	mu                sync.Mutex
	taskID            string
	totalItems        int
	totalSegments     int
	segmentBySeq      []bool
	remaining         map[string]uint32
	failed            map[string]struct{}
	dispatching       map[string]struct{}
	gidToFile         map[string]string
	fileToGID         map[string]string
	remainingSegments int
	paused            bool
	dirty             bool
	dirtySince        time.Time
	lastAccessAt      time.Time
}

func NewManager(aria2 *downloader.Aria2Client, db *sql.DB) (*Manager, error) {
	m := &Manager{
		aria2:            aria2,
		db:               db,
		deleteSem:        make(chan struct{}, 1),
		progressNotifyCh: make(chan aria2NotificationEvent, 4096),
		runtimes:         make(map[string]*taskRuntime),
		dispatches:       make(map[string]context.CancelFunc),
	}
	if err := m.InitTable(); err != nil {
		return nil, err
	}
	m.startBackgroundLoops()
	return m, nil
}

func (m *Manager) startBackgroundLoops() {
	go m.progressNotificationLoop()
	go m.progressNotificationWorker()
	go m.flushDirtyLoop()
	go m.reconcileLoop()
	go m.cleanupRuntimeLoop()
}

func (m *Manager) CreateTaskWithItems(meta TaskMetadata, items []playlist.DownloadItem) (bool, error) {
	if len(items) == 0 {
		return false, fmt.Errorf("no items to download")
	}
	manifest := buildManifest(meta.ID, meta.OriginalURL, items, meta.TotalSegments)
	progress := buildInitialProgress(manifest)
	meta.TotalItems = len(manifest.Items)
	meta.DoneItems = 0
	meta.DownloadedSegments = 0
	meta.FailedItems = 0
	meta.Status = TaskStatusDownloading

	created, err := m.TryCreateTask(meta)
	if err != nil || !created {
		return created, err
	}

	if err := m.SaveTaskManifest(manifest); err != nil {
		_ = m.DeleteTaskDB(meta.ID)
		return false, err
	}
	if err := writeJSONAtomic(taskProgressPath(meta.ID), progress); err != nil {
		_ = m.DeleteTaskDB(meta.ID)
		return false, err
	}
	if _, err := m.loadRuntime(meta.ID); err != nil {
		return false, err
	}
	m.StartDispatch(meta.ID)
	return true, nil
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

func (m *Manager) RuntimeMetrics() RuntimeMetrics {
	m.runtimeMu.Lock()
	runtimeCount := len(m.runtimes)
	dirtyCount := 0
	for _, rt := range m.runtimes {
		rt.mu.Lock()
		if rt.dirty {
			dirtyCount++
		}
		rt.mu.Unlock()
	}
	m.runtimeMu.Unlock()

	m.dispatchMu.Lock()
	dispatchCount := len(m.dispatches)
	m.dispatchMu.Unlock()

	m.metricsMu.Lock()
	lastFlush := m.lastFlushCost
	totalFlush := m.totalFlushCost
	totalCount := m.totalFlushCount
	m.metricsMu.Unlock()

	avg := int64(0)
	if totalCount > 0 {
		avg = totalFlush.Milliseconds() / totalCount
	}
	return RuntimeMetrics{
		RuntimeCount:       runtimeCount,
		DirtyRuntimeCount:  dirtyCount,
		ActiveDispatches:   dispatchCount,
		LastFlushCostMs:    lastFlush.Milliseconds(),
		AverageFlushCostMs: avg,
	}
}

func summarizeTask(meta TaskMetadata) TaskSummary {
	progress := 0.0
	total := meta.TotalSegments
	done := meta.DownloadedSegments
	if total <= 0 {
		total = meta.TotalItems
		done = meta.DoneItems
	}
	if total > 0 {
		progress = float64(done) / float64(total)
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

func (m *Manager) PauseTask(taskID string) (int, error) {
	meta, err := m.GetTask(taskID)
	if err != nil {
		return 0, err
	}
	switch meta.Status {
	case TaskStatusCompleted, TaskStatusDeleted:
		return 0, fmt.Errorf("task status %s does not support pause", meta.Status)
	}
	rt, err := m.loadRuntime(taskID)
	if err != nil {
		return 0, err
	}
	m.cancelDispatch(taskID)

	rt.mu.Lock()
	rt.paused = true
	gids := keysOfMap(rt.gidToFile)
	pendingCount := rt.pendingDispatchableCountLocked()
	rt.markDirtyLocked()
	rt.mu.Unlock()

	if m.aria2 != nil && len(gids) > 0 {
		_ = m.aria2.BatchPause(gids)
	}
	if err := m.flushRuntime(taskID, rt); err != nil {
		return 0, err
	}
	return pendingCount + len(gids), nil
}

func (m *Manager) ResumeTask(taskID string) (int, error) {
	meta, err := m.GetTask(taskID)
	if err != nil {
		return 0, err
	}
	switch meta.Status {
	case TaskStatusCompleted, TaskStatusDeleted:
		return 0, fmt.Errorf("task status %s does not support resume", meta.Status)
	}
	rt, err := m.loadRuntime(taskID)
	if err != nil {
		return 0, err
	}
	rt.mu.Lock()
	rt.paused = false
	gids := keysOfMap(rt.gidToFile)
	count := rt.pendingDispatchableCountLocked() + len(gids)
	rt.markDirtyLocked()
	rt.mu.Unlock()

	if m.aria2 != nil && len(gids) > 0 {
		_ = m.aria2.BatchUnpause(gids)
	}
	if err := m.flushRuntime(taskID, rt); err != nil {
		return 0, err
	}
	m.StartDispatch(taskID)
	return count, nil
}

func (m *Manager) RetryTask(taskID string) (int, error) {
	meta, err := m.GetTask(taskID)
	if err != nil {
		return 0, err
	}
	switch meta.Status {
	case TaskStatusCompleted, TaskStatusDeleted:
		return 0, fmt.Errorf("task status %s does not support retry", meta.Status)
	}
	rt, err := m.loadRuntime(taskID)
	if err != nil {
		return 0, err
	}
	rt.mu.Lock()
	count := len(rt.failed)
	rt.failed = make(map[string]struct{})
	rt.paused = false
	rt.markDirtyLocked()
	rt.mu.Unlock()

	if err := m.flushRuntime(taskID, rt); err != nil {
		return 0, err
	}
	m.StartDispatch(taskID)
	return count, nil
}

func (m *Manager) DeleteTask(taskID string) error {
	meta, err := m.GetTask(taskID)
	if err != nil {
		return err
	}
	if meta.Status == TaskStatusDownloading || meta.Status == TaskStatusParsing {
		return fmt.Errorf("cannot delete running task, please pause it first")
	}
	m.cancelDispatch(taskID)
	if err := m.UpdateTaskStatus(taskID, TaskStatusDeleted); err != nil {
		return err
	}
	m.runtimeMu.Lock()
	delete(m.runtimes, taskID)
	m.runtimeMu.Unlock()
	go m.deleteTaskAsync(taskID)
	return nil
}

func (m *Manager) deleteTaskAsync(taskID string) {
	m.acquireDeleteSlot()
	defer m.releaseDeleteSlot()

	if m.aria2 != nil {
		_, _ = m.aria2.ForceRemoveTaskDownloads(cache.GetTaskDir(taskID))
		_ = m.aria2.PurgeDownloadResult()
	}
	_ = os.Remove(taskProgressPath(taskID))
	_ = os.RemoveAll(cache.GetTaskDir(taskID))
	_ = m.DeleteTaskDB(taskID)

	m.runtimeMu.Lock()
	delete(m.runtimes, taskID)
	m.runtimeMu.Unlock()
}

func (m *Manager) acquireDeleteSlot() { m.deleteSem <- struct{}{} }
func (m *Manager) releaseDeleteSlot() { <-m.deleteSem }

func (m *Manager) StartDispatch(taskID string) {
	m.cancelDispatch(taskID)
	ctx, cancel := context.WithCancel(context.Background())
	m.dispatchMu.Lock()
	m.dispatches[taskID] = cancel
	m.dispatchMu.Unlock()

	go func() {
		defer m.cancelDispatch(taskID)
		m.dispatchTask(ctx, taskID)
	}()
}

func (m *Manager) cancelDispatch(taskID string) {
	m.dispatchMu.Lock()
	cancel, ok := m.dispatches[taskID]
	if ok {
		delete(m.dispatches, taskID)
	}
	m.dispatchMu.Unlock()
	if ok && cancel != nil {
		cancel()
	}
}

func (m *Manager) dispatchTask(ctx context.Context, taskID string) {
	rt, err := m.loadRuntime(taskID)
	if err != nil {
		log.Printf("load runtime failed task=%s: %v", taskID, err)
		return
	}

	const batchSize = 50
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		filenames := rt.claimPending(batchSize)
		if len(filenames) == 0 {
			return
		}

		itemsByFilename, err := m.LoadManifestItemsByFilenames(taskID, filenames)
		if err != nil {
			log.Printf("load manifest items failed task=%s: %v", taskID, err)
			for _, filename := range filenames {
				m.markFailedByFilename(taskID, filename, "load manifest items failed")
			}
			return
		}

		requests := make([]downloader.AddURIRequest, 0, len(filenames))
		for _, filename := range filenames {
			item, ok := itemsByFilename[filename]
			if !ok {
				m.markFailedByFilename(taskID, filename, "manifest item not found")
				continue
			}
			if cache.FileExists(taskID, item.Filename) && !cache.FileExists(taskID, item.Filename+".aria2") {
				m.markCompletedByFilename(taskID, item.Filename)
				continue
			}
			if err := cleanupResumeArtifacts(taskID, item.Filename); err != nil {
				m.markFailedByFilename(taskID, item.Filename, err.Error())
				continue
			}
			requests = append(requests, downloader.AddURIRequest{
				URI:      item.URL,
				Dir:      cache.GetTaskDir(taskID),
				Filename: item.Filename,
				Headers:  defaultHeaders(),
			})
		}
		if len(requests) == 0 {
			continue
		}

		gids, err := m.aria2.BatchAddURIs(requests)
		if err != nil {
			for _, req := range requests {
				m.markFailedByFilename(taskID, req.Filename, err.Error())
			}
			return
		}

		for idx, req := range requests {
			if idx >= len(gids) || gids[idx] == "" {
				m.markFailedByFilename(taskID, req.Filename, "missing gid from aria2")
				continue
			}
			if paused := rt.bindGID(req.Filename, gids[idx]); paused {
				_ = m.aria2.BatchPause([]string{gids[idx]})
			}
		}
		_ = m.flushRuntime(taskID, rt)
	}
}

func (m *Manager) progressNotificationLoop() {
	if m.aria2 == nil {
		return
	}
	for {
		err := m.aria2.ListenNotifications(context.Background(), func(method, gid string) {
			if method == "" || gid == "" {
				return
			}
			select {
			case m.progressNotifyCh <- aria2NotificationEvent{Method: method, GID: gid}:
			default:
			}
		})
		if err != nil {
			log.Printf("aria2 notification loop disconnected: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (m *Manager) progressNotificationWorker() {
	ticker := time.NewTicker(80 * time.Millisecond)
	defer ticker.Stop()

	buffer := make([]aria2NotificationEvent, 0, 256)
	flush := func() {
		if len(buffer) == 0 {
			return
		}
		events := append([]aria2NotificationEvent(nil), buffer...)
		buffer = buffer[:0]
		for _, event := range events {
			m.applyAria2Event(event)
		}
	}

	for {
		select {
		case event := <-m.progressNotifyCh:
			buffer = append(buffer, event)
			if len(buffer) >= 256 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (m *Manager) applyAria2Event(event aria2NotificationEvent) {
	taskID, filename, ok := m.findTaskByGID(event.GID)
	if !ok {
		return
	}
	switch event.Method {
	case "aria2.onDownloadComplete":
		m.markCompletedByFilename(taskID, filename)
	case "aria2.onDownloadError":
		m.markFailedByFilename(taskID, filename, "aria2 download error")
	case "aria2.onDownloadPause", "aria2.onDownloadStop":
		// runtime state is already sufficient; no-op
	case "aria2.onDownloadStart":
		// no-op
	}
}

func (m *Manager) flushDirtyLoop() {
	ticker := time.NewTicker(dirtyFlushTick)
	defer ticker.Stop()
	for range ticker.C {
		m.runtimeMu.Lock()
		ids := make([]string, 0, len(m.runtimes))
		for taskID := range m.runtimes {
			ids = append(ids, taskID)
		}
		m.runtimeMu.Unlock()
		for _, taskID := range ids {
			rt, err := m.loadRuntime(taskID)
			if err != nil {
				continue
			}
			if rt.shouldFlush(time.Now()) {
				_ = m.flushRuntime(taskID, rt)
			}
		}
	}
}

func (m *Manager) cleanupRuntimeLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		m.cleanupInactiveRuntimes()
	}
}

func (m *Manager) cleanupInactiveRuntimes() {
	now := time.Now()

	m.runtimeMu.Lock()
	snapshot := make(map[string]*taskRuntime, len(m.runtimes))
	for taskID, rt := range m.runtimes {
		snapshot[taskID] = rt
	}
	m.runtimeMu.Unlock()

	for taskID, rt := range snapshot {
		status, lastAccessAt, dirty := rt.stateForEviction()
		if dirty {
			continue
		}
		if status != TaskStatusPaused {
			continue
		}
		if now.Sub(lastAccessAt) < pausedRuntimeTTL {
			continue
		}
		if m.hasDispatch(taskID) {
			continue
		}
		m.evictRuntime(taskID, rt)
	}
}

func (m *Manager) reconcileLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		updated, err := m.SyncTaskProgress()
		if err != nil {
			log.Printf("reconcile failed: %v", err)
		} else if updated > 0 {
			log.Printf("reconcile updated %d items", updated)
		}
	}
}

func (m *Manager) SyncTaskProgress() (int, error) {
	tasks, err := m.GetTasksByStatuses(TaskStatusDownloading, TaskStatusPaused, TaskStatusFailed)
	if err != nil {
		return 0, err
	}
	updated := 0
	for _, meta := range tasks {
		count, err := m.ReconcileTask(meta.ID)
		if err != nil {
			return updated, err
		}
		updated += count
	}
	return updated, nil
}

func (m *Manager) ReconcileTask(taskID string) (int, error) {
	rt, err := m.loadRuntime(taskID)
	if err != nil {
		return 0, err
	}
	updated := 0

	for _, filename := range rt.remainingFilenamesSnapshot() {
		if cache.FileExists(taskID, filename) && !cache.FileExists(taskID, filename+".aria2") {
			if m.markCompletedByFilename(taskID, filename) {
				updated++
			}
		}
	}

	if m.aria2 != nil {
		statuses, err := m.aria2.QueueStatusesByDir(cache.GetTaskDir(taskID))
		if err != nil {
			return updated, err
		}
		for _, status := range statuses {
			filename := filepath.Base(status.FirstFilePath())
			if filename == "." || filename == "" {
				continue
			}
			rt.registerGID(status.Gid, filename)
			switch status.Status {
			case "complete":
				if m.markCompletedByFilename(taskID, filename) {
					updated++
				}
			case "error":
				if m.markFailedByFilename(taskID, filename, firstNonEmpty(status.ErrorMessage, "aria2 reconcile error")) {
					updated++
				}
			}
		}
	}
	if updated > 0 || rt.shouldFlush(time.Now()) {
		_ = m.flushRuntime(taskID, rt)
	}
	return updated, nil
}

func (m *Manager) loadRuntime(taskID string) (*taskRuntime, error) {
	m.runtimeMu.Lock()
	if rt, ok := m.runtimes[taskID]; ok {
		m.runtimeMu.Unlock()
		rt.touch()
		return rt, nil
	}
	m.runtimeMu.Unlock()

	meta, err := m.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	manifestIndex, err := m.LoadTaskManifestIndex(taskID)
	if err != nil {
		return nil, err
	}
	progress, err := readProgress(taskProgressPath(taskID), taskID)
	if err != nil {
		return nil, err
	}

	rt := newTaskRuntime(taskID, meta.TotalItems, meta.TotalSegments, manifestIndex, progress, meta.Status == TaskStatusPaused)
	rt.syncCompletedFiles(taskID)

	m.runtimeMu.Lock()
	if existing, ok := m.runtimes[taskID]; ok {
		m.runtimeMu.Unlock()
		existing.touch()
		return existing, nil
	}
	m.runtimes[taskID] = rt
	m.runtimeMu.Unlock()
	return rt, nil
}

func (m *Manager) flushRuntime(taskID string, rt *taskRuntime) error {
	start := time.Now()
	progress, snapshot := rt.snapshot()
	if err := writeJSONAtomic(taskProgressPath(taskID), progress); err != nil {
		return err
	}
	if err := m.UpdateTaskSnapshot(taskID, snapshot.Status, snapshot.DoneItems, snapshot.DownloadedSegments, snapshot.FailedItems); err != nil {
		return err
	}
	rt.markClean()
	switch snapshot.Status {
	case TaskStatusCompleted, TaskStatusFailed:
		m.evictRuntime(taskID, rt)
	}
	m.recordFlushCost(time.Since(start))
	return nil
}

func (m *Manager) recordFlushCost(cost time.Duration) {
	m.metricsMu.Lock()
	m.lastFlushCost = cost
	m.totalFlushCost += cost
	m.totalFlushCount++
	m.metricsMu.Unlock()
}

func (m *Manager) hasDispatch(taskID string) bool {
	m.dispatchMu.Lock()
	_, ok := m.dispatches[taskID]
	m.dispatchMu.Unlock()
	return ok
}

func (m *Manager) evictRuntime(taskID string, rt *taskRuntime) {
	m.runtimeMu.Lock()
	if current, ok := m.runtimes[taskID]; ok && current == rt {
		delete(m.runtimes, taskID)
	}
	m.runtimeMu.Unlock()
}

func (m *Manager) findTaskByGID(gid string) (string, string, bool) {
	m.runtimeMu.Lock()
	runtimes := make([]*taskRuntime, 0, len(m.runtimes))
	taskIDs := make([]string, 0, len(m.runtimes))
	for taskID, rt := range m.runtimes {
		taskIDs = append(taskIDs, taskID)
		runtimes = append(runtimes, rt)
	}
	m.runtimeMu.Unlock()

	for idx, rt := range runtimes {
		rt.mu.Lock()
		filename, ok := rt.gidToFile[gid]
		rt.mu.Unlock()
		if ok {
			return taskIDs[idx], filename, true
		}
	}
	return "", "", false
}

func (m *Manager) markCompletedByFilename(taskID, filename string) bool {
	rt, err := m.loadRuntime(taskID)
	if err != nil {
		return false
	}
	if !rt.markCompleted(filename) {
		return false
	}
	return true
}

func (m *Manager) markFailedByFilename(taskID, filename, errMsg string) bool {
	rt, err := m.loadRuntime(taskID)
	if err != nil {
		return false
	}
	if !rt.markFailed(filename, errMsg) {
		return false
	}
	return true
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
	writeJSON(w, map[string]interface{}{
		"total":   len(tasks),
		"items":   tasks,
		"metrics": m.RuntimeMetrics(),
	})
}

func (m *Manager) HandlePauseV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	paused, err := m.PauseTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]interface{}{"paused_items": paused})
}

func (m *Manager) HandleResumeV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	resumed, err := m.ResumeTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]interface{}{"resumed_items": resumed})
}

func (m *Manager) HandleRetryV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	retried, err := m.RetryTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]interface{}{"retried_items": retried})
}

func (m *Manager) HandleDeleteV1(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if err := m.DeleteTask(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Manager) HandleSyncProgress(w http.ResponseWriter, r *http.Request) {
	updated, err := m.SyncTaskProgress()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"updated": updated})
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
		http.Error(w, "invalid url", http.StatusBadRequest)
		return
	}

	taskID := cache.GetTaskID(body.URL)
	exists, status, err := m.CheckTaskExists(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if exists && status != TaskStatusDeleted {
		http.Error(w, fmt.Sprintf("task already exists with status: %s", status), http.StatusConflict)
		return
	}

	go func() {
		if err := triggerFunc(body); err != nil {
			log.Printf("start task failed url=%s err=%v", body.URL, err)
		}
	}()
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, map[string]interface{}{"id": taskID, "name": body.Name, "url": body.URL, "status": TaskStatusPending})
}

func (m *Manager) DeleteCompletedTasks() error {
	tasks, err := m.GetTasksByStatus(TaskStatusCompleted)
	if err != nil {
		return err
	}
	var errs []string
	for _, item := range tasks {
		if err := m.DeleteTask(item.ID); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func buildManifest(taskID, originalURL string, items []playlist.DownloadItem, totalSegments int) TaskManifest {
	manifestItems := make([]ManifestItem, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, ok := seen[item.Filename]; ok {
			continue
		}
		seen[item.Filename] = struct{}{}
		manifestItems = append(manifestItems, ManifestItem{
			Filename: item.Filename,
			URL:      item.URL,
			Type:     normalizeManifestType(item.Type),
		})
	}
	return TaskManifest{
		TaskID:        taskID,
		OriginalURL:   originalURL,
		TotalSegments: totalSegments,
		Items:         manifestItems,
	}
}

func buildInitialProgress(manifest TaskManifest) TaskProgressFile {
	return TaskProgressFile{
		TaskID:    manifest.TaskID,
		Failed:    []string{},
		UpdatedAt: time.Now(),
	}
}

func newTaskRuntime(taskID string, totalItems, totalSegments int, manifestIndex []ManifestIndexItem, progress TaskProgressFile, paused bool) *taskRuntime {
	remaining := make(map[string]uint32, len(manifestIndex))
	var maxSeq uint32
	for _, item := range manifestIndex {
		if item.Seq > maxSeq {
			maxSeq = item.Seq
		}
	}
	segmentBySeq := make([]bool, int(maxSeq)+1)
	remainingSegments := 0
	for _, item := range manifestIndex {
		remaining[item.Filename] = item.Seq
		if item.IsSegment {
			segmentBySeq[item.Seq] = true
			remainingSegments++
		}
	}
	return &taskRuntime{
		taskID:            taskID,
		totalItems:        totalItems,
		totalSegments:     totalSegments,
		segmentBySeq:      segmentBySeq,
		remaining:         remaining,
		failed:            failedSet(progress.Failed),
		dispatching:       make(map[string]struct{}),
		gidToFile:         make(map[string]string),
		fileToGID:         make(map[string]string),
		remainingSegments: remainingSegments,
		paused:            paused,
		dirty:             false,
		dirtySince:        time.Time{},
		lastAccessAt:      time.Now(),
	}
}

func (rt *taskRuntime) syncCompletedFiles(taskID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.lastAccessAt = time.Now()
	for filename, index := range rt.remaining {
		if cache.FileExists(taskID, filename) && !cache.FileExists(taskID, filename+".aria2") {
			delete(rt.remaining, filename)
			delete(rt.failed, filename)
			if rt.isSegment(index) && rt.remainingSegments > 0 {
				rt.remainingSegments--
			}
			rt.markDirtyLocked()
		}
	}
}

func (rt *taskRuntime) claimPending(limit int) []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.lastAccessAt = time.Now()
	if rt.paused {
		return nil
	}
	items := make([]string, 0, limit)
	for filename := range rt.remaining {
		if len(items) >= limit {
			break
		}
		if _, failed := rt.failed[filename]; failed {
			continue
		}
		if _, active := rt.fileToGID[filename]; active {
			continue
		}
		if _, dispatching := rt.dispatching[filename]; dispatching {
			continue
		}
		rt.dispatching[filename] = struct{}{}
		items = append(items, filename)
	}
	return items
}

func (rt *taskRuntime) bindGID(filename, gid string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.lastAccessAt = time.Now()
	delete(rt.dispatching, filename)
	rt.gidToFile[gid] = filename
	rt.fileToGID[filename] = gid
	rt.markDirtyLocked()
	return rt.paused
}

func (rt *taskRuntime) registerGID(gid, filename string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.lastAccessAt = time.Now()
	if _, ok := rt.remaining[filename]; !ok {
		return
	}
	rt.gidToFile[gid] = filename
	rt.fileToGID[filename] = gid
}

func (rt *taskRuntime) markCompleted(filename string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.lastAccessAt = time.Now()
	index, ok := rt.remaining[filename]
	if !ok {
		return false
	}
	delete(rt.remaining, filename)
	delete(rt.failed, filename)
	delete(rt.dispatching, filename)
	if gid, ok := rt.fileToGID[filename]; ok {
		delete(rt.fileToGID, filename)
		delete(rt.gidToFile, gid)
	}
	if rt.isSegment(index) && rt.remainingSegments > 0 {
		rt.remainingSegments--
	}
	rt.markDirtyLocked()
	return true
}

func (rt *taskRuntime) markFailed(filename, errMsg string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.lastAccessAt = time.Now()
	if _, ok := rt.remaining[filename]; !ok {
		return false
	}
	delete(rt.dispatching, filename)
	if gid, ok := rt.fileToGID[filename]; ok {
		delete(rt.fileToGID, filename)
		delete(rt.gidToFile, gid)
	}
	rt.failed[filename] = struct{}{}
	rt.markDirtyLocked()
	return true
}

func (rt *taskRuntime) pendingDispatchableCountLocked() int {
	count := 0
	for filename := range rt.remaining {
		if _, failed := rt.failed[filename]; failed {
			continue
		}
		if _, active := rt.fileToGID[filename]; active {
			continue
		}
		if _, dispatching := rt.dispatching[filename]; dispatching {
			continue
		}
		count++
	}
	return count
}

func (rt *taskRuntime) remainingFilenamesSnapshot() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	items := make([]string, 0, len(rt.remaining))
	for filename := range rt.remaining {
		items = append(items, filename)
	}
	return items
}

type runtimeSnapshot struct {
	Status             string
	DoneItems          int
	DownloadedSegments int
	FailedItems        int
}

func (rt *taskRuntime) snapshot() (TaskProgressFile, runtimeSnapshot) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.lastAccessAt = time.Now()

	doneItems := rt.totalItems - len(rt.remaining)
	downloadedSegments := rt.totalSegments - rt.remainingSegments
	failedItems := len(rt.failed)
	status := TaskStatusDownloading
	switch {
	case len(rt.remaining) == 0:
		status = TaskStatusCompleted
	case rt.paused:
		status = TaskStatusPaused
	case failedItems > 0 && failedItems == len(rt.remaining) && len(rt.fileToGID) == 0 && len(rt.dispatching) == 0:
		status = TaskStatusFailed
	}

	progress := TaskProgressFile{
		TaskID:             rt.taskID,
		Failed:             failedNames(rt.failed),
		DownloadedSegments: downloadedSegments,
		DoneItems:          doneItems,
		UpdatedAt:          time.Now(),
	}
	return progress, runtimeSnapshot{
		Status:             status,
		DoneItems:          doneItems,
		DownloadedSegments: downloadedSegments,
		FailedItems:        failedItems,
	}
}

func (rt *taskRuntime) markClean() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.dirty = false
	rt.dirtySince = time.Time{}
	rt.lastAccessAt = time.Now()
}

func (rt *taskRuntime) touch() {
	rt.mu.Lock()
	rt.lastAccessAt = time.Now()
	rt.mu.Unlock()
}

func (rt *taskRuntime) stateForEviction() (string, time.Time, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	status := TaskStatusDownloading
	switch {
	case len(rt.remaining) == 0:
		status = TaskStatusCompleted
	case rt.paused:
		status = TaskStatusPaused
	case len(rt.failed) > 0 && len(rt.failed) == len(rt.remaining) && len(rt.fileToGID) == 0 && len(rt.dispatching) == 0:
		status = TaskStatusFailed
	}
	return status, rt.lastAccessAt, rt.dirty
}

func (rt *taskRuntime) shouldFlush(now time.Time) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.dirty {
		return false
	}
	if rt.dirtySince.IsZero() {
		rt.dirtySince = now
	}
	wait := downloadingFlushInterval
	switch {
	case len(rt.remaining) == 0:
		wait = terminalFlushInterval
	case rt.paused:
		wait = pausedFlushInterval
	case len(rt.failed) > 0 && len(rt.failed) == len(rt.remaining) && len(rt.fileToGID) == 0 && len(rt.dispatching) == 0:
		wait = terminalFlushInterval
	}
	return now.Sub(rt.dirtySince) >= wait
}

func (rt *taskRuntime) markDirtyLocked() {
	if !rt.dirty {
		rt.dirtySince = time.Now()
	}
	rt.dirty = true
}

func (rt *taskRuntime) isSegment(seq uint32) bool {
	if int(seq) >= len(rt.segmentBySeq) {
		return false
	}
	return rt.segmentBySeq[seq]
}

func taskProgressPath(taskID string) string {
	return filepath.Join(cache.GetTaskDir(taskID), "progress.json")
}

func readProgress(path string, taskID string) (TaskProgressFile, error) {
	var progress TaskProgressFile
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return TaskProgressFile{TaskID: taskID, Failed: []string{}, UpdatedAt: time.Now()}, nil
	}
	if err != nil {
		return progress, err
	}
	if err := json.Unmarshal(data, &progress); err != nil {
		return progress, err
	}
	if progress.Failed == nil {
		progress.Failed = []string{}
	}
	return progress, nil
}

func writeJSONAtomic(path string, value interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func defaultHeaders() map[string]string {
	return config.GlobalConfig.Headers
}

func failedSet(names []string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func failedNames(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	return out
}

func normalizeManifestType(itemType string) string {
	if strings.EqualFold(strings.TrimSpace(itemType), "key") {
		return "key"
	}
	return "segment"
}

func cleanupResumeArtifacts(taskID, filename string) error {
	for _, path := range []string{cache.GetFilePath(taskID, filename), cache.GetFilePath(taskID, filename+".aria2")} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func keysOfMap(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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
