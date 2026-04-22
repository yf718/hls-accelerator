package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"hls-accelerator/internal/cache"
	"hls-accelerator/internal/config"
	"hls-accelerator/internal/database"
	"hls-accelerator/internal/downloader"
	playlist "hls-accelerator/internal/m3u8"
	"hls-accelerator/internal/task"

	"github.com/grafov/m3u8"
)

type Server struct {
	addr        string
	aria2       *downloader.Aria2Client
	client      *http.Client
	taskManager *task.Manager
	dispatchMu  sync.Mutex
	dispatchSeq uint64
	dispatches  map[string]dispatchState
}

type dispatchState struct {
	token  uint64
	cancel context.CancelFunc
}

func NewServer() (*Server, error) {
	aria2 := downloader.NewClient()
	db, err := database.Init(config.GlobalConfig.CacheDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init db: %v", err)
	}
	tm, err := task.NewManager(aria2, db)
	if err != nil {
		return nil, fmt.Errorf("failed to init task manager: %v", err)
	}
	return &Server{
		addr:  fmt.Sprintf(":%d", config.GlobalConfig.ProxyPort),
		aria2: aria2,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		taskManager: tm,
		dispatches:  make(map[string]dispatchState),
	}, nil
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	mux.HandleFunc("GET /api/v1/tasks", s.taskManager.HandleListV1)
	mux.HandleFunc("POST /api/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		s.taskManager.HandleAdd(w, r, s.startDownloadFromURL)
	})
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.taskManager.HandleGetTaskV1)
	mux.HandleFunc("GET /api/v1/tasks/{id}/items", s.taskManager.HandleListTaskItemsV1)
	mux.HandleFunc("GET /api/v1/tasks/{id}/items/{itemId}", s.taskManager.HandleGetTaskItemV1)
	mux.HandleFunc("POST /api/v1/tasks/{id}/pause", s.handlePauseTask)
	mux.HandleFunc("POST /api/v1/tasks/{id}/resume", s.handleResumeTask)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", s.handleRetryTask)
	mux.HandleFunc("POST /api/v1/tasks/sync", s.taskManager.HandleSyncProgress)
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", s.handleDeleteTask)

	mux.HandleFunc("/proxy/m3u8/", s.handleM3U8)
	mux.HandleFunc("/proxy/seg/", s.handleSegment)
	mux.HandleFunc("/proxy/key/", s.handleKey)

	if config.GlobalConfig.AutoCleanupEnabled {
		go s.startAutoCleanup()
	}

	log.Printf("Proxy starting at http://localhost%s", s.addr)
	return http.ListenAndServe(s.addr, mux)
}

func (s *Server) startDownloadFromURL(addReq task.AddTaskRequest) error {
	rawURL := addReq.URL
	taskName := strings.TrimSpace(addReq.Name)
	if taskName == "" {
		taskName = task.DeriveTaskName(rawURL)
	}

	httpReq, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	for key, value := range config.GlobalConfig.Headers {
		httpReq.Header.Set(key, value)
	}

	resp, err := s.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	base, _ := url.Parse(rawURL)
	pl, type_, err := playlist.Parse(resp.Body)
	if err != nil {
		return err
	}

	if type_ == playlist.Master {
		masterPl := pl.(*m3u8.MasterPlaylist)
		if len(masterPl.Variants) == 0 {
			return fmt.Errorf("master playlist has no variants")
		}
		bestVar := masterPl.Variants[0]
		for _, variant := range masterPl.Variants {
			if variant.Bandwidth > bestVar.Bandwidth {
				bestVar = variant
			}
		}
		return s.startDownloadFromURL(task.AddTaskRequest{
			Name: taskName,
			URL:  playlist.ResolveURL(base, bestVar.URI),
		})
	}

	if type_ != playlist.Variant {
		return fmt.Errorf("unsupported playlist type")
	}

	taskID := cache.GetTaskID(rawURL)
	mediaPl := pl.(*m3u8.MediaPlaylist)
	proxyBase := fmt.Sprintf("http://localhost:%d/proxy", config.GlobalConfig.ProxyPort)
	updated, items, total := playlist.RewriteVariant(mediaPl, proxyBase, taskID, base)

	if err := cache.EnsureTaskDir(taskID); err != nil {
		return err
	}

	meta := task.TaskMetadata{
		ID:             taskID,
		Name:           taskName,
		OriginalURL:    rawURL,
		TotalSegments:  total,
		CreatedTime:    time.Now(),
		UpdatedTime:    time.Now(),
		Status:         task.TaskStatusParsing,
		OutputDir:      cache.GetTaskDir(taskID),
		ProxiedContent: updated,
	}
	created, err := s.taskManager.TryCreateTask(meta)
	if err != nil {
		return err
	}
	if !created {
		_, status, err := s.taskManager.CheckTaskExists(taskID)
		if err != nil {
			return fmt.Errorf("task already exists")
		}
		return fmt.Errorf("task already exists: %s", status)
	}

	s.triggerDownloads(taskID, items)
	return nil
}

func (s *Server) setM3U8Headers(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func (s *Server) writeM3U8Response(w http.ResponseWriter, content string) {
	s.setM3U8Headers(w)
	_, _ = w.Write([]byte(content))
}

func (s *Server) fetchUpstreamM3U8(originURL string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, originURL, nil)
	if err != nil {
		return nil, err
	}
	for key, value := range config.GlobalConfig.Headers {
		req.Header.Set(key, value)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("upstream returned bad status: %d", resp.StatusCode)
	}
	return resp, nil
}

func (s *Server) handleM3U8(w http.ResponseWriter, r *http.Request) {
	encodedURL := strings.TrimPrefix(r.URL.Path, "/proxy/m3u8/")
	if encodedURL == "" {
		http.Error(w, "Missing URL parameter", http.StatusBadRequest)
		return
	}
	originURL, err := url.QueryUnescape(encodedURL)
	if err != nil {
		http.Error(w, "Invalid URL encoding", http.StatusBadRequest)
		return
	}
	parsedURL, err := url.Parse(originURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}
	taskID := cache.GetTaskID(originURL)
	if content, err := s.taskManager.GetTaskProxiedContent(taskID); err == nil && content != "" {
		s.writeM3U8Response(w, content)
		return
	}

	resp, err := s.fetchUpstreamM3U8(originURL)
	if err != nil {
		http.Error(w, "Failed to fetch upstream", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	pl, playlistType, err := playlist.Parse(resp.Body)
	if err != nil {
		http.Error(w, "Failed to parse m3u8", http.StatusBadGateway)
		return
	}

	proxyBase := fmt.Sprintf("http://%s/proxy", r.Host)
	switch playlistType {
	case playlist.Master:
		updated := playlist.RewriteMaster(pl.(*m3u8.MasterPlaylist), proxyBase, parsedURL)
		s.writeM3U8Response(w, updated)
	case playlist.Variant:
		s.handleVariantPlaylist(w, pl.(*m3u8.MediaPlaylist), proxyBase, parsedURL, originURL, taskID)
	default:
		http.Error(w, "Unknown playlist type", http.StatusBadGateway)
	}
}

func (s *Server) handleVariantPlaylist(w http.ResponseWriter, mediaPl *m3u8.MediaPlaylist, proxyBase string, baseURL *url.URL, originURL, taskID string) {
	if err := cache.EnsureTaskDir(taskID); err != nil {
		http.Error(w, "Failed to initialize cache", http.StatusInternalServerError)
		return
	}
	updated, items, total := playlist.RewriteVariant(mediaPl, proxyBase, taskID, baseURL)

	go func() {
		meta := task.TaskMetadata{
			ID:             taskID,
			Name:           task.DeriveTaskName(originURL),
			OriginalURL:    originURL,
			TotalSegments:  total,
			CreatedTime:    time.Now(),
			UpdatedTime:    time.Now(),
			Status:         task.TaskStatusParsing,
			OutputDir:      cache.GetTaskDir(taskID),
			ProxiedContent: updated,
		}
		created, err := s.taskManager.TryCreateTask(meta)
		if err != nil {
			log.Printf("Failed to create task %s: %v", taskID, err)
			return
		}
		if !created {
			return
		}
		s.triggerDownloads(taskID, items)
	}()

	s.writeM3U8Response(w, updated)
}

func (s *Server) handleProxyFile(w http.ResponseWriter, r *http.Request, pathPrefix string) {
	pathValue := strings.TrimPrefix(r.URL.Path, pathPrefix)
	parts := strings.SplitN(pathValue, "/", 3)
	if len(parts) < 3 {
		http.Error(w, "Invalid path structure", http.StatusBadRequest)
		return
	}
	taskID, filename, encodedURL := parts[0], parts[1], parts[2]
	originURL, err := url.QueryUnescape(encodedURL)
	if err != nil {
		http.Error(w, "Invalid URL encoding", http.StatusBadRequest)
		return
	}

	if cache.FileExists(taskID, filename) && !cache.FileExists(taskID, filename+".aria2") {
		http.ServeFile(w, r, cache.GetFilePath(taskID, filename))
		return
	}

	req, _ := http.NewRequest(http.MethodGet, originURL, nil)
	for key, value := range config.GlobalConfig.Headers {
		req.Header.Set(key, value)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, "Failed to fetch upstream", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		w.Header()[key] = values
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleSegment(w http.ResponseWriter, r *http.Request) {
	s.handleProxyFile(w, r, "/proxy/seg/")
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	s.handleProxyFile(w, r, "/proxy/key/")
}

func (s *Server) triggerDownloads(taskID string, items []playlist.DownloadItem) {
	if len(items) == 0 {
		return
	}
	if err := s.taskManager.CreateTaskItemsPlaceholders(taskID, items); err != nil {
		log.Printf("CreateTaskItemsPlaceholders failed task=%s: %v", taskID, err)
		return
	}
	s.startDispatchPendingTaskItems(taskID)
}

func (s *Server) startDispatchPendingTaskItems(taskID string) {
	s.cancelDispatchPendingTaskItems(taskID)
	ctx, cancel := context.WithCancel(context.Background())
	s.dispatchMu.Lock()
	s.dispatchSeq++
	token := s.dispatchSeq
	s.dispatches[taskID] = dispatchState{token: token, cancel: cancel}
	s.dispatchMu.Unlock()
	go func() {
		defer s.finishDispatchPendingTaskItems(taskID, token)
		s.dispatchPendingTaskItems(ctx, taskID)
	}()
}

func (s *Server) cancelDispatchPendingTaskItems(taskID string) {
	s.dispatchMu.Lock()
	state, ok := s.dispatches[taskID]
	delete(s.dispatches, taskID)
	s.dispatchMu.Unlock()
	if ok && state.cancel != nil {
		state.cancel()
	}
}

func (s *Server) finishDispatchPendingTaskItems(taskID string, token uint64) {
	s.dispatchMu.Lock()
	current, ok := s.dispatches[taskID]
	if ok && current.token == token {
		delete(s.dispatches, taskID)
	}
	s.dispatchMu.Unlock()
}

func (s *Server) canDispatchTask(ctx context.Context, taskID string) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}
	meta, err := s.taskManager.GetTask(taskID)
	if err != nil {
		return false
	}
	return meta.Status == task.TaskStatusDownloading || meta.Status == task.TaskStatusParsing
}

func (s *Server) resetCanceledDispatchItem(taskID, filename string) {
	meta, err := s.taskManager.GetTask(taskID)
	if err != nil {
		_ = s.taskManager.ResetTaskItemToPending(taskID, filename)
		return
	}
	if meta.Status == task.TaskStatusPaused {
		_ = s.taskManager.ResetTaskItemToPaused(taskID, filename)
		return
	}
	_ = s.taskManager.ResetTaskItemToPending(taskID, filename)
}

func (s *Server) dispatchPendingTaskItems(ctx context.Context, taskID string) {
	if s.aria2 == nil {
		return
	}
	dir := cache.GetTaskDir(taskID)
	headers := config.GlobalConfig.Headers

	pending, err := s.taskManager.ListPendingTaskItems(taskID)
	if err != nil {
		log.Printf("ListPendingTaskItems failed task=%s: %v", taskID, err)
		return
	}

	const batchSize = 50
	for start := 0; start < len(pending); start += batchSize {
		select {
		case <-ctx.Done():
			return
		default:
		}

		end := start + batchSize
		if end > len(pending) {
			end = len(pending)
		}

		chunk := pending[start:end]
		requests := make([]downloader.AddURIRequest, 0, len(chunk))
		readyItems := make([]task.TaskItem, 0, len(chunk))

		for _, item := range chunk {
			marked, err := s.taskManager.MarkTaskItemSubmitting(taskID, item.Filename)
			if err != nil || !marked {
				continue
			}
			if !s.canDispatchTask(ctx, taskID) {
				s.resetCanceledDispatchItem(taskID, item.Filename)
				return
			}
			if cache.FileExists(taskID, item.Filename) {
				if !cache.FileExists(taskID, item.Filename+".aria2") {
					_, _ = s.taskManager.MarkTaskItemCompletedByFilename(taskID, item.Filename)
					continue
				}
				if err := cleanupResumeArtifacts(taskID, item.Filename); err != nil {
					_ = s.taskManager.MarkTaskItemSubmitFailed(taskID, item.Filename, err.Error())
					continue
				}
			}
			requests = append(requests, downloader.AddURIRequest{
				URI:      item.URL,
				Dir:      dir,
				Filename: item.Filename,
				Headers:  headers,
			})
			readyItems = append(readyItems, item)
		}

		if len(requests) == 0 {
			continue
		}

		gids, err := s.aria2.BatchAddURIs(requests)
		if err != nil {
			for _, item := range readyItems {
				_ = s.taskManager.MarkTaskItemSubmitFailed(taskID, item.Filename, err.Error())
			}
			continue
		}
		if !s.canDispatchTask(ctx, taskID) {
			s.aria2.ForceRemoveMany(gids)
			for _, item := range readyItems {
				s.resetCanceledDispatchItem(taskID, item.Filename)
			}
			return
		}

		for idx, item := range readyItems {
			if idx >= len(gids) || gids[idx] == "" {
				_ = s.taskManager.MarkTaskItemSubmitFailed(taskID, item.Filename, "missing gid from aria2")
				continue
			}
			if err := s.taskManager.BindTaskItemToAria2(taskID, item.Filename, gids[idx]); err != nil {
				_ = s.aria2.ForceRemove(gids[idx])
				_ = s.taskManager.MarkTaskItemSubmitFailed(taskID, item.Filename, err.Error())
				continue
			}
			s.pauseBoundItemIfTaskPaused(taskID, gids[idx])
		}
	}
}

func (s *Server) pauseBoundItemIfTaskPaused(taskID, gid string) {
	meta, err := s.taskManager.GetTask(taskID)
	if err != nil || meta.Status != task.TaskStatusPaused || gid == "" || s.aria2 == nil {
		return
	}
	_ = s.aria2.BatchPause([]string{gid})
	_, _, _ = s.taskManager.UpdateTaskItemStateByGID(gid, "paused", "", "", 0)
}

func cleanupResumeArtifacts(taskID, filename string) error {
	paths := []string{
		cache.GetFilePath(taskID, filename),
		cache.GetFilePath(taskID, filename+".aria2"),
	}
	for _, current := range paths {
		if err := os.Remove(current); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (s *Server) handlePauseTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	s.cancelDispatchPendingTaskItems(taskID)
	paused, err := s.taskManager.PauseTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	go func() {
		if updated, err := s.taskManager.ReconcileTaskItems(taskID); err != nil {
			log.Printf("pause reconcile failed task=%s: %v", taskID, err)
		} else if updated > 0 {
			log.Printf("pause reconcile updated %d items for task=%s", updated, taskID)
		}
	}()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"paused_items": paused})
}

func (s *Server) handleResumeTask(w http.ResponseWriter, r *http.Request) {
	s.taskManager.HandleResumeV1(w, r, s.startDispatchPendingTaskItems)
}

func (s *Server) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	s.taskManager.HandleRetryV1(w, r, s.startDispatchPendingTaskItems)
}

func (s *Server) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	s.cancelDispatchPendingTaskItems(taskID)
	s.taskManager.HandleDeleteV1(w, r)
}

func (s *Server) startAutoCleanup() {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day()+1, 3, 0, 0, 0, now.Location())
	time.Sleep(next.Sub(now))
	s.runCleanup()
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		s.runCleanup()
	}
}

func (s *Server) runCleanup() {
	if err := s.taskManager.DeleteCompletedTasks(); err != nil {
		log.Printf("Auto cleanup failed: %v", err)
	}
}
