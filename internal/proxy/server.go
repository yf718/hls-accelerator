package proxy

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
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
}

func NewServer() (*Server, error) {
	aria2 := downloader.NewClient()

	db, err := database.Init(config.GlobalConfig.CacheDir) // Store DB in cache dir
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
	}, nil
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Static Files (Web UI)
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	// API Endpoints
	mux.HandleFunc("GET /api/tasks", s.taskManager.HandleList)
	mux.HandleFunc("POST /api/tasks", func(w http.ResponseWriter, r *http.Request) {
		s.taskManager.HandleAdd(w, r, s.startDownloadFromURL)
	})
	mux.HandleFunc("POST /api/tasks/sync", s.taskManager.HandleSyncProgress)
	mux.HandleFunc("POST /api/tasks/{id}/sync", s.handleSyncTask)
	mux.HandleFunc("DELETE /api/tasks/{id}", s.taskManager.HandleDelete)
	mux.HandleFunc("POST /api/tasks/{id}/stop", s.taskManager.HandleStop)

	// Proxy Endpoints
	mux.HandleFunc("/proxy/m3u8/", s.handleM3U8)
	mux.HandleFunc("/proxy/seg/", s.handleSegment)
	mux.HandleFunc("/proxy/key/", s.handleKey)

	// Start auto cleanup task if enabled
	if config.GlobalConfig.AutoCleanupEnabled {
		go s.startAutoCleanup()
		log.Println("Auto cleanup enabled: will run daily at midnight")
	}

	log.Printf("Proxy starting at http://localhost%s", s.addr)
	return http.ListenAndServe(s.addr, mux)
}

// startDownloadFromURL is used by the API to start a task without a player
func (s *Server) startDownloadFromURL(addReq task.AddTaskRequest) error {
	rawURL := addReq.URL
	taskName := strings.TrimSpace(addReq.Name)
	if taskName == "" {
		taskName = task.DeriveTaskName(rawURL)
	}

	httpReq, _ := http.NewRequest("GET", rawURL, nil)
	for k, v := range config.GlobalConfig.Headers {
		httpReq.Header.Set(k, v)
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

	// If Master, we just pick the first variant for now (Simplified)
	// In a real tool we might want to ask the user or fetch all.
	// For now, let's just error or handle if it's Variant.

	if type_ == playlist.Master {
		// Just log for now. A better implementation would find the best variant.
		masterPl := pl.(*m3u8.MasterPlaylist)
		if len(masterPl.Variants) > 0 {
			// recursively call with the best variant
			bestVar := masterPl.Variants[0]
			// Find max bandwidth?
			for _, v := range masterPl.Variants {
				if v.Bandwidth > bestVar.Bandwidth {
					bestVar = v
				}
			}
			fullURL := playlist.ResolveURL(base, bestVar.URI)
			return s.startDownloadFromURL(task.AddTaskRequest{
				Name: taskName,
				URL:  fullURL,
			})
		}
		return fmt.Errorf("master playlist has no variants")
	}

	if type_ == playlist.Variant {
		taskID := cache.GetTaskID(rawURL)
		// Save Metadata
		mediaPl := pl.(*m3u8.MediaPlaylist)
		// We use a dummy proxy base since we are just downloading
		proxyBase := fmt.Sprintf("http://localhost:%d/proxy", config.GlobalConfig.ProxyPort)
		updated, items, total := playlist.RewriteVariant(mediaPl, proxyBase, taskID, base)

		// Create task directory
		creatDirErr := cache.EnsureTaskDir(taskID)
		if creatDirErr != nil {
			return fmt.Errorf("failed to create task directory: %v", creatDirErr)
		}

		meta := task.TaskMetadata{
			ID:             taskID,
			Name:           taskName,
			OriginalURL:    rawURL,
			TotalSegments:  total,
			CreatedTime:    time.Now(),
			Status:         "downloading",
			ProxiedContent: updated,
		}
		created, taskErr := s.taskManager.TryCreateTask(meta)
		if taskErr != nil {
			return taskErr
		}
		if !created {
			_, status, err := s.taskManager.CheckTaskExists(taskID)
			if err != nil {
				return fmt.Errorf("task already exists")
			}
			return fmt.Errorf("task already exists: %s", status)
		}

		s.triggerDownloads(taskID, items)
	}
	return nil
}

// setM3U8Headers sets common headers for M3U8 responses
func (s *Server) setM3U8Headers(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

// writeM3U8Response writes M3U8 content to response with proper headers
func (s *Server) writeM3U8Response(w http.ResponseWriter, content string) {
	s.setM3U8Headers(w)
	w.Write([]byte(content))
}

// fetchUpstreamM3U8 fetches M3U8 content from upstream server
func (s *Server) fetchUpstreamM3U8(originURL string) (*http.Response, error) {
	req, err := http.NewRequest("GET", originURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set custom headers
	for k, v := range config.GlobalConfig.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch upstream: %w", err)
	}

	// Check HTTP status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("upstream returned bad status: %d", resp.StatusCode)
	}

	return resp, nil
}

func (s *Server) handleM3U8(w http.ResponseWriter, r *http.Request) {
	// Path: /proxy/m3u8/{encoded_url}
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

	// Validate origin URL
	parsedURL, err := url.Parse(originURL)
	if err != nil {
		http.Error(w, "Invalid URL format", http.StatusBadRequest)
		return
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		http.Error(w, "Invalid URL: missing scheme or host", http.StatusBadRequest)
		return
	}

	// Calculate taskID once (used for both cache check and task management)
	taskID := cache.GetTaskID(originURL)

	// Optimization: Check DB first to avoid unnecessary upstream requests
	if content, err := s.taskManager.GetTaskProxiedContent(taskID); err == nil && content != "" {
		s.writeM3U8Response(w, content)
		return
	}

	// Fetch from upstream
	resp, err := s.fetchUpstreamM3U8(originURL)
	if err != nil {
		log.Printf("Failed to fetch M3U8 from %s: %v", originURL, err)
		http.Error(w, "Failed to fetch upstream", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Parse M3U8 playlist
	pl, playlistType, err := playlist.Parse(resp.Body)
	if err != nil {
		log.Printf("Failed to parse M3U8 from %s: %v", originURL, err)
		http.Error(w, "Failed to parse m3u8", http.StatusBadGateway)
		return
	}

	proxyBase := fmt.Sprintf("http://%s/proxy", r.Host)

	// Handle based on playlist type
	switch playlistType {
	case playlist.Master:
		s.handleMasterPlaylist(w, pl.(*m3u8.MasterPlaylist), proxyBase, parsedURL)
	case playlist.Variant:
		s.handleVariantPlaylist(w, r, pl.(*m3u8.MediaPlaylist), proxyBase, parsedURL, originURL, taskID)
	default:
		http.Error(w, "Unknown playlist type", http.StatusBadGateway)
	}
}

// handleMasterPlaylist processes and returns a master playlist
func (s *Server) handleMasterPlaylist(w http.ResponseWriter, masterPl *m3u8.MasterPlaylist, proxyBase string, baseURL *url.URL) {
	updated := playlist.RewriteMaster(masterPl, proxyBase, baseURL)
	s.writeM3U8Response(w, updated)
}

// handleVariantPlaylist processes a variant playlist and manages download tasks
func (s *Server) handleVariantPlaylist(w http.ResponseWriter, r *http.Request, mediaPl *m3u8.MediaPlaylist, proxyBase string, baseURL *url.URL, originURL, taskID string) {
	// Ensure cache directory exists
	if err := cache.EnsureTaskDir(taskID); err != nil {
		log.Printf("Failed to create task directory for %s: %v", taskID, err)
		http.Error(w, "Failed to initialize cache", http.StatusInternalServerError)
		return
	}

	// Rewrite playlist and get download items
	updated, items, total := playlist.RewriteVariant(mediaPl, proxyBase, taskID, baseURL)

	// Let the unique index arbitrate ownership. Only the creator triggers downloads.
	go func() {
		meta := task.TaskMetadata{
			ID:             taskID,
			Name:           task.DeriveTaskName(originURL),
			OriginalURL:    originURL,
			TotalSegments:  total,
			CreatedTime:    time.Now(),
			Status:         "downloading",
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

	// Return rewritten playlist immediately
	s.writeM3U8Response(w, updated)
}

// Common handler for Segments and Keys
// Path format: /proxy/{type}/{taskID}/{filename}/{encoded_url}
func (s *Server) handleProxyFile(w http.ResponseWriter, r *http.Request, pathPrefix string) {
	path := strings.TrimPrefix(r.URL.Path, pathPrefix)
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		http.Error(w, "Invalid path structure", http.StatusBadRequest)
		return
	}
	taskID := parts[0]
	filename := parts[1]
	encodedURL := parts[2]

	originURL, err := url.QueryUnescape(encodedURL)
	if err != nil {
		http.Error(w, "Invalid URL encoding", http.StatusBadRequest)
		return
	}

	// Check Cache
	if cache.FileExists(taskID, filename) {
		// Check for .aria2 file implies incomplete
		if !cache.FileExists(taskID, filename+".aria2") {
			localPath := cache.GetFilePath(taskID, filename)
			http.ServeFile(w, r, localPath)
			return
		}
	}

	// Fallback to Live Proxy
	req, _ := http.NewRequest("GET", originURL, nil)
	for k, v := range config.GlobalConfig.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, "Failed to fetch upstream", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy headers
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (s *Server) handleSegment(w http.ResponseWriter, r *http.Request) {
	s.handleProxyFile(w, r, "/proxy/seg/")
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	s.handleProxyFile(w, r, "/proxy/key/")
}

func (s *Server) triggerDownloads(taskID string, items []playlist.DownloadItem) {
	if len(items) > 0 {
		if err := s.taskManager.CreateTaskItemsPlaceholders(taskID, items); err != nil {
			log.Printf("triggerDownloads: CreateTaskItemsPlaceholders failed task=%s: %v", taskID, err)
			return
		}
	}

	s.dispatchPendingTaskItems(taskID)
}

func (s *Server) dispatchPendingTaskItems(taskID string) {
	dir := cache.GetTaskDir(taskID)
	headers := config.GlobalConfig.Headers

	pending, err := s.taskManager.ListPendingTaskItems(taskID)
	if err != nil {
		log.Printf("dispatchPendingTaskItems: ListPendingTaskItems failed task=%s: %v", taskID, err)
		return
	}

	for _, item := range pending {
		marked, err := s.taskManager.MarkTaskItemSubmitting(taskID, item.Filename)
		if err != nil {
			log.Printf("dispatchPendingTaskItems: MarkTaskItemSubmitting failed task=%s file=%s: %v", taskID, item.Filename, err)
			continue
		}
		if !marked {
			continue
		}

		if cache.FileExists(taskID, item.Filename) {
			if !cache.FileExists(taskID, item.Filename+".aria2") {
				updated, err := s.taskManager.MarkTaskItemCompletedByFilename(taskID, item.Filename)
				if err != nil {
					log.Printf("dispatchPendingTaskItems: MarkTaskItemCompletedByFilename failed task=%s file=%s: %v", taskID, item.Filename, err)
				} else if updated {
					log.Printf("dispatchPendingTaskItems: marked existing file as completed task=%s file=%s", taskID, item.Filename)
				}
				continue
			}
			if err := s.taskManager.MarkTaskItemSubmitFailed(taskID, item.Filename, "local partial file exists"); err != nil {
				log.Printf("dispatchPendingTaskItems: MarkTaskItemSubmitFailed partial file failed task=%s file=%s: %v", taskID, item.Filename, err)
			}
			continue
		}

		actualGID, err := s.aria2.AddUri(item.URL, dir, item.Filename, headers, "")
		if err != nil {
			log.Printf("dispatchPendingTaskItems: aria2.addUri failed task=%s file=%s: %v", taskID, item.Filename, err)
			if markErr := s.taskManager.MarkTaskItemSubmitFailed(taskID, item.Filename, err.Error()); markErr != nil {
				log.Printf("dispatchPendingTaskItems: MarkTaskItemSubmitFailed failed task=%s file=%s: %v", taskID, item.Filename, markErr)
			}
			continue
		}

		if err := s.taskManager.BindTaskItemToAria2(taskID, item.Filename, actualGID); err != nil {
			log.Printf("dispatchPendingTaskItems: BindTaskItemToAria2 failed task=%s file=%s gid=%s: %v", taskID, item.Filename, actualGID, err)
			if rmErr := s.aria2.ForceRemove(actualGID); rmErr != nil {
				log.Printf("dispatchPendingTaskItems: ForceRemove orphan gid=%s after bind error: %v", actualGID, rmErr)
			}
			if markErr := s.taskManager.MarkTaskItemSubmitFailed(taskID, item.Filename, err.Error()); markErr != nil {
				log.Printf("dispatchPendingTaskItems: MarkTaskItemSubmitFailed after bind error failed task=%s file=%s: %v", taskID, item.Filename, markErr)
			}
		}
	}
}

func (s *Server) handleSyncTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if taskID == "" {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	meta, err := s.taskManager.GetTask(taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Task not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if meta.Status != "stopped" {
		http.Error(w, "Only stopped tasks can be resumed", http.StatusConflict)
		return
	}

	updated, err := s.taskManager.SyncTaskProgressForTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reset, err := s.taskManager.ResetFailedTaskItemsToPending(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := s.taskManager.UpdateTaskStatus(taskID, "downloading"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.dispatchPendingTaskItems(taskID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"updated":      updated,
		"failed_reset": reset,
		"resumed":      true,
		"task_id":      taskID,
	})
}

// startAutoCleanup starts a goroutine that runs cleanup at midnight every day
func (s *Server) startAutoCleanup() {
	// Calculate duration until next midnight
	now := time.Now()
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 3, 0, 0, 0, now.Location())
	durationUntilMidnight := midnight.Sub(now)

	// Wait until midnight
	time.Sleep(durationUntilMidnight)

	// Run cleanup immediately at first midnight
	s.runCleanup()

	// Then run cleanup every 24 hours
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		s.runCleanup()
	}
}

// runCleanup executes the cleanup of completed tasks
func (s *Server) runCleanup() {
	log.Println("Starting auto cleanup of completed tasks...")
	if err := s.taskManager.DeleteCompletedTasks(); err != nil {
		log.Printf("Auto cleanup failed: %v", err)
	} else {
		log.Println("Auto cleanup completed successfully")
	}
}
