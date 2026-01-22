package proxy

import (
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
	mux.HandleFunc("DELETE /api/tasks/{id}", s.taskManager.HandleDelete)
	mux.HandleFunc("POST /api/tasks/{id}/stop", s.taskManager.HandleStop)

	// Proxy Endpoints
	mux.HandleFunc("/proxy/m3u8/", s.handleM3U8)
	mux.HandleFunc("/proxy/seg/", s.handleSegment)
	mux.HandleFunc("/proxy/key/", s.handleKey)

	log.Printf("Proxy starting at http://localhost%s", s.addr)
	return http.ListenAndServe(s.addr, mux)
}

// startDownloadFromURL is used by the API to start a task without a player
func (s *Server) startDownloadFromURL(rawURL string) error {
	// Check if task exists (for idempotency and recursive calls)
	taskID := cache.GetTaskID(rawURL)

	exists, status, err := s.taskManager.CheckTaskExists(taskID)
	if err != nil {
		return err
	}
	if exists {
		if status == "downloading" || status == "completed" {
			return fmt.Errorf("task already exists: %s", status)
		}
	}

	req, _ := http.NewRequest("GET", rawURL, nil)
	for k, v := range config.GlobalConfig.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
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
			return s.startDownloadFromURL(fullURL)
		}
		return fmt.Errorf("master playlist has no variants")
	}

	if type_ == playlist.Variant {
		taskID := cache.GetTaskID(rawURL)
		cache.EnsureTaskDir(taskID)

		// Save Metadata
		mediaPl := pl.(*m3u8.MediaPlaylist)
		// We use a dummy proxy base since we are just downloading
		// Ideally we should use the actual proxy port if known, but localhost/proxy is just a placeholder here
		// unless the saved content is used for playback.
		// NOTE: 'updated' string from RewriteVariant with "http://localhost/proxy" might be broken for playback
		// if the player connects to localhost:8080.
		// FIX: Use config port to make it usable.
		proxyBase := fmt.Sprintf("http://localhost:%d/proxy", config.GlobalConfig.ProxyPort)
		updated, items, total := playlist.RewriteVariant(mediaPl, proxyBase, taskID, base)

		meta := task.TaskMetadata{
			ID:             taskID,
			OriginalURL:    rawURL,
			TotalSegments:  total,
			CreatedTime:    time.Now(),
			Status:         "downloading",
			ProxiedContent: updated,
		}
		// Use DB instead of file
		if err := s.taskManager.CreateTask(meta); err != nil {
			return err
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

	// Check if task already exists
	exists, status, err := s.taskManager.CheckTaskExists(taskID)
	if err != nil {
		log.Printf("Failed to check task existence for %s: %v", taskID, err)
		// Continue anyway - we'll try to create/update
	}

	// Determine if we should start a new task
	// If task exists and is downloading/completed, proxied content was already saved during creation
	shouldStartNewTask := !exists || (status != "downloading" && status != "completed")

	if shouldStartNewTask {
		// Start new task asynchronously
		go func() {
			meta := task.TaskMetadata{
				ID:             taskID,
				OriginalURL:    originURL,
				TotalSegments:  total,
				CreatedTime:    time.Now(),
				Status:         "downloading",
				ProxiedContent: updated,
			}
			if err := s.taskManager.CreateTask(meta); err != nil {
				log.Printf("Failed to create task %s: %v", taskID, err)
				return
			}

			// Trigger downloads
			s.triggerDownloads(taskID, items)
		}()
	}
	// If task already exists, proxied content was saved during creation and unlikely to change

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
	dir := cache.GetTaskDir(taskID)
	headers := config.GlobalConfig.Headers

	for _, item := range items {
		// Skip if file already exists
		if cache.FileExists(taskID, item.Filename) {
			continue
		}

		// Let aria2 generate GID automatically
		actualGID, err := s.aria2.AddUri(item.URL, dir, item.Filename, headers, "")
		if err != nil {
			// Ignore "GID already used" or similar errors
			// logging might be noisy if re-adding existing tasks
			// log.Printf("Failed to add download for %s: %v", item.Filename, err)
			continue
		}

		// Save the actual GID returned by aria2 to database
		s.taskManager.CreateTaskItem(taskID, item.Filename, actualGID, item.URL)
	}
}
