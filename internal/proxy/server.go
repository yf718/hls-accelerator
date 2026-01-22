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

func (s *Server) handleM3U8(w http.ResponseWriter, r *http.Request) {
	// Path: /proxy/m3u8/{encoded_url}
	encodedURL := strings.TrimPrefix(r.URL.Path, "/proxy/m3u8/")
	originURL, err := url.QueryUnescape(encodedURL)
	if err != nil {
		http.Error(w, "Invalid URL encoding", http.StatusBadRequest)
		return
	}

	// Optimization: Check DB first to avoid unnecessary upstream requests
	taskID := cache.GetTaskID(originURL)
	if content, err := s.taskManager.GetTaskProxiedContent(taskID); err == nil && content != "" {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(content))
		return
	}

	// Fetch
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

	// Parse
	base, _ := url.Parse(originURL)
	pl, type_, err := playlist.Parse(resp.Body)
	if err != nil {
		http.Error(w, "Failed to parse m3u8", http.StatusBadGateway)
		return
	}

	proxyBase := fmt.Sprintf("http://%s/proxy", r.Host)

	if type_ == playlist.Master {
		masterPl := pl.(*m3u8.MasterPlaylist)
		updated := playlist.RewriteMaster(masterPl, proxyBase, base)

		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(updated))
		return
	}

	if type_ == playlist.Variant {
		taskID := cache.GetTaskID(originURL)
		cache.EnsureTaskDir(taskID)

		mediaPl := pl.(*m3u8.MediaPlaylist)
		updated, items, total := playlist.RewriteVariant(mediaPl, proxyBase, taskID, base)

		// Check if task exists to prevent overwriting progress
		shouldStartTask := true
		exists, status, _ := s.taskManager.CheckTaskExists(taskID)
		if exists && (status == "downloading" || status == "completed") {
			shouldStartTask = false
			// Task exists but no content in DB? We should update it.
			go s.taskManager.UpdateTaskProxiedContent(taskID, updated)
		}

		if shouldStartTask {
			// Save/Update Metadata (Player triggered)
			go func() {
				meta := task.TaskMetadata{
					ID:             taskID,
					OriginalURL:    originURL,
					TotalSegments:  total,
					CreatedTime:    time.Now(),
					Status:         "downloading",
					ProxiedContent: updated,
				}
				s.taskManager.CreateTask(meta)
			}()

			// Trigger Downloads
			go s.triggerDownloads(taskID, items)
		}

		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(updated))
		return
	}
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
