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
	client      *http.Client
	taskManager *task.Manager
}

func NewServer() (*Server, error) {
	aria2 := downloader.NewClient()
	db, err := database.Init(config.GlobalConfig.CacheDir)
	if err != nil {
		return nil, err
	}
	tm, err := task.NewManager(aria2, db)
	if err != nil {
		return nil, err
	}
	return &Server{
		addr: fmt.Sprintf(":%d", config.GlobalConfig.ProxyPort),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		taskManager: tm,
	}, nil
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	mux.HandleFunc("GET /api/v1/tasks", s.taskManager.HandleListV1)
	mux.HandleFunc("POST /api/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		s.taskManager.HandleAdd(w, r, s.startDownloadFromURL)
	})
	mux.HandleFunc("POST /api/v1/tasks/{id}/pause", s.taskManager.HandlePauseV1)
	mux.HandleFunc("POST /api/v1/tasks/{id}/resume", s.taskManager.HandleResumeV1)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", s.taskManager.HandleRetryV1)
	mux.HandleFunc("POST /api/v1/tasks/sync", s.taskManager.HandleSyncProgress)
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", s.taskManager.HandleDeleteV1)

	mux.HandleFunc("/proxy/m3u8/", s.handleM3U8)
	mux.HandleFunc("/proxy/seg/", s.handleSegment)
	mux.HandleFunc("/proxy/key/", s.handleKey)

	log.Printf("Proxy starting at http://localhost%s", s.addr)
	return http.ListenAndServe(s.addr, mux)
}

func (s *Server) startDownloadFromURL(addReq task.AddTaskRequest) error {
	rawURL := addReq.URL
	taskName := strings.TrimSpace(addReq.Name)
	if taskName == "" {
		taskName = task.DeriveTaskName(rawURL)
	}

	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	for key, value := range config.GlobalConfig.Headers {
		req.Header.Set(key, value)
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
	if type_ == playlist.Master {
		masterPl := pl.(*m3u8.MasterPlaylist)
		if len(masterPl.Variants) == 0 {
			return fmt.Errorf("master playlist has no variants")
		}
		best := masterPl.Variants[0]
		for _, variant := range masterPl.Variants {
			if variant.Bandwidth > best.Bandwidth {
				best = variant
			}
		}
		return s.startDownloadFromURL(task.AddTaskRequest{
			Name: taskName,
			URL:  playlist.ResolveURL(base, best.URI),
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
		OutputDir:      cache.GetTaskDir(taskID),
		CreatedTime:    time.Now(),
		UpdatedTime:    time.Now(),
		Status:         task.TaskStatusParsing,
		ProxiedContent: updated,
	}
	created, err := s.taskManager.CreateTaskWithItems(meta, items)
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("task already exists")
	}
	return s.taskManager.UpdateTaskProxiedContent(taskID, updated)
}

func (s *Server) handleM3U8(w http.ResponseWriter, r *http.Request) {
	encodedURL := strings.TrimPrefix(r.URL.Path, "/proxy/m3u8/")
	if encodedURL == "" {
		http.Error(w, "missing url parameter", http.StatusBadRequest)
		return
	}
	originURL, err := url.QueryUnescape(encodedURL)
	if err != nil {
		http.Error(w, "invalid url encoding", http.StatusBadRequest)
		return
	}
	parsedURL, err := url.Parse(originURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		http.Error(w, "invalid url", http.StatusBadRequest)
		return
	}

	taskID := cache.GetTaskID(originURL)
	if content, err := s.taskManager.GetTaskProxiedContent(taskID); err == nil && content != "" {
		writeM3U8(w, content)
		return
	}

	resp, err := s.fetchUpstreamM3U8(originURL)
	if err != nil {
		http.Error(w, "failed to fetch upstream", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	pl, playlistType, err := playlist.Parse(resp.Body)
	if err != nil {
		http.Error(w, "failed to parse m3u8", http.StatusBadGateway)
		return
	}
	proxyBase := fmt.Sprintf("http://%s/proxy", r.Host)
	switch playlistType {
	case playlist.Master:
		writeM3U8(w, playlist.RewriteMaster(pl.(*m3u8.MasterPlaylist), proxyBase, parsedURL))
	case playlist.Variant:
		updated, _, _ := playlist.RewriteVariant(pl.(*m3u8.MediaPlaylist), proxyBase, taskID, parsedURL)
		writeM3U8(w, updated)
	default:
		http.Error(w, "unknown playlist type", http.StatusBadGateway)
	}
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
		return nil, fmt.Errorf("bad upstream status: %d", resp.StatusCode)
	}
	return resp, nil
}

func writeM3U8(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_, _ = w.Write([]byte(content))
}

func (s *Server) handleSegment(w http.ResponseWriter, r *http.Request) {
	s.handleProxyFile(w, r, "/proxy/seg/")
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	s.handleProxyFile(w, r, "/proxy/key/")
}

func (s *Server) handleProxyFile(w http.ResponseWriter, r *http.Request, prefix string) {
	pathValue := strings.TrimPrefix(r.URL.Path, prefix)
	parts := strings.SplitN(pathValue, "/", 3)
	if len(parts) < 3 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	taskID, filename, encodedURL := parts[0], parts[1], parts[2]
	originURL, err := url.QueryUnescape(encodedURL)
	if err != nil {
		http.Error(w, "invalid url encoding", http.StatusBadRequest)
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
		http.Error(w, "failed to fetch upstream", http.StatusBadGateway)
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
