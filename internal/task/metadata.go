package task

import "time"

const (
	TaskStatusPending     = "pending"
	TaskStatusParsing     = "parsing"
	TaskStatusDownloading = "downloading"
	TaskStatusPaused      = "paused"
	TaskStatusCompleted   = "completed"
	TaskStatusFailed      = "failed"
	TaskStatusDeleted     = "deleted"
)

type TaskMetadata struct {
	ID                 string     `json:"id"`
	Name               string     `json:"name"`
	OriginalURL        string     `json:"original_url"`
	TotalSegments      int        `json:"total_segments"`
	DownloadedSegments int        `json:"downloaded_segments"`
	TotalItems         int        `json:"total_items"`
	DoneItems          int        `json:"done_items"`
	FailedItems        int        `json:"failed_items"`
	OutputDir          string     `json:"output_dir"`
	CreatedTime        time.Time  `json:"created_time"`
	UpdatedTime        time.Time  `json:"updated_time"`
	FinishedTime       *time.Time `json:"finished_time,omitempty"`
	Status             string     `json:"status"`
	ProxiedContent     string     `json:"-"`
}

type TaskManifest struct {
	TaskID        string         `json:"tid"`
	OriginalURL   string         `json:"src,omitempty"`
	TotalSegments int            `json:"seg,omitempty"`
	Items         []ManifestItem `json:"items"`
}

type ManifestItem struct {
	Filename string `json:"f"`
	URL      string `json:"u"`
	Type     string `json:"t,omitempty"`
}

type TaskProgressFile struct {
	TaskID             string            `json:"tid"`
	Failed             map[string]string `json:"f,omitempty"`
	DownloadedSegments int               `json:"seg_done,omitempty"`
	DoneItems          int               `json:"done,omitempty"`
	UpdatedAt          time.Time         `json:"u,omitempty"`
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

type AddTaskRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}
