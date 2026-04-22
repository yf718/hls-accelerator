package task

import (
	"time"
)

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
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	OriginalURL        string    `json:"original_url"`
	TotalSegments      int       `json:"total_segments"`
	DownloadedSegments int       `json:"downloaded_segments"`
	TotalItems         int       `json:"total_items"`
	DoneItems          int       `json:"done_items"`
	FailedItems        int       `json:"failed_items"`
	OutputDir          string    `json:"output_dir"`
	CreatedTime        time.Time `json:"created_time"`
	UpdatedTime        time.Time `json:"updated_time"`
	FinishedTime       *time.Time `json:"finished_time,omitempty"`
	Status             string    `json:"status"`
	Extra              string    `json:"extra,omitempty"`
	ProxiedContent     string    `json:"-"`      // Large field, excluding from JSON list
}
