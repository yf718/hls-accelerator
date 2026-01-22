package task

import (
	"time"
)

type TaskMetadata struct {
	ID             string    `json:"id"`
	OriginalURL    string    `json:"original_url"`
	TotalSegments  int       `json:"total_segments"`
	CreatedTime    time.Time `json:"created_time"`
	Status         string    `json:"status"` // "downloading", "completed", "stopped"
	ProxiedContent string    `json:"-"`      // Large field, excluding from JSON list
}
