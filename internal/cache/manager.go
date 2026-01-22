package cache

import (
	"crypto/md5"
	"encoding/hex"
	"hls-accelerator/internal/config"
	"os"
	"path/filepath"
)

// GetTaskID generates a unique ID for a task based on its URL
func GetTaskID(url string) string {
	hash := md5.Sum([]byte(url))
	return hex.EncodeToString(hash[:])
}

// GetTaskDir returns the absolute path to the task's cache directory
func GetTaskDir(taskID string) string {
	path, _ := filepath.Abs(filepath.Join(config.GlobalConfig.CacheDir, taskID))
	return path
}

// EnsureTaskDir creates the task directory if it doesn't exist
func EnsureTaskDir(taskID string) error {
	path := GetTaskDir(taskID)
	return os.MkdirAll(path, 0755)
}

// FileExists checks if a file exists in the task's cache
func FileExists(taskID, filename string) bool {
	path := filepath.Join(GetTaskDir(taskID), filename)
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// GetFilePath returns the full path for a file in the task's cache
func GetFilePath(taskID, filename string) string {
	return filepath.Join(GetTaskDir(taskID), filename)
}
