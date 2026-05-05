package task

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"hls-accelerator/internal/config"
)

func isSQLiteUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint failed") ||
		strings.Contains(msg, "constraint failed") ||
		strings.Contains(msg, "sql constraint")
}

func (m *Manager) InitTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL DEFAULT '',
		original_url TEXT NOT NULL DEFAULT '',
		total_segments INTEGER NOT NULL DEFAULT 0,
		downloaded_segments INTEGER NOT NULL DEFAULT 0,
		total_items INTEGER NOT NULL DEFAULT 0,
		done_items INTEGER NOT NULL DEFAULT 0,
		failed_items INTEGER NOT NULL DEFAULT 0,
		output_dir TEXT NOT NULL DEFAULT '',
		m3u8_file_path TEXT NOT NULL DEFAULT '',
		created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		finished_time DATETIME,
		status TEXT NOT NULL DEFAULT 'pending',
		proxied_content TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS task_manifest (
		task_id TEXT NOT NULL,
		seq INTEGER NOT NULL,
		filename TEXT NOT NULL,
		url TEXT NOT NULL DEFAULT '',
		item_type TEXT NOT NULL DEFAULT 'segment',
		PRIMARY KEY (task_id, filename)
	);

	CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
	CREATE INDEX IF NOT EXISTS idx_task_manifest_task_seq ON task_manifest(task_id, seq);
	`
	_, err := m.db.Exec(query)
	return err
}

func (m *Manager) CreateTask(meta TaskMetadata) error {
	_, err := m.db.Exec(`
	INSERT INTO tasks (
		id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir, m3u8_file_path,
		created_time, updated_time, finished_time, status, proxied_content
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		meta.ID,
		meta.Name,
		meta.OriginalURL,
		meta.TotalSegments,
		meta.DownloadedSegments,
		meta.TotalItems,
		meta.DoneItems,
		meta.FailedItems,
		meta.OutputDir,
		meta.M3U8FilePath,
		nonZeroTime(meta.CreatedTime),
		nonZeroTime(meta.UpdatedTime),
		meta.FinishedTime,
		defaultTaskStatus(meta.Status),
		meta.ProxiedContent,
	)
	return err
}

func (m *Manager) TryCreateTask(meta TaskMetadata) (bool, error) {
	err := m.CreateTask(meta)
	if err == nil {
		return true, nil
	}
	if isSQLiteUniqueConstraintError(err) {
		return false, nil
	}
	return false, err
}

func (m *Manager) GetTask(id string) (*TaskMetadata, error) {
	var meta TaskMetadata
	var finished sql.NullTime
	err := m.db.QueryRow(`
	SELECT id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir, m3u8_file_path,
		created_time, updated_time, finished_time, status
	FROM tasks
	WHERE id = ?
	`, id).Scan(
		&meta.ID,
		&meta.Name,
		&meta.OriginalURL,
		&meta.TotalSegments,
		&meta.DownloadedSegments,
		&meta.TotalItems,
		&meta.DoneItems,
		&meta.FailedItems,
		&meta.OutputDir,
		&meta.M3U8FilePath,
		&meta.CreatedTime,
		&meta.UpdatedTime,
		&finished,
		&meta.Status,
	)
	if err != nil {
		return nil, err
	}
	if finished.Valid {
		meta.FinishedTime = &finished.Time
	}
	return &meta, nil
}

func (m *Manager) GetTaskProxiedContent(id string) (string, error) {
	var content string
	err := m.db.QueryRow(`SELECT proxied_content FROM tasks WHERE id = ?`, id).Scan(&content)
	return content, err
}

func (m *Manager) UpdateTaskProxiedContent(id, content string) error {
	_, err := m.db.Exec(`UPDATE tasks SET proxied_content = ?, updated_time = datetime('now') WHERE id = ?`, content, id)
	return err
}

func (m *Manager) UpdateTaskSnapshot(taskID, status string, doneItems, downloadedSegments, failedItems int) error {
	_, err := m.db.Exec(`
	UPDATE tasks
	SET status = ?,
		done_items = ?,
		downloaded_segments = ?,
		failed_items = ?,
		updated_time = datetime('now'),
		finished_time = CASE WHEN ? = ? THEN COALESCE(finished_time, datetime('now')) ELSE NULL END
	WHERE id = ?
	`, status, doneItems, downloadedSegments, failedItems, status, TaskStatusCompleted, taskID)
	return err
}

func (m *Manager) UpdateTaskStatus(taskID, status string) error {
	_, err := m.db.Exec(`
	UPDATE tasks
	SET status = ?, updated_time = datetime('now'),
		finished_time = CASE WHEN ? = ? THEN COALESCE(finished_time, datetime('now')) ELSE finished_time END
	WHERE id = ?
	`, status, status, TaskStatusCompleted, taskID)
	return err
}

func (m *Manager) ListTasksDB() ([]TaskMetadata, error) {
	rows, err := m.db.Query(`
	SELECT id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir, m3u8_file_path,
		created_time, updated_time, finished_time, status
	FROM tasks
	WHERE status != ?
	ORDER BY created_time DESC
	`, TaskStatusDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskMetadata
	for rows.Next() {
		var meta TaskMetadata
		var finished sql.NullTime
		if err := rows.Scan(
			&meta.ID,
			&meta.Name,
			&meta.OriginalURL,
			&meta.TotalSegments,
			&meta.DownloadedSegments,
			&meta.TotalItems,
			&meta.DoneItems,
			&meta.FailedItems,
			&meta.OutputDir,
			&meta.M3U8FilePath,
			&meta.CreatedTime,
			&meta.UpdatedTime,
			&finished,
			&meta.Status,
		); err != nil {
			return nil, err
		}
		if finished.Valid {
			meta.FinishedTime = &finished.Time
		}
		tasks = append(tasks, meta)
	}
	return tasks, rows.Err()
}

func (m *Manager) SaveTaskManifest(manifest TaskManifest) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.Exec(`DELETE FROM task_manifest WHERE task_id = ?`, manifest.TaskID); err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
	INSERT INTO task_manifest (task_id, seq, filename, url, item_type)
	VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for index, item := range manifest.Items {
		if _, err = stmt.Exec(manifest.TaskID, index, item.Filename, item.URL, normalizeManifestType(item.Type)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (m *Manager) LoadTaskManifest(taskID, originalURL string, totalSegments int) (TaskManifest, error) {
	rows, err := m.db.Query(`
	SELECT filename, url, item_type
	FROM task_manifest
	WHERE task_id = ?
	ORDER BY seq ASC
	`, taskID)
	if err != nil {
		return TaskManifest{}, err
	}
	defer rows.Close()

	items := make([]ManifestItem, 0)
	for rows.Next() {
		var item ManifestItem
		if err := rows.Scan(&item.Filename, &item.URL, &item.Type); err != nil {
			return TaskManifest{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return TaskManifest{}, err
	}

	return TaskManifest{
		TaskID:        taskID,
		OriginalURL:   originalURL,
		TotalSegments: totalSegments,
		Items:         items,
	}, nil
}

func (m *Manager) LoadTaskManifestIndex(taskID string) ([]ManifestIndexItem, error) {
	rows, err := m.db.Query(`
	SELECT seq, filename, item_type
	FROM task_manifest
	WHERE task_id = ?
	ORDER BY seq ASC
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]ManifestIndexItem, 0)
	for rows.Next() {
		var (
			seq      uint32
			filename string
			itemType string
		)
		if err := rows.Scan(&seq, &filename, &itemType); err != nil {
			return nil, err
		}
		out = append(out, ManifestIndexItem{
			Seq:       seq,
			Filename:  filename,
			IsSegment: normalizeManifestType(itemType) != "key",
		})
	}
	return out, rows.Err()
}

func (m *Manager) LoadManifestItemsByFilenames(taskID string, filenames []string) (map[string]ManifestItem, error) {
	if len(filenames) == 0 {
		return map[string]ManifestItem{}, nil
	}
	placeholders := make([]string, 0, len(filenames))
	args := make([]interface{}, 0, len(filenames)+1)
	args = append(args, taskID)
	for _, filename := range filenames {
		placeholders = append(placeholders, "?")
		args = append(args, filename)
	}

	rows, err := m.db.Query(fmt.Sprintf(`
	SELECT filename, url, item_type
	FROM task_manifest
	WHERE task_id = ? AND filename IN (%s)
	`, strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]ManifestItem, len(filenames))
	for rows.Next() {
		var item ManifestItem
		if err := rows.Scan(&item.Filename, &item.URL, &item.Type); err != nil {
			return nil, err
		}
		item.Type = normalizeManifestType(item.Type)
		out[item.Filename] = item
	}
	return out, rows.Err()
}

func (m *Manager) DeleteTaskDB(id string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.Exec(`DELETE FROM task_manifest WHERE task_id = ?`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM tasks WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *Manager) CheckTaskExists(id string) (bool, string, error) {
	var status string
	err := m.db.QueryRow(`SELECT status FROM tasks WHERE id = ?`, id).Scan(&status)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	return err == nil, status, err
}

func (m *Manager) GetTasksByStatus(status string) ([]TaskMetadata, error) {
	return m.GetTasksByStatuses(status)
}

func (m *Manager) GetTasksByStatuses(statuses ...string) ([]TaskMetadata, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(statuses))
	args := make([]interface{}, len(statuses))
	for i, status := range statuses {
		placeholders[i] = "?"
		args[i] = status
	}
	query := fmt.Sprintf(`
	SELECT id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir, m3u8_file_path,
		created_time, updated_time, finished_time, status
	FROM tasks
	WHERE status IN (%s)
	ORDER BY created_time DESC
	`, strings.Join(placeholders, ","))

	rows, err := m.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskMetadata
	for rows.Next() {
		var meta TaskMetadata
		var finished sql.NullTime
		if err := rows.Scan(
			&meta.ID,
			&meta.Name,
			&meta.OriginalURL,
			&meta.TotalSegments,
			&meta.DownloadedSegments,
			&meta.TotalItems,
			&meta.DoneItems,
			&meta.FailedItems,
			&meta.OutputDir,
			&meta.M3U8FilePath,
			&meta.CreatedTime,
			&meta.UpdatedTime,
			&finished,
			&meta.Status,
		); err != nil {
			return nil, err
		}
		if finished.Valid {
			meta.FinishedTime = &finished.Time
		}
		tasks = append(tasks, meta)
	}
	return tasks, rows.Err()
}

func nonZeroTime(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Now()
	}
	return ts
}

func defaultTaskStatus(status string) string {
	if strings.TrimSpace(status) == "" {
		return TaskStatusPending
	}
	return status
}

func (m *Manager) SaveTaskM3U8File(taskName, content string) (string, error) {
	storeDir := strings.TrimSpace(config.GlobalConfig.M3U8StoreDir)
	if storeDir == "" || strings.TrimSpace(content) == "" {
		return "", nil
	}
	info, err := os.Stat(storeDir)
	if err != nil || !info.IsDir() {
		return "", nil
	}

	baseName := sanitizeFileName(strings.TrimSpace(taskName))
	if baseName == "" {
		baseName = "Untitled Task"
	}

	usedSuffixes, err := m.listUsedM3U8Suffixes(storeDir, baseName)
	if err != nil {
		return "", err
	}

	suffix := 0
	for {
		if _, taken := usedSuffixes[suffix]; !taken {
			break
		}
		suffix++
	}

	fileBase := baseName
	if suffix > 0 {
		fileBase = fmt.Sprintf("%s(%d)", baseName, suffix)
	}
	fullPath := filepath.Join(storeDir, fileBase+".m3u8")
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return "", err
	}
	return fullPath, nil
}

func (m *Manager) listUsedM3U8Suffixes(storeDir, baseName string) (map[int]struct{}, error) {
	prefixPath := filepath.Join(storeDir, baseName)
	likePattern := prefixPath + "%.m3u8"

	rows, err := m.db.Query(`
	SELECT m3u8_file_path FROM tasks
	WHERE status != ? AND m3u8_file_path LIKE ?
	`, TaskStatusDeleted, likePattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[int]struct{}{}
	for rows.Next() {
		var fullPath string
		if err := rows.Scan(&fullPath); err != nil {
			return nil, err
		}
		name := strings.TrimSuffix(filepath.Base(fullPath), ".m3u8")
		if name == baseName {
			out[0] = struct{}{}
			continue
		}
		if !strings.HasPrefix(name, baseName+"(") || !strings.HasSuffix(name, ")") {
			continue
		}
		num := strings.TrimSuffix(strings.TrimPrefix(name, baseName+"("), ")")
		n, err := strconv.Atoi(num)
		if err != nil || n <= 0 {
			continue
		}
		out[n] = struct{}{}
	}
	return out, rows.Err()
}

func sanitizeFileName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	name = replacer.Replace(name)
	name = strings.TrimSpace(name)
	name = strings.TrimRight(name, ".")
	if name == "" {
		return ""
	}
	return name
}
