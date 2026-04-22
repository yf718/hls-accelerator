package task

import (
	"database/sql"
	"fmt"
	playlist "hls-accelerator/internal/m3u8"
	"path/filepath"
	"strings"
	"time"
)

const (
	taskItemStatusPending    = "pending"
	taskItemStatusSubmitting = "submitting"
	taskItemStatusQueued     = "queued"
	taskItemStatusDownloading = "downloading"
	taskItemStatusPaused     = "paused"
	taskItemStatusCompleted  = "completed"
	taskItemStatusFailed     = "failed"
	taskItemStatusRemoved    = "removed"
)

type TaskItem struct {
	ID            int64      `json:"id"`
	TaskID        string     `json:"task_id"`
	Seq           int        `json:"seq"`
	Filename      string     `json:"filename"`
	Aria2GID      string     `json:"aria2_gid"`
	URL           string     `json:"url"`
	Status        string     `json:"status"`
	ItemType      string     `json:"item_type"`
	FilePath      string     `json:"file_path"`
	FileSize      int64      `json:"file_size"`
	RetryCount    int        `json:"retry_count"`
	LastError     string     `json:"last_error"`
	CreatedTime   time.Time  `json:"created_time"`
	UpdatedTime   time.Time  `json:"updated_time"`
	CompletedTime *time.Time `json:"completed_time,omitempty"`
}

type TaskStatusCounts struct {
	TotalItems         int
	PendingItems       int
	QueuedItems        int
	DownloadingItems   int
	PausedItems        int
	CompletedItems     int
	FailedItems        int
	RemovedItems       int
	CompletedSegments  int
	FailedSegments     int
}

func isSQLiteBusyOrLocked(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "sqlite_busy") ||
		strings.Contains(msg, "busy")
}

func sleepForBusyRetry(attempt int) {
	d := time.Duration(25*(1<<attempt)) * time.Millisecond
	if d > 400*time.Millisecond {
		d = 400 * time.Millisecond
	}
	time.Sleep(d)
}

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
		extra TEXT NOT NULL DEFAULT '{}',
		created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		finished_time DATETIME,
		status TEXT NOT NULL DEFAULT 'pending',
		proxied_content TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS task_item (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		seq INTEGER NOT NULL DEFAULT 0,
		filename TEXT NOT NULL,
		aria2_gid TEXT,
		url TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		completed_time DATETIME,
		created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		item_type TEXT NOT NULL DEFAULT 'segment',
		last_error TEXT NOT NULL DEFAULT '',
		retry_count INTEGER NOT NULL DEFAULT 0,
		updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		file_path TEXT NOT NULL DEFAULT '',
		file_size INTEGER NOT NULL DEFAULT 0,
		UNIQUE(task_id, filename),
		FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_task_status ON tasks(status);
	CREATE INDEX IF NOT EXISTS idx_task_item_task_id ON task_item(task_id);
	CREATE INDEX IF NOT EXISTS idx_task_item_gid ON task_item(aria2_gid);
	CREATE INDEX IF NOT EXISTS idx_task_item_task_status ON task_item(task_id, status);
	`
	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	columns := []struct {
		table string
		name  string
		def   string
	}{
		{"tasks", "name", "TEXT NOT NULL DEFAULT ''"},
		{"tasks", "downloaded_segments", "INTEGER NOT NULL DEFAULT 0"},
		{"tasks", "total_items", "INTEGER NOT NULL DEFAULT 0"},
		{"tasks", "done_items", "INTEGER NOT NULL DEFAULT 0"},
		{"tasks", "failed_items", "INTEGER NOT NULL DEFAULT 0"},
		{"tasks", "output_dir", "TEXT NOT NULL DEFAULT ''"},
		{"tasks", "extra", "TEXT NOT NULL DEFAULT '{}'"},
		{"tasks", "updated_time", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"},
		{"tasks", "finished_time", "DATETIME"},
		{"tasks", "proxied_content", "TEXT NOT NULL DEFAULT ''"},
		{"task_item", "seq", "INTEGER NOT NULL DEFAULT 0"},
		{"task_item", "status", "TEXT NOT NULL DEFAULT 'pending'"},
		{"task_item", "completed_time", "DATETIME"},
		{"task_item", "item_type", "TEXT NOT NULL DEFAULT 'segment'"},
		{"task_item", "last_error", "TEXT NOT NULL DEFAULT ''"},
		{"task_item", "retry_count", "INTEGER NOT NULL DEFAULT 0"},
		{"task_item", "updated_time", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"},
		{"task_item", "file_path", "TEXT NOT NULL DEFAULT ''"},
		{"task_item", "file_size", "INTEGER NOT NULL DEFAULT 0"},
	}
	for _, column := range columns {
		if err := m.ensureColumn(column.table, column.name, column.def); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) CreateTask(meta TaskMetadata) error {
	query := `
	INSERT INTO tasks (
		id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir, extra,
		created_time, updated_time, finished_time, status, proxied_content
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := m.db.Exec(
		query,
		meta.ID,
		meta.Name,
		meta.OriginalURL,
		meta.TotalSegments,
		meta.DownloadedSegments,
		meta.TotalItems,
		meta.DoneItems,
		meta.FailedItems,
		meta.OutputDir,
		emptyJSON(meta.Extra),
		meta.CreatedTime,
		nonZeroTime(meta.UpdatedTime, meta.CreatedTime),
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
	query := `
	SELECT id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir, created_time,
		updated_time, finished_time, status, extra
	FROM tasks
	WHERE id = ?
	`
	row := m.db.QueryRow(query, id)
	var meta TaskMetadata
	var finished sql.NullTime
	if err := row.Scan(
		&meta.ID,
		&meta.Name,
		&meta.OriginalURL,
		&meta.TotalSegments,
		&meta.DownloadedSegments,
		&meta.TotalItems,
		&meta.DoneItems,
		&meta.FailedItems,
		&meta.OutputDir,
		&meta.CreatedTime,
		&meta.UpdatedTime,
		&finished,
		&meta.Status,
		&meta.Extra,
	); err != nil {
		return nil, err
	}
	if finished.Valid {
		meta.FinishedTime = &finished.Time
	}
	return &meta, nil
}

func (m *Manager) GetTaskProxiedContent(id string) (string, error) {
	query := `SELECT proxied_content FROM tasks WHERE id = ?`
	var content sql.NullString
	if err := m.db.QueryRow(query, id).Scan(&content); err != nil {
		return "", err
	}
	return content.String, nil
}

func (m *Manager) UpdateTaskProxiedContent(id, content string) error {
	query := `UPDATE tasks SET proxied_content = ?, updated_time = datetime('now') WHERE id = ?`
	_, err := m.db.Exec(query, content, id)
	return err
}

func (m *Manager) UpdateTaskStatus(id, status string) error {
	query := `
	UPDATE tasks
	SET status = ?,
		updated_time = datetime('now'),
		finished_time = CASE WHEN ? = ? THEN COALESCE(finished_time, datetime('now')) ELSE NULL END
	WHERE id = ?
	`
	_, err := m.db.Exec(query, status, status, TaskStatusCompleted, id)
	return err
}

func (m *Manager) UpdateTaskDownloadedSegments(id string, downloaded int) error {
	query := `UPDATE tasks SET downloaded_segments = ?, done_items = ?, updated_time = datetime('now') WHERE id = ?`
	_, err := m.db.Exec(query, downloaded, downloaded, id)
	return err
}

func (m *Manager) ListTasksDB() ([]TaskMetadata, error) {
	query := `
	SELECT id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir, created_time,
		updated_time, finished_time, status, extra
	FROM tasks
	WHERE status != ?
	ORDER BY created_time DESC
	`
	rows, err := m.db.Query(query, TaskStatusDeleted)
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
			&meta.CreatedTime,
			&meta.UpdatedTime,
			&finished,
			&meta.Status,
			&meta.Extra,
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

func (m *Manager) DeleteTaskDB(id string) error {
	_, err := m.db.Exec(`DELETE FROM tasks WHERE id = ?`, id)
	return err
}

func (m *Manager) CheckTaskExists(id string) (bool, string, error) {
	var status string
	err := m.db.QueryRow(`SELECT status FROM tasks WHERE id = ?`, id).Scan(&status)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, status, nil
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
		total_items, done_items, failed_items, output_dir, created_time,
		updated_time, finished_time, status, extra
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
			&meta.CreatedTime,
			&meta.UpdatedTime,
			&finished,
			&meta.Status,
			&meta.Extra,
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

func (m *Manager) CreateTaskItem(taskID, filename, aria2GID, url, itemType string) error {
	query := `
	INSERT INTO task_item (task_id, seq, filename, aria2_gid, url, item_type, status, created_time, updated_time, file_path)
	VALUES (?, 0, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), ?)
	ON CONFLICT(task_id, filename) DO UPDATE SET
		aria2_gid = excluded.aria2_gid,
		url = excluded.url,
		item_type = excluded.item_type,
		status = excluded.status,
		last_error = '',
		updated_time = datetime('now'),
		file_path = excluded.file_path
	`
	_, err := m.db.Exec(query, taskID, filename, emptyStringToNullString(aria2GID), url, normalizeItemType(itemType), taskItemStatusQueued, guessFilePathForTask(taskID, filename))
	if err != nil {
		return err
	}
	return m.AggregateTask(taskID)
}

func (m *Manager) CreateTaskItemsPlaceholders(taskID string, items []playlist.DownloadItem) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
	INSERT INTO task_item (task_id, seq, filename, aria2_gid, url, item_type, status, created_time, updated_time, last_error, file_path)
	VALUES (?, ?, ?, NULL, ?, ?, ?, datetime('now'), datetime('now'), '', ?)
	ON CONFLICT(task_id, filename) DO UPDATE SET
		seq = excluded.seq,
		url = excluded.url,
		item_type = excluded.item_type,
		file_path = excluded.file_path,
		status = CASE
			WHEN task_item.status IN ('pending', 'failed', 'paused', 'removed') THEN 'pending'
			ELSE task_item.status
		END,
		aria2_gid = CASE
			WHEN task_item.status IN ('pending', 'failed', 'paused', 'removed') THEN NULL
			ELSE task_item.aria2_gid
		END,
		last_error = CASE
			WHEN task_item.status IN ('pending', 'failed', 'paused', 'removed') THEN ''
			ELSE task_item.last_error
		END,
		updated_time = datetime('now')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	lastByFilename := make(map[string]playlist.DownloadItem, len(items))
	seqByFilename := make(map[string]int, len(items))
	for idx, item := range items {
		lastByFilename[item.Filename] = item
		seqByFilename[item.Filename] = idx
	}
	for _, item := range items {
		current, ok := lastByFilename[item.Filename]
		if !ok {
			continue
		}
		delete(lastByFilename, item.Filename)
		item = current
		if _, err := stmt.Exec(
			taskID,
			seqByFilename[item.Filename],
			item.Filename,
			item.URL,
			normalizeItemType(item.Type),
			taskItemStatusPending,
			guessFilePathForTask(taskID, item.Filename),
		); err != nil {
			return err
		}
	}

	var totalItems int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM task_item WHERE task_id = ? AND status != ?`, taskID, taskItemStatusRemoved).Scan(&totalItems); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE tasks SET total_items = ?, updated_time = datetime('now') WHERE id = ?`, totalItems, taskID); err != nil {
		return err
	}
	if err := m.aggregateTaskInTx(tx, taskID); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *Manager) ListPendingTaskItems(taskID string) ([]TaskItem, error) {
	rows, err := m.db.Query(`
	SELECT id, task_id, seq, filename, COALESCE(aria2_gid, ''), url, status,
		COALESCE(item_type, 'segment'), COALESCE(file_path, ''), COALESCE(file_size, 0),
		retry_count, COALESCE(last_error, ''), created_time, updated_time, completed_time
	FROM task_item
	WHERE task_id = ? AND status = ?
	ORDER BY CASE WHEN item_type = 'key' THEN -1 ELSE seq END, id
	`, taskID, taskItemStatusPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTaskItems(rows)
}

func (m *Manager) ListTaskItemsByTask(taskID, status string) ([]TaskItem, error) {
	base := `
	SELECT id, task_id, seq, filename, COALESCE(aria2_gid, ''), url, status,
		COALESCE(item_type, 'segment'), COALESCE(file_path, ''), COALESCE(file_size, 0),
		retry_count, COALESCE(last_error, ''), created_time, updated_time, completed_time
	FROM task_item
	WHERE task_id = ?
	`
	args := []interface{}{taskID}
	if status != "" {
		base += ` AND status = ?`
		args = append(args, status)
	}
	base += ` ORDER BY seq, id`
	rows, err := m.db.Query(base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTaskItems(rows)
}

func (m *Manager) GetTaskItem(taskID string, itemID int64) (*TaskItem, error) {
	rows, err := m.db.Query(`
	SELECT id, task_id, seq, filename, COALESCE(aria2_gid, ''), url, status,
		COALESCE(item_type, 'segment'), COALESCE(file_path, ''), COALESCE(file_size, 0),
		retry_count, COALESCE(last_error, ''), created_time, updated_time, completed_time
	FROM task_item
	WHERE task_id = ? AND id = ?
	`, taskID, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanTaskItems(rows)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, sql.ErrNoRows
	}
	return &items[0], nil
}

func (m *Manager) ListActiveTaskItemsWithGID() ([]TaskItem, error) {
	rows, err := m.db.Query(`
	SELECT id, task_id, seq, filename, COALESCE(aria2_gid, ''), url, status,
		COALESCE(item_type, 'segment'), COALESCE(file_path, ''), COALESCE(file_size, 0),
		retry_count, COALESCE(last_error, ''), created_time, updated_time, completed_time
	FROM task_item
	WHERE aria2_gid IS NOT NULL AND aria2_gid != ''
		AND status IN (?, ?, ?, ?)
	ORDER BY task_id, seq, id
	`, taskItemStatusQueued, taskItemStatusDownloading, taskItemStatusPaused, taskItemStatusSubmitting)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTaskItems(rows)
}

func (m *Manager) MarkTaskItemSubmitting(taskID, filename string) (bool, error) {
	const maxAttempts = 8
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		res, err := m.db.Exec(`
		UPDATE task_item
		SET status = ?, updated_time = datetime('now')
		WHERE task_id = ? AND filename = ? AND status = ?
		`, taskItemStatusSubmitting, taskID, filename, taskItemStatusPending)
		if err != nil {
			lastErr = err
			if !isSQLiteBusyOrLocked(err) {
				return false, err
			}
			sleepForBusyRetry(attempt)
			continue
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return false, err
		}
		if affected > 0 {
			_ = m.AggregateTask(taskID)
		}
		return affected > 0, nil
	}
	return false, fmt.Errorf("mark task item submitting busy after %d attempts: %w", maxAttempts, lastErr)
}

func (m *Manager) BindTaskItemToAria2(taskID, filename, gid string) error {
	res, err := m.db.Exec(`
	UPDATE task_item
	SET aria2_gid = ?, status = ?, last_error = '', updated_time = datetime('now')
	WHERE task_id = ? AND filename = ? AND status = ?
	`, emptyStringToNullString(gid), taskItemStatusQueued, taskID, filename, taskItemStatusSubmitting)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("task item not in submitting state: task=%s file=%s", taskID, filename)
	}
	return m.AggregateTask(taskID)
}

func (m *Manager) MarkTaskItemSubmitFailed(taskID, filename, errMsg string) error {
	_, err := m.db.Exec(`
	UPDATE task_item
	SET aria2_gid = NULL,
		status = ?,
		last_error = ?,
		retry_count = retry_count + 1,
		updated_time = datetime('now')
	WHERE task_id = ? AND filename = ? AND status = ?
	`, taskItemStatusFailed, errMsg, taskID, filename, taskItemStatusSubmitting)
	if err != nil {
		return err
	}
	return m.AggregateTask(taskID)
}

func (m *Manager) ResetFailedTaskItemsToPending(taskID string) (int, error) {
	res, err := m.db.Exec(`
	UPDATE task_item
	SET status = ?,
		aria2_gid = NULL,
		last_error = '',
		updated_time = datetime('now')
	WHERE task_id = ? AND status = ?
	`, taskItemStatusPending, taskID, taskItemStatusFailed)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected > 0 {
		_ = m.AggregateTask(taskID)
	}
	return int(affected), nil
}

func (m *Manager) ResetQueuedTaskItemsToPending(taskID string) (int, error) {
	res, err := m.db.Exec(`
	UPDATE task_item
	SET status = ?,
		aria2_gid = NULL,
		last_error = '',
		updated_time = datetime('now')
	WHERE task_id = ? AND status IN (?, ?, ?, ?)
	`, taskItemStatusPending, taskID, taskItemStatusQueued, taskItemStatusSubmitting, taskItemStatusDownloading, taskItemStatusPaused)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected > 0 {
		_ = m.AggregateTask(taskID)
	}
	return int(affected), nil
}

func (m *Manager) ResetTaskItemToPending(taskID, filename string) error {
	_, err := m.db.Exec(`
	UPDATE task_item
	SET status = ?,
		aria2_gid = NULL,
		last_error = '',
		updated_time = datetime('now')
	WHERE task_id = ? AND filename = ? AND status IN (?, ?, ?, ?)
	`, taskItemStatusPending, taskID, filename, taskItemStatusSubmitting, taskItemStatusQueued, taskItemStatusDownloading, taskItemStatusPaused)
	if err != nil {
		return err
	}
	return m.AggregateTask(taskID)
}

func (m *Manager) ResetTaskItemToPaused(taskID, filename string) error {
	_, err := m.db.Exec(`
	UPDATE task_item
	SET status = ?,
		last_error = '',
		updated_time = datetime('now')
	WHERE task_id = ? AND filename = ? AND status IN (?, ?, ?, ?)
	`, taskItemStatusPaused, taskID, filename, taskItemStatusSubmitting, taskItemStatusQueued, taskItemStatusDownloading, taskItemStatusPending)
	if err != nil {
		return err
	}
	return m.AggregateTask(taskID)
}

func (m *Manager) RecoverSubmittingTaskItems() error {
	_, err := m.db.Exec(`
	UPDATE task_item
	SET status = ?,
		last_error = CASE WHEN last_error = '' THEN 'recovered after restart' ELSE last_error END,
		updated_time = datetime('now')
	WHERE status = ?
	`, taskItemStatusPending, taskItemStatusSubmitting)
	return err
}

func (m *Manager) GetTaskItemGIDs(taskID string) ([]string, error) {
	rows, err := m.db.Query(`SELECT aria2_gid FROM task_item WHERE task_id = ? AND aria2_gid IS NOT NULL AND aria2_gid != ''`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var gids []string
	for rows.Next() {
		var gid string
		if err := rows.Scan(&gid); err != nil {
			return nil, err
		}
		gids = append(gids, gid)
	}
	return gids, rows.Err()
}

func (m *Manager) GetTaskItems(taskID string) ([]TaskItem, error) {
	return m.ListTaskItemsByTask(taskID, "")
}

func (m *Manager) GetIncompleteTaskItems(taskID string) ([]TaskItem, error) {
	rows, err := m.db.Query(`
	SELECT id, task_id, seq, filename, COALESCE(aria2_gid, ''), url, status,
		COALESCE(item_type, 'segment'), COALESCE(file_path, ''), COALESCE(file_size, 0),
		retry_count, COALESCE(last_error, ''), created_time, updated_time, completed_time
	FROM task_item
	WHERE task_id = ? AND status NOT IN (?, ?)
	ORDER BY seq, id
	`, taskID, taskItemStatusCompleted, taskItemStatusRemoved)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTaskItems(rows)
}

func (m *Manager) MarkTaskItemCompletedByGID(gid string) (string, bool, error) {
	return m.UpdateTaskItemStateByGID(gid, taskItemStatusCompleted, "", "", 0)
}

func (m *Manager) MarkTaskItemCompletedByFilename(taskID, filename string) (bool, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`
	UPDATE task_item
	SET status = ?,
		aria2_gid = NULL,
		completed_time = datetime('now'),
		updated_time = datetime('now'),
		last_error = ''
	WHERE task_id = ? AND filename = ? AND status NOT IN (?, ?)
	`, taskItemStatusCompleted, taskID, filename, taskItemStatusCompleted, taskItemStatusRemoved)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := m.aggregateTaskInTx(tx, taskID); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Manager) UpdateTaskItemStateByGID(gid, status, errMsg, filePath string, fileSize int64) (string, bool, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	var taskID string
	if err := tx.QueryRow(`SELECT task_id FROM task_item WHERE aria2_gid = ?`, gid).Scan(&taskID); err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	args := []interface{}{status, filePath, fileSize, errMsg, gid}
	query := `
	UPDATE task_item
	SET status = ?,
		file_path = CASE WHEN ? != '' THEN ? ELSE file_path END,
		file_size = CASE WHEN ? > 0 THEN ? ELSE file_size END,
		last_error = ?,
		updated_time = datetime('now'),
		completed_time = CASE WHEN ? = ? THEN datetime('now') ELSE completed_time END,
		retry_count = CASE WHEN ? = ? THEN retry_count + 1 ELSE retry_count END
	WHERE aria2_gid = ? AND status NOT IN (?, ?)
	`
	args = []interface{}{
		status,
		filePath, filePath,
		fileSize, fileSize,
		errMsg,
		status, taskItemStatusCompleted,
		status, taskItemStatusFailed,
		gid,
		status,
		taskItemStatusRemoved,
	}
	res, err := tx.Exec(query, args...)
	if err != nil {
		return "", false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return "", false, err
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return "", false, err
		}
		return taskID, false, nil
	}
	if err := m.aggregateTaskInTx(tx, taskID); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return taskID, true, nil
}

func (m *Manager) UpdateTaskItemsStateByIDs(taskID string, ids []int64, fromStatuses []string, toStatus string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	idPlaceholders := make([]string, len(ids))
	args := make([]interface{}, 0, len(ids)+len(fromStatuses)+2)
	args = append(args, toStatus)
	for i, id := range ids {
		idPlaceholders[i] = "?"
		args = append(args, id)
	}
	statusPlaceholders := make([]string, len(fromStatuses))
	for i, status := range fromStatuses {
		statusPlaceholders[i] = "?"
		args = append(args, status)
	}
	query := fmt.Sprintf(`
	UPDATE task_item
	SET status = ?, updated_time = datetime('now')
	WHERE task_id = ? AND id IN (%s) AND status IN (%s)
	`, strings.Join(idPlaceholders, ","), strings.Join(statusPlaceholders, ","))
	args = append([]interface{}{toStatus, taskID}, args[1:]...)
	res, err := m.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected > 0 {
		_ = m.AggregateTask(taskID)
	}
	return int(affected), nil
}

func (m *Manager) CountTaskItemsByStatus(taskID string) (TaskStatusCounts, error) {
	var counts TaskStatusCounts
	err := m.db.QueryRow(`
	SELECT
		COUNT(*) AS total_items,
		SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_items,
		SUM(CASE WHEN status IN ('queued', 'submitting') THEN 1 ELSE 0 END) AS queued_items,
		SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END) AS downloading_items,
		SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END) AS paused_items,
		SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_items,
		SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_items,
		SUM(CASE WHEN status = 'removed' THEN 1 ELSE 0 END) AS removed_items,
		SUM(CASE WHEN status = 'completed' AND item_type != 'key' THEN 1 ELSE 0 END) AS completed_segments,
		SUM(CASE WHEN status = 'failed' AND item_type != 'key' THEN 1 ELSE 0 END) AS failed_segments
	FROM task_item
	WHERE task_id = ?
	`, taskID).Scan(
		&counts.TotalItems,
		&counts.PendingItems,
		&counts.QueuedItems,
		&counts.DownloadingItems,
		&counts.PausedItems,
		&counts.CompletedItems,
		&counts.FailedItems,
		&counts.RemovedItems,
		&counts.CompletedSegments,
		&counts.FailedSegments,
	)
	return counts, err
}

func (m *Manager) AggregateTask(taskID string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := m.aggregateTaskInTx(tx, taskID); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *Manager) aggregateTaskInTx(tx *sql.Tx, taskID string) error {
	counts, currentStatus, err := countTaskStatusInTx(tx, taskID)
	if err != nil {
		return err
	}

	newStatus := computeTaskStatus(counts, currentStatus)
	var finished interface{}
	if newStatus == TaskStatusCompleted {
		finished = sql.Named("finished_time", time.Now())
	} else {
		finished = nil
	}

	_, err = tx.Exec(`
	UPDATE tasks
	SET status = ?,
		total_items = ?,
		done_items = ?,
		failed_items = ?,
		downloaded_segments = ?,
		updated_time = datetime('now'),
		finished_time = CASE WHEN ? = ? THEN COALESCE(finished_time, datetime('now')) ELSE NULL END
	WHERE id = ?
	`,
		newStatus,
		counts.TotalItems,
		counts.CompletedItems,
		counts.FailedItems,
		counts.CompletedSegments,
		newStatus,
		TaskStatusCompleted,
		taskID,
	)
	_ = finished
	return err
}

func computeTaskStatus(counts TaskStatusCounts, current string) string {
	if current == TaskStatusDeleted {
		return current
	}
	if counts.TotalItems == 0 {
		if current == TaskStatusPending || current == TaskStatusParsing {
			return current
		}
		return TaskStatusPending
	}

	active := counts.QueuedItems + counts.DownloadingItems
	if counts.CompletedItems == counts.TotalItems {
		return TaskStatusCompleted
	}
	if counts.FailedItems > 0 && active == 0 && counts.PendingItems == 0 && counts.PausedItems == 0 {
		return TaskStatusFailed
	}
	if counts.PausedItems > 0 && active == 0 && counts.PendingItems == 0 {
		return TaskStatusPaused
	}
	if active > 0 || counts.PendingItems > 0 || counts.CompletedItems > 0 {
		return TaskStatusDownloading
	}
	if current != "" {
		return current
	}
	return TaskStatusPending
}

func countTaskStatusInTx(tx *sql.Tx, taskID string) (TaskStatusCounts, string, error) {
	var counts TaskStatusCounts
	var current string
	err := tx.QueryRow(`
	SELECT
		COALESCE(t.status, 'pending'),
		COUNT(i.id) AS total_items,
		COALESCE(SUM(CASE WHEN i.status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_items,
		COALESCE(SUM(CASE WHEN i.status IN ('queued', 'submitting') THEN 1 ELSE 0 END), 0) AS queued_items,
		COALESCE(SUM(CASE WHEN i.status = 'downloading' THEN 1 ELSE 0 END), 0) AS downloading_items,
		COALESCE(SUM(CASE WHEN i.status = 'paused' THEN 1 ELSE 0 END), 0) AS paused_items,
		COALESCE(SUM(CASE WHEN i.status = 'completed' THEN 1 ELSE 0 END), 0) AS completed_items,
		COALESCE(SUM(CASE WHEN i.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_items,
		COALESCE(SUM(CASE WHEN i.status = 'removed' THEN 1 ELSE 0 END), 0) AS removed_items,
		COALESCE(SUM(CASE WHEN i.status = 'completed' AND i.item_type != 'key' THEN 1 ELSE 0 END), 0) AS completed_segments,
		COALESCE(SUM(CASE WHEN i.status = 'failed' AND i.item_type != 'key' THEN 1 ELSE 0 END), 0) AS failed_segments
	FROM tasks t
	LEFT JOIN task_item i ON i.task_id = t.id
	WHERE t.id = ?
	GROUP BY t.id, t.status
	`, taskID).Scan(
		&current,
		&counts.TotalItems,
		&counts.PendingItems,
		&counts.QueuedItems,
		&counts.DownloadingItems,
		&counts.PausedItems,
		&counts.CompletedItems,
		&counts.FailedItems,
		&counts.RemovedItems,
		&counts.CompletedSegments,
		&counts.FailedSegments,
	)
	return counts, current, err
}

func (m *Manager) DeleteTaskItems(taskID string) error {
	_, err := m.db.Exec(`DELETE FROM task_item WHERE task_id = ?`, taskID)
	return err
}

func (m *Manager) ensureColumn(tableName, columnName, definition string) error {
	rows, err := m.db.Query(fmt.Sprintf(`SELECT name FROM pragma_table_info('%s')`, tableName))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var existing string
		if err := rows.Scan(&existing); err != nil {
			return err
		}
		if existing == columnName {
			return nil
		}
	}
	_, err = m.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, tableName, columnName, definition))
	return err
}

func emptyStringToNullString(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func emptyJSON(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return value
}

func defaultTaskStatus(status string) string {
	if strings.TrimSpace(status) == "" {
		return TaskStatusPending
	}
	return status
}

func nonZeroTime(primary, fallback time.Time) time.Time {
	if !primary.IsZero() {
		return primary
	}
	if !fallback.IsZero() {
		return fallback
	}
	return time.Now()
}

func normalizeItemType(itemType string) string {
	switch strings.ToLower(strings.TrimSpace(itemType)) {
	case "key":
		return "key"
	default:
		return "segment"
	}
}

func guessFilePathForTask(taskID, filename string) string {
	return filepath.Join(taskID, filename)
}

type taskItemScanner interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

func scanTaskItems(rows taskItemScanner) ([]TaskItem, error) {
	var items []TaskItem
	for rows.Next() {
		var item TaskItem
		var completed sql.NullTime
		if err := rows.Scan(
			&item.ID,
			&item.TaskID,
			&item.Seq,
			&item.Filename,
			&item.Aria2GID,
			&item.URL,
			&item.Status,
			&item.ItemType,
			&item.FilePath,
			&item.FileSize,
			&item.RetryCount,
			&item.LastError,
			&item.CreatedTime,
			&item.UpdatedTime,
			&completed,
		); err != nil {
			return nil, err
		}
		if completed.Valid {
			item.CompletedTime = &completed.Time
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
