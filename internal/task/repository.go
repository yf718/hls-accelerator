package task

import (
	"database/sql"
	"fmt"
	playlist "hls-accelerator/internal/m3u8"
	"strings"
	"time"
)

const (
	taskItemStatusPending    = "pending"
	taskItemStatusSubmitting = "submitting"
	taskItemStatusQueued     = "queued"
	taskItemStatusCompleted  = "completed"
	taskItemStatusFailed     = "failed"
)

type tableColumnInfo struct {
	Name         string
	NotNull      bool
	DefaultValue sql.NullString
}

func isSQLiteBusyOrLocked(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") ||
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

// InitTable creates the tasks and task_item tables if they don't exist
func (m *Manager) InitTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL DEFAULT '',
		original_url TEXT,
		total_segments INTEGER,
		downloaded_segments INTEGER NOT NULL DEFAULT 0,
		created_time DATETIME,
		status TEXT,
		proxied_content TEXT
	);

	CREATE TABLE IF NOT EXISTS task_item (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		filename TEXT NOT NULL,
		aria2_gid TEXT,
		url TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		completed_time DATETIME,
		created_time DATETIME,
		item_type TEXT NOT NULL DEFAULT '',
		last_error TEXT NOT NULL DEFAULT '',
		retry_count INTEGER NOT NULL DEFAULT 0,
		updated_time DATETIME,
		UNIQUE(task_id, filename)
	);

	CREATE INDEX IF NOT EXISTS idx_task_item_task_id ON task_item(task_id);
	CREATE INDEX IF NOT EXISTS idx_task_item_gid ON task_item(aria2_gid);
	CREATE INDEX IF NOT EXISTS idx_task_item_task_status ON task_item(task_id, status);
	`
	if _, err := m.db.Exec(query); err != nil {
		return err
	}

	if err := m.ensureColumn("tasks", "downloaded_segments", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := m.ensureColumn("tasks", "name", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "status", "TEXT NOT NULL DEFAULT 'pending'"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "completed_time", "DATETIME"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "item_type", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "last_error", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "retry_count", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "updated_time", "DATETIME"); err != nil {
		return err
	}
	if err := m.migrateTaskItemTable(); err != nil {
		return err
	}
	_, err := m.db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_task_item_gid ON task_item(aria2_gid);
		CREATE INDEX IF NOT EXISTS idx_task_item_task_status ON task_item(task_id, status);
	`)
	return err
}

func (m *Manager) CreateTask(meta TaskMetadata) error {
	query := `INSERT INTO tasks (id, name, original_url, total_segments, downloaded_segments, created_time, status, proxied_content) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := m.db.Exec(query, meta.ID, meta.Name, meta.OriginalURL, meta.TotalSegments, meta.DownloadedSegments, meta.CreatedTime, meta.Status, meta.ProxiedContent)
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
	query := `SELECT id, name, original_url, total_segments, downloaded_segments, created_time, status FROM tasks WHERE id = ?`
	row := m.db.QueryRow(query, id)
	var meta TaskMetadata
	err := row.Scan(&meta.ID, &meta.Name, &meta.OriginalURL, &meta.TotalSegments, &meta.DownloadedSegments, &meta.CreatedTime, &meta.Status)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (m *Manager) GetTaskProxiedContent(id string) (string, error) {
	query := `SELECT proxied_content FROM tasks WHERE id = ?`
	var content sql.NullString
	err := m.db.QueryRow(query, id).Scan(&content)
	if err != nil {
		return "", err
	}
	return content.String, nil
}

func (m *Manager) UpdateTaskProxiedContent(id, content string) error {
	query := `UPDATE tasks SET proxied_content = ? WHERE id = ?`
	_, err := m.db.Exec(query, content, id)
	return err
}

func (m *Manager) UpdateTaskStatus(id, status string) error {
	query := `UPDATE tasks SET status = ? WHERE id = ?`
	_, err := m.db.Exec(query, status, id)
	return err
}

func (m *Manager) UpdateTaskDownloadedSegments(id string, downloaded int) error {
	query := `UPDATE tasks SET downloaded_segments = ? WHERE id = ?`
	_, err := m.db.Exec(query, downloaded, id)
	return err
}

func (m *Manager) ListTasksDB() ([]TaskMetadata, error) {
	query := `SELECT id, name, original_url, total_segments, downloaded_segments, created_time, status FROM tasks ORDER BY created_time DESC`
	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskMetadata
	for rows.Next() {
		var meta TaskMetadata
		if err := rows.Scan(&meta.ID, &meta.Name, &meta.OriginalURL, &meta.TotalSegments, &meta.DownloadedSegments, &meta.CreatedTime, &meta.Status); err != nil {
			return nil, err
		}
		tasks = append(tasks, meta)
	}
	return tasks, nil
}

func (m *Manager) DeleteTaskDB(id string) error {
	query := `DELETE FROM tasks WHERE id = ?`
	_, err := m.db.Exec(query, id)
	return err
}

// CheckTaskExists checks if a task exists and returns its status
func (m *Manager) CheckTaskExists(id string) (bool, string, error) {
	query := `SELECT status FROM tasks WHERE id = ?`
	var status string
	err := m.db.QueryRow(query, id).Scan(&status)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, status, nil
}

func (m *Manager) GetTasksByStatus(status string) ([]TaskMetadata, error) {
	query := `SELECT id, name, original_url, total_segments, downloaded_segments, created_time, status FROM tasks WHERE status = ?`
	rows, err := m.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskMetadata
	for rows.Next() {
		var meta TaskMetadata
		if err := rows.Scan(&meta.ID, &meta.Name, &meta.OriginalURL, &meta.TotalSegments, &meta.DownloadedSegments, &meta.CreatedTime, &meta.Status); err != nil {
			return nil, err
		}
		tasks = append(tasks, meta)
	}
	return tasks, nil
}

// Task Item operations

// CreateTaskItem adds a queued task item record. Tests and one-off callers use this helper.
func (m *Manager) CreateTaskItem(taskID, filename, aria2GID, url, itemType string) error {
	query := `
INSERT INTO task_item (task_id, filename, aria2_gid, url, item_type, status, created_time, updated_time)
VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
ON CONFLICT(task_id, filename) DO UPDATE SET
	aria2_gid = excluded.aria2_gid,
	url = excluded.url,
	item_type = excluded.item_type,
	status = excluded.status,
	last_error = '',
	updated_time = datetime('now')
`
	_, err := m.db.Exec(query, taskID, filename, emptyStringToNullString(aria2GID), url, itemType, taskItemStatusQueued)
	return err
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
INSERT INTO task_item (task_id, filename, aria2_gid, url, item_type, status, created_time, updated_time, last_error)
VALUES (?, ?, NULL, ?, ?, ?, datetime('now'), datetime('now'), '')
ON CONFLICT(task_id, filename) DO UPDATE SET
	url = excluded.url,
	item_type = excluded.item_type,
	status = CASE
		WHEN task_item.status IN ('pending', 'failed') THEN 'pending'
		ELSE task_item.status
	END,
	aria2_gid = CASE
		WHEN task_item.status IN ('pending', 'failed') THEN NULL
		ELSE task_item.aria2_gid
	END,
	last_error = CASE
		WHEN task_item.status IN ('pending', 'failed') THEN ''
		ELSE task_item.last_error
	END,
	updated_time = datetime('now')
`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := item.Filename + "\x00" + item.URL + "\x00" + item.Type
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if _, err := stmt.Exec(taskID, item.Filename, item.URL, item.Type, taskItemStatusPending); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (m *Manager) ClaimPendingTaskItems(taskID string, limit int) ([]TaskItem, error) {
	if limit <= 0 {
		limit = 1
	}

	const maxAttempts = 8
	query := `
WITH picked AS (
	SELECT id
	FROM task_item
	WHERE task_id = ? AND status = 'pending'
	ORDER BY id
	LIMIT ?
)
UPDATE task_item
SET status = ?, updated_time = datetime('now')
WHERE id IN (SELECT id FROM picked) AND status = 'pending'
RETURNING filename, COALESCE(aria2_gid, ''), url, status, COALESCE(item_type, '')
`
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		rows, err := m.db.Query(query, taskID, limit, taskItemStatusSubmitting)
		if err != nil {
			lastErr = err
			if !isSQLiteBusyOrLocked(err) {
				return nil, err
			}
			sleepForBusyRetry(attempt)
			continue
		}

		var out []TaskItem
		for rows.Next() {
			var item TaskItem
			if err := rows.Scan(&item.Filename, &item.Aria2GID, &item.URL, &item.Status, &item.ItemType); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			lastErr = err
			if !isSQLiteBusyOrLocked(err) {
				return nil, err
			}
			sleepForBusyRetry(attempt)
			continue
		}
		if err := rows.Close(); err != nil {
			lastErr = err
			if !isSQLiteBusyOrLocked(err) {
				return nil, err
			}
			sleepForBusyRetry(attempt)
			continue
		}
		return out, nil
	}
	return nil, fmt.Errorf("claim pending task items busy after %d attempts: %w", maxAttempts, lastErr)
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
	return nil
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
	return err
}

func (m *Manager) RecoverSubmittingTaskItems() error {
	_, err := m.db.Exec(`
UPDATE task_item
SET status = ?, last_error = CASE WHEN last_error = '' THEN 'recovered after restart' ELSE last_error END, updated_time = datetime('now')
WHERE status = ?
`, taskItemStatusPending, taskItemStatusSubmitting)
	return err
}

// GetTaskItemGIDs returns all non-empty aria2 GIDs for a given task
func (m *Manager) GetTaskItemGIDs(taskID string) ([]string, error) {
	query := `SELECT aria2_gid FROM task_item WHERE task_id = ? AND aria2_gid IS NOT NULL AND aria2_gid != ''`
	rows, err := m.db.Query(query, taskID)
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
	return gids, nil
}

// TaskItem represents a single download item
type TaskItem struct {
	Filename string
	Aria2GID string
	URL      string
	Status   string
	ItemType string
}

// GetTaskItems returns all task items for a given task
func (m *Manager) GetTaskItems(taskID string) ([]TaskItem, error) {
	query := `SELECT filename, COALESCE(aria2_gid, ''), url, status, COALESCE(item_type, '') FROM task_item WHERE task_id = ?`
	rows, err := m.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []TaskItem
	for rows.Next() {
		var item TaskItem
		if err := rows.Scan(&item.Filename, &item.Aria2GID, &item.URL, &item.Status, &item.ItemType); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (m *Manager) GetIncompleteTaskItems(taskID string) ([]TaskItem, error) {
	query := `
SELECT filename, COALESCE(aria2_gid, ''), url, status, COALESCE(item_type, '')
FROM task_item
WHERE task_id = ? AND status != 'completed'
`
	rows, err := m.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []TaskItem
	for rows.Next() {
		var item TaskItem
		if err := rows.Scan(&item.Filename, &item.Aria2GID, &item.URL, &item.Status, &item.ItemType); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (m *Manager) MarkTaskItemCompletedByGID(gid string) (string, bool, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	var taskID string
	var itemType string
	var filename string
	if err := tx.QueryRow(`
SELECT task_id, COALESCE(item_type, ''), filename
FROM task_item
WHERE aria2_gid = ? AND aria2_gid IS NOT NULL AND aria2_gid != ''
`, gid).Scan(&taskID, &itemType, &filename); err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	res, err := tx.Exec(`
UPDATE task_item
SET status = 'completed', completed_time = datetime('now'), updated_time = datetime('now')
WHERE aria2_gid = ? AND status = 'queued'
`, gid)
	if err != nil {
		return "", false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return "", false, err
	}
	if rowsAffected == 0 {
		if err := tx.Commit(); err != nil {
			return "", false, err
		}
		return taskID, false, nil
	}

	shouldCountTowardsProgress := itemType != "key" && !strings.HasSuffix(strings.ToLower(filename), ".key")
	if !shouldCountTowardsProgress {
		if err := tx.Commit(); err != nil {
			return "", false, err
		}
		return taskID, true, nil
	}

	_, err = tx.Exec(`
		UPDATE tasks
		SET downloaded_segments = CASE
				WHEN downloaded_segments < total_segments THEN downloaded_segments + 1
				ELSE downloaded_segments
			END,
			status = CASE
				WHEN downloaded_segments + 1 >= total_segments THEN 'completed'
				ELSE status
			END
		WHERE id = ?
	`, taskID)
	if err != nil {
		return "", false, err
	}

	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return taskID, true, nil
}

func (m *Manager) MarkTaskItemCompletedByFilename(taskID, filename string) (bool, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var itemType string
	if err := tx.QueryRow(`
SELECT COALESCE(item_type, '')
FROM task_item
WHERE task_id = ? AND filename = ?
`, taskID, filename).Scan(&itemType); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	res, err := tx.Exec(`
UPDATE task_item
SET status = 'completed',
	aria2_gid = NULL,
	completed_time = datetime('now'),
	updated_time = datetime('now'),
	last_error = ''
WHERE task_id = ? AND filename = ? AND status != 'completed'
`, taskID, filename)
	if err != nil {
		return false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if rowsAffected == 0 {
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}

	shouldCountTowardsProgress := itemType != "key" && !strings.HasSuffix(strings.ToLower(filename), ".key")
	if shouldCountTowardsProgress {
		if _, err := tx.Exec(`
			UPDATE tasks
			SET downloaded_segments = CASE
					WHEN downloaded_segments < total_segments THEN downloaded_segments + 1
					ELSE downloaded_segments
				END,
				status = CASE
					WHEN downloaded_segments + 1 >= total_segments THEN 'completed'
					ELSE status
				END
			WHERE id = ?
		`, taskID); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
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

	query := fmt.Sprintf(
		`SELECT id, name, original_url, total_segments, downloaded_segments, created_time, status FROM tasks WHERE status IN (%s) ORDER BY created_time DESC`,
		strings.Join(placeholders, ","),
	)
	rows, err := m.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskMetadata
	for rows.Next() {
		var meta TaskMetadata
		if err := rows.Scan(&meta.ID, &meta.Name, &meta.OriginalURL, &meta.TotalSegments, &meta.DownloadedSegments, &meta.CreatedTime, &meta.Status); err != nil {
			return nil, err
		}
		tasks = append(tasks, meta)
	}
	return tasks, nil
}

// DeleteTaskItems removes all task items for a given task
func (m *Manager) DeleteTaskItems(taskID string) error {
	query := `DELETE FROM task_item WHERE task_id = ?`
	_, err := m.db.Exec(query, taskID)
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

func (m *Manager) tableColumns(tableName string) (map[string]tableColumnInfo, error) {
	rows, err := m.db.Query(fmt.Sprintf(`SELECT name, "notnull", dflt_value FROM pragma_table_info('%s')`, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]tableColumnInfo)
	for rows.Next() {
		var info tableColumnInfo
		var notNull int
		if err := rows.Scan(&info.Name, &notNull, &info.DefaultValue); err != nil {
			return nil, err
		}
		info.NotNull = notNull != 0
		columns[info.Name] = info
	}
	return columns, nil
}

func (m *Manager) migrateTaskItemTable() error {
	columns, err := m.tableColumns("task_item")
	if err != nil {
		return err
	}
	aria2GID, ok := columns["aria2_gid"]
	if !ok || !aria2GID.NotNull {
		return nil
	}

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
CREATE TABLE task_item_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	task_id TEXT NOT NULL,
	filename TEXT NOT NULL,
	aria2_gid TEXT,
	url TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'pending',
	completed_time DATETIME,
	created_time DATETIME,
	item_type TEXT NOT NULL DEFAULT '',
	last_error TEXT NOT NULL DEFAULT '',
	retry_count INTEGER NOT NULL DEFAULT 0,
	updated_time DATETIME,
	UNIQUE(task_id, filename)
)
`); err != nil {
		return err
	}

	itemTypeExpr := `''`
	if _, ok := columns["item_type"]; ok {
		itemTypeExpr = `COALESCE(item_type, '')`
	}
	lastErrorExpr := `''`
	if _, ok := columns["last_error"]; ok {
		lastErrorExpr = `COALESCE(last_error, '')`
	}
	retryCountExpr := `0`
	if _, ok := columns["retry_count"]; ok {
		retryCountExpr = `COALESCE(retry_count, 0)`
	}
	updatedTimeExpr := `created_time`
	if _, ok := columns["updated_time"]; ok {
		updatedTimeExpr = `updated_time`
	}

	copyQuery := fmt.Sprintf(`
INSERT INTO task_item_new (id, task_id, filename, aria2_gid, url, status, completed_time, created_time, item_type, last_error, retry_count, updated_time)
SELECT
	id,
	task_id,
	filename,
	NULLIF(aria2_gid, ''),
	url,
	CASE WHEN COALESCE(status, '') = '' THEN '%s' ELSE status END,
	completed_time,
	created_time,
	%s,
	%s,
	%s,
	%s
FROM task_item
`, taskItemStatusQueued, itemTypeExpr, lastErrorExpr, retryCountExpr, updatedTimeExpr)
	if _, err := tx.Exec(copyQuery); err != nil {
		return err
	}

	if _, err := tx.Exec(`DROP TABLE task_item`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE task_item_new RENAME TO task_item`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
CREATE INDEX IF NOT EXISTS idx_task_item_task_id ON task_item(task_id);
CREATE INDEX IF NOT EXISTS idx_task_item_gid ON task_item(aria2_gid);
CREATE INDEX IF NOT EXISTS idx_task_item_task_status ON task_item(task_id, status);
`); err != nil {
		return err
	}

	return tx.Commit()
}

func emptyStringToNullString(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
