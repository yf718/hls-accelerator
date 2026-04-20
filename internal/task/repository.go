package task

import (
	"database/sql"
	"fmt"
	"strings"
)

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
		aria2_gid TEXT NOT NULL,
		url TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'queued',
		completed_time DATETIME,
		created_time DATETIME,
		UNIQUE(task_id, filename)
	);

	CREATE INDEX IF NOT EXISTS idx_task_item_task_id ON task_item(task_id);
	CREATE INDEX IF NOT EXISTS idx_task_item_gid ON task_item(aria2_gid);
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
	if err := m.ensureColumn("task_item", "status", "TEXT NOT NULL DEFAULT 'queued'"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "completed_time", "DATETIME"); err != nil {
		return err
	}
	if err := m.ensureColumn("task_item", "item_type", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	_, err := m.db.Exec(`CREATE INDEX IF NOT EXISTS idx_task_item_gid ON task_item(aria2_gid)`)
	return err
}

func (m *Manager) CreateTask(meta TaskMetadata) error {
	query := `INSERT INTO tasks (id, name, original_url, total_segments, downloaded_segments, created_time, status, proxied_content) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := m.db.Exec(query, meta.ID, meta.Name, meta.OriginalURL, meta.TotalSegments, meta.DownloadedSegments, meta.CreatedTime, meta.Status, meta.ProxiedContent)
	return err
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
	// Explicitly NOT selecting proxied_content
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

// CreateTaskItem adds a new task item record
func (m *Manager) CreateTaskItem(taskID, filename, aria2GID, url, itemType string) error {
	query := `INSERT OR IGNORE INTO task_item (task_id, filename, aria2_gid, url, item_type, created_time) VALUES (?, ?, ?, ?, ?, datetime('now'))`
	_, err := m.db.Exec(query, taskID, filename, aria2GID, url, itemType)
	return err
}

// GetTaskItemGIDs returns all aria2 GIDs for a given task
func (m *Manager) GetTaskItemGIDs(taskID string) ([]string, error) {
	query := `SELECT aria2_gid FROM task_item WHERE task_id = ?`
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
}

// GetTaskItems returns all task items for a given task
func (m *Manager) GetTaskItems(taskID string) ([]TaskItem, error) {
	query := `SELECT filename, aria2_gid, url, status FROM task_item WHERE task_id = ?`
	rows, err := m.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []TaskItem
	for rows.Next() {
		var item TaskItem
		if err := rows.Scan(&item.Filename, &item.Aria2GID, &item.URL, &item.Status); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (m *Manager) GetIncompleteTaskItems(taskID string) ([]TaskItem, error) {
	query := `SELECT filename, aria2_gid, url, status FROM task_item WHERE task_id = ? AND status != 'completed'`
	rows, err := m.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []TaskItem
	for rows.Next() {
		var item TaskItem
		if err := rows.Scan(&item.Filename, &item.Aria2GID, &item.URL, &item.Status); err != nil {
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
	if err := tx.QueryRow(`SELECT task_id, COALESCE(item_type, ''), filename FROM task_item WHERE aria2_gid = ?`, gid).Scan(&taskID, &itemType, &filename); err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	res, err := tx.Exec(`UPDATE task_item SET status = 'completed', completed_time = datetime('now') WHERE aria2_gid = ? AND status != 'completed'`, gid)
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
