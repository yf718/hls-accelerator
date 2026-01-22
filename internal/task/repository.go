package task

import (
	"database/sql"
)

// InitTable creates the tasks and task_item tables if they don't exist
func (m *Manager) InitTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		original_url TEXT,
		total_segments INTEGER,
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
		created_time DATETIME,
		UNIQUE(task_id, filename)
	);

	CREATE INDEX IF NOT EXISTS idx_task_item_task_id ON task_item(task_id);
	`
	_, err := m.db.Exec(query)
	return err
}

func (m *Manager) CreateTask(meta TaskMetadata) error {
	query := `INSERT INTO tasks (id, original_url, total_segments, created_time, status, proxied_content) VALUES (?, ?, ?, ?, ?, ?)`
	_, err := m.db.Exec(query, meta.ID, meta.OriginalURL, meta.TotalSegments, meta.CreatedTime, meta.Status, meta.ProxiedContent)
	return err
}

func (m *Manager) GetTask(id string) (*TaskMetadata, error) {
	query := `SELECT id, original_url, total_segments, created_time, status FROM tasks WHERE id = ?`
	row := m.db.QueryRow(query, id)
	var meta TaskMetadata
	err := row.Scan(&meta.ID, &meta.OriginalURL, &meta.TotalSegments, &meta.CreatedTime, &meta.Status)
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

func (m *Manager) ListTasksDB() ([]TaskMetadata, error) {
	// Explicitly NOT selecting proxied_content
	query := `SELECT id, original_url, total_segments, created_time, status FROM tasks ORDER BY created_time DESC`
	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskMetadata
	for rows.Next() {
		var meta TaskMetadata
		if err := rows.Scan(&meta.ID, &meta.OriginalURL, &meta.TotalSegments, &meta.CreatedTime, &meta.Status); err != nil {
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
	query := `SELECT id, original_url, total_segments, created_time, status FROM tasks WHERE status = ?`
	rows, err := m.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskMetadata
	for rows.Next() {
		var meta TaskMetadata
		if err := rows.Scan(&meta.ID, &meta.OriginalURL, &meta.TotalSegments, &meta.CreatedTime, &meta.Status); err != nil {
			return nil, err
		}
		tasks = append(tasks, meta)
	}
	return tasks, nil
}

// Task Item operations

// CreateTaskItem adds a new task item record
func (m *Manager) CreateTaskItem(taskID, filename, aria2GID, url string) error {
	query := `INSERT OR IGNORE INTO task_item (task_id, filename, aria2_gid, url, created_time) VALUES (?, ?, ?, ?, datetime('now'))`
	_, err := m.db.Exec(query, taskID, filename, aria2GID, url)
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
}

// GetTaskItems returns all task items for a given task
func (m *Manager) GetTaskItems(taskID string) ([]TaskItem, error) {
	query := `SELECT filename, aria2_gid, url FROM task_item WHERE task_id = ?`
	rows, err := m.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []TaskItem
	for rows.Next() {
		var item TaskItem
		if err := rows.Scan(&item.Filename, &item.Aria2GID, &item.URL); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// DeleteTaskItems removes all task items for a given task
func (m *Manager) DeleteTaskItems(taskID string) error {
	query := `DELETE FROM task_item WHERE task_id = ?`
	_, err := m.db.Exec(query, taskID)
	return err
}
