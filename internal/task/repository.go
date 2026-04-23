package task

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
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
		created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		finished_time DATETIME,
		status TEXT NOT NULL DEFAULT 'pending',
		proxied_content TEXT NOT NULL DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
	`
	_, err := m.db.Exec(query)
	return err
}

func (m *Manager) CreateTask(meta TaskMetadata) error {
	_, err := m.db.Exec(`
	INSERT INTO tasks (
		id, name, original_url, total_segments, downloaded_segments,
		total_items, done_items, failed_items, output_dir,
		created_time, updated_time, finished_time, status, proxied_content
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
		total_items, done_items, failed_items, output_dir,
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
		total_items, done_items, failed_items, output_dir,
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
		total_items, done_items, failed_items, output_dir,
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
