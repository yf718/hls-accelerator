package database

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Init initializes the SQLite database
func Init(dataDir string) (*sql.DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	dbFile := filepath.Join(dataDir, "tasks.db")
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}
	// SQLite 在当前这个高频小事务场景下，多连接写入非常容易触发 SQLITE_BUSY。
	// 这里直接串行化 DB 连接，优先保证 task/task_item 状态一致性。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// Enable WAL mode and set busy timeout for better concurrency and performance.
	// High-concurrency task_item state transitions can exceed 5s under load.
	_, err = db.Exec(`
		PRAGMA busy_timeout = 15000;
		PRAGMA journal_mode = WAL;
		PRAGMA foreign_keys = ON;
	`)
	if err != nil {
		// Just log or ignore if WAL fails, not critical
	}

	return db, nil
}
