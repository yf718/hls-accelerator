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
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// Enable WAL mode and set busy timeout for better concurrency and performance
	_, err = db.Exec(`
		PRAGMA busy_timeout = 5000;
		PRAGMA journal_mode = WAL;
		PRAGMA foreign_keys = ON;
	`)
	if err != nil {
		// Just log or ignore if WAL fails, not critical
	}

	return db, nil
}
