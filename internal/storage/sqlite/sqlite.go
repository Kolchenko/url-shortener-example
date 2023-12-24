package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"url-shortener/internal/storage"

	"github.com/mattn/go-sqlite3"
)

const QUERY_INIT_SCHEMA = `
	CREATE TABLE IF NOT EXISTS url(
		id INTEGER PRIMARY KEY,
		alias TEXT NOT NULL UNIQUE,
		url TEXT NOT NULL);
	CREATE INDEX IF NOT EXISTS idx_aliases ON url(alias);
`

const QUERY_SAVE_URL = `
	INSERT INTO url(url, alias) VALUES(?, ?)
`

const QUERY_GET_URL = `
	SELECT url FROM url WHERE alias = ?
`

const QUERY_DELETE_URL = `
	DELETE FROM url WHERE alias = ?
`

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	db, err := sql.Open("sqlite3", storagePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open sqlite database: %w", err)
	}

	stmt, err := db.Prepare(QUERY_INIT_SCHEMA)
	if err != nil {
		return nil, fmt.Errorf("cannot init schema: %w", err)
	}

	_, err = stmt.Exec()
	if err != nil {
		return nil, fmt.Errorf("cannot execute init schema query: %w", err)
	}

	return &Storage{
		db: db,
	}, nil
}

func (s *Storage) SaveURL(url string, alias string) (int64, error) {
	stmt, err := s.db.Prepare(QUERY_SAVE_URL)
	if err != nil {
		return 0, fmt.Errorf("cannot prepare statement to save url: %w", err)
	}

	res, err := stmt.Exec(url, alias)
	if err != nil {
		if isErrConstraintUnique(err) {
			return 0, storage.ErrURLAlreadyExists
		}
		return 0, fmt.Errorf("cannot execute statement to save url: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("cannot get last insert id: %w", err)
	}

	return id, nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	stmt, err := s.db.Prepare(QUERY_GET_URL)
	if err != nil {
		return "", fmt.Errorf("cannot prepare statement to get url: %w", err)
	}

	var url string
	err = stmt.QueryRow(alias).Scan(&url)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrURLNotFound
	}

	if err != nil {
		return "", fmt.Errorf("cannot execute statement to get url: %w", err)
	}

	return url, nil
}

func (s *Storage) DeleteURL(alias string) error {
	stmt, err := s.db.Prepare(QUERY_DELETE_URL)
	if err != nil {
		return fmt.Errorf("cannot prepare statement to delete url: %w", err)
	}

	res, err := stmt.Exec(alias)
	if err != nil {
		return fmt.Errorf("cannot execute statement to delete url: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return storage.ErrURLNotFound
	}

	return nil
}

func isErrConstraintUnique(err error) bool {
	if sqliteErr, ok := err.(sqlite3.Error); ok && sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique {
		return true
	}

	return false
}
