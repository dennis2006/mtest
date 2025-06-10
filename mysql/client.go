package mysql

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
)

func createMySQLClient(port int, dbName string) (*sqlx.DB, *sql.DB, error) {
	dsn := fmt.Sprintf("root:@tcp(127.0.0.1:%d)/%s", port, dbName)
	sqlxDB, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect sqlx client: %w", err)
	}

	if err = sqlxDB.Ping(); err != nil {
		return nil, nil, fmt.Errorf("failed to ping sqlx: %w", err)
	}

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect sql client: %w", err)
	}

	if err = sqlDB.Ping(); err != nil {
		return nil, nil, fmt.Errorf("failed to ping sql: %w", err)
	}

	return sqlxDB, sqlDB, nil
}
