package db

import (
	stdsql "database/sql"
	"errors"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.Database = (*Database)(nil)

type Database struct {
	name string
	pool *stdsql.DB
}

func NewDatabase(name string, pool *stdsql.DB) *Database {
	return &Database{
		name: name,
		pool: pool,
	}
}

func (db *Database) Name() string {
	return db.name
}

func (db *Database) GetTableInsensitive(_ *sql.Context, tblName string) (sql.Table, bool, error) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND LOWER(name) = LOWER(?)"

	var table string
	if err := db.pool.QueryRow(query, tblName).Scan(&table); err != nil {
		if errors.Is(err, stdsql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return NewTable(table, db.pool), true, nil
}

func (db *Database) GetTableNames(_ *sql.Context) ([]string, error) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"

	rows, err := db.pool.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tblNames []string
	for rows.Next() {
		var tblName string
		if err = rows.Scan(&tblName); err != nil {
			return nil, err
		}
		tblNames = append(tblNames, tblName)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tblNames, nil
}
