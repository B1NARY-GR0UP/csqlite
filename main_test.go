package main

import (
	stdsql "database/sql"
	"github.com/B1NARY-GR0UP/csqlite/db"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"testing"
)

func TestPrototype(t *testing.T) {
	pool, err := stdsql.Open("sqlite3", "testdata/test.db")
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	provider := db.NewProvider(db.NewDatabase("test", pool))
	engine := sqle.NewDefault(provider)

	config := server.Config{
		Protocol: "tcp",
		Address:  "127.0.0.1:9999",
	}

	srv, err := server.NewServer(config, engine, sql.NewContext, db.NewSessionBuilder(provider), nil)
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	if err = srv.Start(); err != nil {
		panic(err)
	}
}

func TestSQLiteDriver(t *testing.T) {
	// Connect to SQLite database
	db, err := stdsql.Open("sqlite3", "testdata/test.db")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Query sqlite_master table to get all table names
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Fatalf("Failed to query tables: %v", err)
	}
	defer rows.Close()

	// Count tables and collect table names
	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("Failed to read table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error occurred while iterating result set: %v", err)
	}

	// Output results
	t.Logf("Database contains %d tables in total", len(tables))
	t.Logf("Table name list: %v", tables)

	// Ensure at least one table exists (this can be adjusted according to your actual situation)
	if len(tables) == 0 {
		t.Errorf("No tables found in the database")
	}
}
