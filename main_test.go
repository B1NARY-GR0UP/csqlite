package main

import (
	stdsql "database/sql"
	"testing"
)

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
