package db

import (
	stdsql "database/sql"
	"fmt"
	"github.com/B1NARY-GR0UP/csqlite/pkg/logger"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"
	"strings"
)

var _ sql.Table = (*Table)(nil)

type Table struct {
	name string
	pool *stdsql.DB
}

func NewTable(name string, pool *stdsql.DB) *Table {
	return &Table{
		name: name,
		pool: pool,
	}
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) String() string {
	return t.name
}

func (t *Table) Schema() sql.Schema {
	// table names cannot be parameterized
	query := fmt.Sprintf("PRAGMA table_info(%s)", t.name)

	rows, err := t.pool.Query(query)
	if err != nil {
		logger.GetLogger().Errorf("query table schema error: %v", err)
		return nil
	}
	defer rows.Close()

	var schema sql.Schema
	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notnull   int
			dfltValue any
			pk        int
		)

		if err = rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			logger.GetLogger().Errorf("scan table schema error: %v", err)
			return nil
		}

		var colType sql.Type
		switch strings.ToUpper(ctype) {
		case "INTEGER":
			colType = types.Int64
		case "REAL", "FLOAT", "DOUBLE":
			colType = types.Float64
		case "TEXT", "VARCHAR", "CHAR":
			colType = types.Text
		case "BLOB":
			colType = types.Blob
		case "BOOLEAN":
			colType = types.Boolean
		case "DATE":
			colType = types.Date
		case "DATETIME":
			colType = types.Datetime
		case "TIMESTAMP":
			colType = types.Timestamp
		default:
			colType = types.Text
		}

		column := &sql.Column{
			Name:       name,
			Type:       colType,
			Nullable:   notnull == 0,
			Source:     t.name,
			PrimaryKey: pk == 1,
		}
		schema = append(schema, column)
	}

	if err = rows.Err(); err != nil {
		logger.GetLogger().Errorf("table rows iteration error: %v", err)
		return nil
	}
	return schema
}

func (t *Table) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t *Table) Partitions(_ *sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{
		single:  &partition{},
		visited: false,
	}, nil
}

type partitionIter struct {
	single  sql.Partition
	visited bool
}

func (pi *partitionIter) Close(_ *sql.Context) error {
	return nil
}

func (pi *partitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	if pi.visited {
		return nil, io.EOF
	}
	pi.visited = true
	return pi.single, nil
}

type partition struct{}

func (p *partition) Key() []byte {
	return []byte("single-partition-key")
}

func (t *Table) PartitionRows(_ *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	// table names cannot be parameterized
	query := fmt.Sprintf("SELECT * FROM %s", t.name)

	rows, err := t.pool.Query(query)
	if err != nil {
		return nil, err
	}
	return &rowIter{
		rows: rows,
		n:    len(t.Schema()),
	}, nil
}

type rowIter struct {
	rows *stdsql.Rows
	n    int
}

func (ri *rowIter) Next(_ *sql.Context) (sql.Row, error) {
	if !ri.rows.Next() {
		if err := ri.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	vals := make([]any, ri.n)
	args := make([]any, ri.n)
	for i := range vals {
		args[i] = &vals[i]
	}

	if err := ri.rows.Scan(args...); err != nil {
		return nil, err
	}

	return sql.NewRow(vals...), nil
}

func (ri *rowIter) Close(_ *sql.Context) error {
	return ri.rows.Close()
}
