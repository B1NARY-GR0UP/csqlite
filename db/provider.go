package db

import (
	"cmp"
	"errors"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"slices"
	"strings"
	"sync"
)

var (
	_ sql.DatabaseProvider = (*Provider)(nil)
)

type Provider struct {
	mu        sync.RWMutex
	databases map[string]sql.Database
}

func NewProvider(dbs ...sql.Database) *Provider {
	dbMap := make(map[string]sql.Database, len(dbs))
	for _, db := range dbs {
		dbMap[strings.ToLower(db.Name())] = db
	}
	return &Provider{
		databases: dbMap,
	}
}

func (p *Provider) Database(_ *sql.Context, name string) (sql.Database, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	db, ok := p.databases[strings.ToLower(name)]
	if !ok {
		return nil, errors.New(fmt.Sprintf("database %s not found", name))
	}
	return db, nil
}

func (p *Provider) HasDatabase(_ *sql.Context, name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, ok := p.databases[strings.ToLower(name)]
	return ok
}

func (p *Provider) AllDatabases(_ *sql.Context) []sql.Database {
	p.mu.RLock()
	defer p.mu.RUnlock()

	dbs := make([]sql.Database, 0, len(p.databases))
	for _, db := range p.databases {
		dbs = append(dbs, db)
	}

	slices.SortFunc(dbs, func(a sql.Database, b sql.Database) int {
		return cmp.Compare(a.Name(), b.Name())
	})
	return dbs
}
