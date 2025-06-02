package cmd

import (
	stdsql "database/sql"
	"fmt"
	"github.com/B1NARY-GR0UP/csqlite/db"
	"github.com/B1NARY-GR0UP/csqlite/pkg/logger"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
)

var rootCmd = &cobra.Command{
	Use:     "csqlite",
	Short:   "Client - Server SQLite",
	Long:    "Client - Server SQLite for Contur",
	Version: "dev",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbPath := args[0]

		pool, err := stdsql.Open("sqlite3", dbPath)
		if err != nil {
			panic(fmt.Errorf("failed to open database: %v", err))
		}
		defer pool.Close()

		dbName := strings.TrimSuffix(filepath.Base(dbPath), filepath.Ext(dbPath))
		provider := db.NewProvider(db.NewDatabase(dbName, pool))
		engine := sqle.NewDefault(provider)

		config := server.Config{
			Protocol: "tcp",
			Address:  defaultFlags.Addr,
		}

		srv, err := server.NewServer(config, engine, sql.NewContext, db.NewSessionBuilder(provider), nil)
		if err != nil {
			panic(err)
		}
		defer srv.Close()

		logger.GetLogger().Infof("csqlite server listening on %s", defaultFlags.Addr)
		if err = srv.Start(); err != nil {
			panic(fmt.Errorf("failed to start server: %v", err))
		}
	},
}

type Flags struct {
	Addr string
}

var defaultFlags = Flags{
	Addr: "127.0.0.1:3306",
}

func init() {
	rootCmd.SetVersionTemplate("{{ .Version }}")
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	// flags
	rootCmd.Flags().StringVarP(&defaultFlags.Addr, "addr", "a", defaultFlags.Addr, "address of server")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
