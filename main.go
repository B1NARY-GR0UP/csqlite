package main

import (
	"github.com/B1NARY-GR0UP/csqlite/cmd"
	// register sqlite3 driver
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	cmd.Execute()
}
