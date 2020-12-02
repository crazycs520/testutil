package main

import (
	"database/sql"
	"fmt"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"os"
)

func GetSQLCli(cfg *config.DBConfig) *sql.DB {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&tidb_snapshot=%s&tidb_slow_log_threshold=20", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Snapshot)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		fmt.Println("can not connect to database.")
		os.Exit(1)
	}
	db.SetMaxOpenConns(1)
	return db
}

func main() {
	cfg := &config.DBConfig{
		Host:     "127.0.0.1",
		Port:     4000,
		User:     "root",
		Password: "",
		DBName:   "test",
		Snapshot: "421242806801006594",
	}
	db := GetSQLCli(cfg)
	util.QueryAndPrint(db, "select * from test.t")
}
