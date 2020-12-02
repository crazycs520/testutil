package test_case

import (
	"database/sql"
)

func prepare(db *sql.DB, dbName string, sqls []string) error {
	ss := []string{
		"create database if not exists " + dbName,
		"use " + dbName,
	}
	sqls = append(ss, sqls...)
	for _, s := range sqls {
		_, err := db.Exec(s)
		if err != nil {
			return err
		}
	}
	return nil
}
