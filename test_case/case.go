package test_case

import (
	"database/sql"
	"github.com/crazycs520/testutil/cmd"
)

func init() {
	cmd.RegisterCaseCmd(NewWriteConflict)
	cmd.RegisterCaseCmd(NewPessimisticWriteConflict)
	cmd.RegisterCaseCmd(NewReadWriteConflict)
	cmd.RegisterCaseCmd(NewBenchListPartitionTable)
	cmd.RegisterCaseCmd(NewStressCop)
	cmd.RegisterCaseCmd(NewIndexLookUpWrongPlan)
	cmd.RegisterCaseCmd(NewIndexHashJoinPlan)
}

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
