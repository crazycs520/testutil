package data

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type LoadDataSuit struct {
	cfg         *config.Config
	insertCount int64
}

func NewLoadDataSuit(cfg *config.Config) *LoadDataSuit {
	return &LoadDataSuit{
		cfg: cfg,
	}
}

func (c *LoadDataSuit) Prepare(t *TableInfo, rows, regionRowNum int) error {
	c.cfg.DBName = t.DBName
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	exist := c.checkTableExist(db, t, rows)
	if exist {
		return nil
	}

	prepareSQLs := []string{
		"drop table if exists " + t.DBTableName(),
		t.createSQL(),
	}
	err := prepare(db, c.cfg.DBName, prepareSQLs)
	if err != nil {
		return err
	}
	if rows == 0 {
		return nil
	}

	// split region.
	if rows > regionRowNum && regionRowNum > 0 {
		split := fmt.Sprintf("split table %v between (0) and (%v) regions %v;", t.DBTableName(), rows, rows/regionRowNum)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := db.ExecContext(ctx, split)
		if err != nil {
			fmt.Printf("split region error: %v\n", err)
		}
		cancel()
	}
	return c.loadData(t, rows)
}

func (c *LoadDataSuit) loadData(t *TableInfo, rows int) error {
	// prepare data.
	step := (rows / c.cfg.Concurrency) + 1
	if step < 10 {
		return c.insertData(t, 0, rows)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, c.cfg.Concurrency)
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		start := i * step
		end := (i + 1) * step
		if end > rows {
			end = rows
		}
		go func() {
			defer wg.Done()
			err := c.insertData(t, start, end)
			if err != nil {
				fmt.Printf("insert data error: %v\n", err)
				errCh <- err
			}
		}()
	}
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case err = <-errCh:
				cancel()
			case <-ticker.C:
				fmt.Printf("inserted rows: %v \n", atomic.LoadInt64(&c.insertCount))
			}
		}
	}()
	wg.Wait()
	if err == nil {
		cancel()
	}
	return nil
}

func (c *LoadDataSuit) insertData(t *TableInfo, start, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	var err error
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	for i := start; i < end; i++ {
		sql := t.insertSQL(i)
		_, err = txn.Exec(sql)
		if err != nil {
			return err
		}
		if (i-start)%100 == 1 {
			err = txn.Commit()
			if err != nil {
				return err
			}
			txn, err = db.Begin()
			if err != nil {
				return err
			}
		}
		atomic.AddInt64(&c.insertCount, 1)
	}
	return txn.Commit()
}

func (s *LoadDataSuit) checkTableExist(db *sql.DB, t *TableInfo, rows int) bool {
	colNames := t.getColumnNames()
	query := fmt.Sprintf("select %v from %v limit 1", strings.Join(colNames, ","), t.DBTableName())
	_, err := db.Exec(query)
	if err != nil {
		fmt.Printf("table %v doesn't exists, query error: %v\n", t.DBTableName(), err)
		return false
	}
	query = fmt.Sprintf("select count(1) from %v", t.DBTableName())
	valid := true
	err = util.QueryRows(db, query, func(row, cols []string) error {
		if len(row) != 1 {
			valid = false
			return nil
		}
		cnt, _ := strconv.Atoi(row[0])
		valid = cnt == rows
		if !valid {
			fmt.Printf("table %v current rows is %v, expected rows id %v\n",
				t.DBTableName(), cnt, rows)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("table %v rows count error: %v\n", t.DBTableName(), err)
		return false
	}
	return valid
}

func (t *TableInfo) getColumnNames() []string {
	names := []string{}
	for _, col := range t.Columns {
		names = append(names, col.Name)
	}
	return names
}

func (t *TableInfo) createSQL() string {
	sql := fmt.Sprintf("CREATE TABLE `%s` (", t.TableName)
	cols := t.Columns
	for i, col := range cols {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` %s", col.Name, col.getDefinition())
	}
	for i, idx := range t.Indexs {
		switch idx.Tp {
		case NormalIndex:
			sql += fmt.Sprintf(", index idx%v (%v)", i, strings.Join(idx.Columns, ","))
		case UniqueIndex:
			sql += fmt.Sprintf(", unique index idx%v (%v)", i, strings.Join(idx.Columns, ","))
		case PrimaryKey:
			sql += fmt.Sprintf(", primary key (%v)", strings.Join(idx.Columns, ","))
		}
	}
	sql += ")"
	return sql
}

func (t *TableInfo) insertSQL(num int) string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString(fmt.Sprintf("insert into %v values (", t.DBTableName()))
	for i, col := range t.Columns {
		if i > 0 {
			buf.WriteString(",")
		}
		v := col.seqValue(int64(num))
		buf.WriteString(fmt.Sprintf("'%v'", v))
	}
	buf.WriteString(")")
	return buf.String()
}

func (t *TableInfo) DBTableName() string {
	return t.DBName + "." + t.TableName
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
