package test_case

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/crazycs520/testutil/cmd"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BenchListPartitionTable struct {
	cfg *config.Config

	partitionNum      int
	partitionValueNum int
	conditionNum      int
	rows              int
	Type              string
	maxNum            int

	qps         int64
	avgTime     int64
	wg          sync.WaitGroup
	insertCount int64

	cases map[string]benchListTestCase
}

func NewBenchListPartitionTable(cfg *config.Config) cmd.CMDGenerater {
	return &BenchListPartitionTable{
		cfg:   cfg,
		cases: make(map[string]benchListTestCase),
	}
}

func (c *BenchListPartitionTable) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "bench-list-column",
		Short:        "bench test for select list columns partition",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	tpComment := []string{}
	for tp, ca := range c.cases {
		tpComment = append(tpComment, tp+": "+ca.Comment())
	}
	sort.Strings(tpComment)

	randSelect := &benchRandSelect{c}
	c.registerBench(randSelect)
	simpleSelect := &benchSimpleSelect{randSelect}
	c.registerBench(simpleSelect)
	pointGet := &benchPointGet{c}
	c.registerBench(pointGet)
	preparePointGet := &benchPreparePointGet{pointGet}
	c.registerBench(preparePointGet)

	simpleDelete := &benchSimpleDelete{randSelect}
	c.registerBench(simpleDelete)

	simpleBatchDelete := &benchSimpleBatchDelete{randSelect}
	c.registerBench(simpleBatchDelete)

	simpleBatchDeleteIn := &benchSimpleBatchDeleteIn{randSelect}
	c.registerBench(simpleBatchDeleteIn)

	cmd.Flags().IntVarP(&c.partitionNum, "partition-num", "", 1000, "the partition number")
	cmd.Flags().IntVarP(&c.partitionValueNum, "partition-value-num", "", 100, "the values of one partition")
	cmd.Flags().IntVarP(&c.conditionNum, "condition-num", "", 10, "the condition num of the bench sql")
	cmd.Flags().IntVarP(&c.rows, "rows", "", 0, "the table rows")
	cmd.Flags().StringVarP(&c.Type, "type", "", randSelect.Name(), "bench type:\n"+strings.Join(tpComment, "\n"))
	return cmd
}

func (c *BenchListPartitionTable) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *BenchListPartitionTable) Run() error {
	ca, ok := c.cases[c.Type]
	if !ok {
		return fmt.Errorf("unknow type: %v", c.Type)
	}
	err := ca.prepare()
	if err != nil {
		return err
	}
	err = ca.bench()
	if err != nil {
		return err
	}
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("qps: %v, avg: %v\n", atomic.LoadInt64(&c.qps), time.Duration(atomic.LoadInt64(&c.avgTime)).String())
			atomic.StoreInt64(&c.qps, 0)
		}
	}()
	c.wg.Wait()
	return nil
}

func (c *BenchListPartitionTable) exec(genSQL func() string) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	count := int64(0)
	start := time.Now()
	for {
		count++
		sql := genSQL()
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
		if count > 1000 || time.Since(start) > time.Second {
			atomic.StoreInt64(&c.avgTime, int64(time.Since(start))/count)
			count = 0
			start = time.Now()
		}
		atomic.AddInt64(&c.qps, 1)
	}
}

func (c *BenchListPartitionTable) registerBench(ca benchListTestCase) {
	if ca == nil {
		return
	}
	c.cases[ca.Name()] = ca
}

type benchListTestCase interface {
	Name() string
	Comment() string
	prepare() error
	bench() error
	benchSQL() string
}

type benchRandSelect struct {
	*BenchListPartitionTable
}

func (c *benchRandSelect) Name() string {
	return "rand-select"
}

func (c *benchRandSelect) Comment() string {
	return "bench rand select, without index"
}

func (c *benchRandSelect) checkTableExist(db *sql.DB) bool {
	tableName := "t"
	query := fmt.Sprintf("select id, a, b,name from %v limit 1", tableName)
	_, err := db.Exec(query)
	if err != nil {
		fmt.Printf("table %v doesn't exists\n", tableName)
		return false
	}
	query = fmt.Sprintf("select count(1) from %v", tableName)
	valid := true
	err = util.QueryRows(db, query, func(row, cols []string) error {
		if len(row) != 1 {
			valid = false
			return nil
		}
		cnt, _ := strconv.Atoi(row[0])
		valid = cnt == c.rows
		if !valid {
			fmt.Printf("table %v current rows is %v, expected rows id %v\n",
				tableName, cnt, c.rows)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("table %v rows count error: %v\n", tableName, err)
		return false
	}
	return valid
}

func (c *benchRandSelect) prepare() error {
	c.cfg.DBName = "bench_test"
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	c.maxNum = c.rows
	exist := c.checkTableExist(db)
	if exist {
		return nil
	}

	create := bytes.Buffer{}
	create.WriteString("create table t (id int,a int,b int, name varchar(10)) partition by list columns (id, a, b) (")
	if c.partitionNum < 1 {
		c.partitionNum = 1
	}
	num := 0
	for i := 0; i < c.partitionNum; i++ {
		if i > 0 {
			create.WriteString(",")
		}
		create.WriteString(fmt.Sprintf("partition p%v values in (", i))
		for j := 0; j < c.partitionValueNum; j++ {
			if j > 0 {
				create.WriteString(",")
			}
			create.WriteString(fmt.Sprintf("(%[1]v,%[1]v,%[1]v)", num))
			num++
		}
		create.WriteString(")")
	}
	c.maxNum = num
	create.WriteString(")")
	prepareSQLs := []string{
		"drop table if exists t",
		create.String(),
	}
	err := prepare(db, c.cfg.DBName, prepareSQLs)
	if err != nil {
		return err
	}
	if c.rows == 0 {
		return nil
	}
	return c.loadData()
}

func (c *benchRandSelect) loadData() error {
	// prepare data.
	step := (c.rows / c.cfg.Concurrency) + 1
	if step < 10 {
		return c.insertData(0, c.rows)
	}
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		start := i * step
		end := (i + 1) * step
		if end > c.rows {
			end = c.rows
		}
		go func() {
			defer wg.Done()
			err := c.insertData(start, end)
			if err != nil {
				fmt.Printf("insert data error: %v\n", err)
			}
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("inserted rows: %v \n", atomic.LoadInt64(&c.insertCount))
			}
		}
	}()
	wg.Wait()
	cancel()
	return nil
}

func (c *benchRandSelect) insertData(start, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for i := start; i < end; i++ {
		letter := 'a' + byte(i%26)
		name := make([]byte, 10)
		for i := range name {
			name[i] = letter
		}
		query := fmt.Sprintf("insert into t values (%v,%v,%v,'%v')", i, i, i, string(name))
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
		atomic.AddInt64(&c.insertCount, 1)
	}
	return nil
}

func (c *benchRandSelect) benchSQL() string {
	sql := bytes.Buffer{}
	sql.WriteString("select * from t where ")
	colName := []string{"id", "a", "b"}
	for i := 0; i < c.conditionNum; i++ {
		if i > 0 {
			if i%2 == 0 {
				sql.WriteString(" and ")
			} else {
				sql.WriteString(" or ")
			}
		}
		cn := rand.Intn(len(colName))
		col := colName[cn]
		v := rand.Intn(c.maxNum * 2)
		sql.WriteString(fmt.Sprintf(" %v = %v ", col, v))
	}
	return sql.String()
}

func (c *BenchListPartitionTable) bench(genSQL func() string) error {
	for i := 0; i < c.cfg.Concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			err := c.exec(func() string {
				return genSQL()
			})
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	return nil
}

func (c *BenchListPartitionTable) benchInTxnAndRollback(genSQL func() string) error {
	for i := 0; i < c.cfg.Concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()
			count := int64(0)
			start := time.Now()
			for {
				count++
				txn, err := db.Begin()
				if err != nil {
					fmt.Println(err.Error())
				}
				sql := genSQL()
				_, err = txn.Exec(sql)
				if err != nil {
					fmt.Println(err.Error())
				}
				err = txn.Rollback()
				if err != nil {
					fmt.Println(err.Error())
				}
				if count > 1000 || time.Since(start) > time.Second {
					atomic.StoreInt64(&c.avgTime, int64(time.Since(start))/count)
					count = 0
					start = time.Now()
				}
				atomic.AddInt64(&c.qps, 1)
			}
		}()
	}
	return nil
}

func (c *benchRandSelect) bench() error {
	return c.BenchListPartitionTable.bench(c.benchSQL)
}

type benchSimpleSelect struct {
	*benchRandSelect
}

func (c *benchSimpleSelect) Name() string {
	return "simple-select"
}

func (c *benchSimpleSelect) Comment() string {
	return "bench simple select, without index, such as: select * from t where id=1"
}

func (c *benchSimpleSelect) benchSQL() string {
	sql := bytes.Buffer{}
	sql.WriteString("select * from t where ")
	colName := []string{"id", "a", "b"}
	colIdx := rand.Intn(len(colName))
	col := colName[colIdx]
	v := rand.Intn(c.maxNum * 2)
	sql.WriteString(fmt.Sprintf(" %v = %v ", col, v))
	return sql.String()
}

func (c *benchSimpleSelect) bench() error {
	return c.BenchListPartitionTable.bench(c.benchSQL)
}

type benchPointGet struct {
	*BenchListPartitionTable
}

func (c *benchPointGet) Name() string {
	return "point-get"
}

func (c *benchPointGet) Comment() string {
	return "bench point get"
}

func (c *benchPointGet) prepare() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	c.cfg.DBName = "bench_test"
	create := bytes.Buffer{}
	create.WriteString("create table t (id int,a int,b int, name varchar(10), unique index (id)) partition by list columns (id) (")
	if c.partitionNum < 1 {
		c.partitionNum = 1
	}
	num := 0
	for i := 0; i < c.partitionNum; i++ {
		if i > 0 {
			create.WriteString(",")
		}
		create.WriteString(fmt.Sprintf("partition p%v values in (", i))
		for j := 0; j < c.partitionValueNum; j++ {
			if j > 0 {
				create.WriteString(",")
			}
			create.WriteString(fmt.Sprintf("(%[1]v)", num))
			num++
		}
		create.WriteString(")")
	}
	c.maxNum = num
	create.WriteString(")")
	prepareSQLs := []string{
		"drop table if exists t",
		create.String(),
	}
	err := prepare(db, c.cfg.DBName, prepareSQLs)
	if err != nil {
		return err
	}
	if c.rows == 0 {
		return nil
	}
	randSelect := &benchRandSelect{c.BenchListPartitionTable}
	return randSelect.loadData()
}

func (c *benchPointGet) benchSQL() string {
	return fmt.Sprintf("select * from t where id = %v", rand.Intn(c.maxNum))
}

func (c *benchPointGet) bench() error {
	return c.BenchListPartitionTable.bench(c.benchSQL)
}

type benchPreparePointGet struct {
	*benchPointGet
}

func (c *benchPreparePointGet) Name() string {
	return "prepare-point-get"
}

func (c *benchPreparePointGet) Comment() string {
	return "bench point get with prepare"
}

func (c *benchPreparePointGet) benchSQL() string {
	return ""
}

func (c *benchPreparePointGet) bench() error {
	for i := 0; i < c.cfg.Concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()
			stmt, err := db.Prepare("select * from t where id = ?")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			for {
				_, err := stmt.Exec(rand.Intn(c.maxNum * 2))
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				atomic.AddInt64(&c.qps, 1)
			}
		}()
	}
	return nil
}

type benchSimpleDelete struct {
	*benchRandSelect
}

func (c *benchSimpleDelete) Name() string {
	return "simple-delete"
}

func (c *benchSimpleDelete) Comment() string {
	return "bench simple delete 1 row, without index, such as: delete from t where id=1"
}

func (c *benchSimpleDelete) benchSQL() string {
	sql := bytes.Buffer{}
	sql.WriteString("delete from t where ")
	colName := []string{"id", "a", "b"}
	colIdx := rand.Intn(len(colName))
	col := colName[colIdx]
	v := rand.Intn(c.maxNum * 2)
	sql.WriteString(fmt.Sprintf(" %v = %v ", col, v))
	return sql.String()
}

func (c *benchSimpleDelete) bench() error {
	return c.BenchListPartitionTable.bench(c.benchSQL)
}

type benchSimpleBatchDelete struct {
	*benchRandSelect
}

func (c *benchSimpleBatchDelete) Name() string {
	return "simple-batch-delete"
}

func (c *benchSimpleBatchDelete) Comment() string {
	return "bench simple delete a batch rows, without index, such as: delete from t where delete from t where id > 100 and id < 200"
}

func (c *benchSimpleBatchDelete) benchSQL() string {
	sql := bytes.Buffer{}
	sql.WriteString("delete from t where id > 100 and id < 200")
	return sql.String()
}

func (c *benchSimpleBatchDelete) bench() error {
	return c.BenchListPartitionTable.benchInTxnAndRollback(func() string {
		return c.benchSQL()
	})
}

type benchSimpleBatchDeleteIn struct {
	*benchRandSelect
}

func (c *benchSimpleBatchDeleteIn) Name() string {
	return "simple-batch-delete-in"
}

func (c *benchSimpleBatchDeleteIn) Comment() string {
	return "bench simple delete a batch rows, without index, such as: delete from t where delete from t where id in (1,2,3,4,5,6,7,8,9,10)"
}

func (c *benchSimpleBatchDeleteIn) benchSQL() string {
	sql := bytes.Buffer{}
	v := rand.Intn(c.rows)
	sql.WriteString("delete from t where id in (")
	for i := 0; i < 10; i++ {
		if i > 0 {
			sql.WriteString(",")
		}
		sql.WriteString(strconv.Itoa(v + i))
	}
	sql.WriteString(")")
	return sql.String()
}

func (c *benchSimpleBatchDeleteIn) bench() error {
	return c.BenchListPartitionTable.benchInTxnAndRollback(func() string {
		return c.benchSQL()
	})
}
