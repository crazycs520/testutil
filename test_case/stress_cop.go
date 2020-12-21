package test_case

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/crazycs520/testutil/cmd"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type StressCop struct {
	cfg       *config.Config
	tableName string

	rows        int
	interval    int64
	insertCount int64
}

func NewStressCop(cfg *config.Config) cmd.CMDGenerater {
	return &StressCop{
		cfg: cfg,
	}
}

func (c *StressCop) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "stress-cop",
		Short:        "stress test for tikv coprocessor, should disable cop-cache first.",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, "rows", "", 100000, "test table rows")
	cmd.Flags().Int64VarP(&c.interval, "interval", "", 1, "print message interval seconds")
	return cmd
}

func (c *StressCop) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *StressCop) queryTableName() string {
	return c.cfg.DBName + "." + c.tableName
}

func (c *StressCop) checkTableExist(db *sql.DB) bool {
	query := fmt.Sprintf("select id, name, count, age from %v limit 1", c.queryTableName())
	_, err := db.Exec(query)
	if err != nil {
		fmt.Printf("table %v doesn't exists\n", c.queryTableName())
		return false
	}
	query = fmt.Sprintf("select count(1) from %v", c.queryTableName())
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
				c.queryTableName(), cnt, c.rows)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("table %v rows count error: %v\n", c.queryTableName(), err)
		return false
	}
	return valid
}

func (c *StressCop) prepare() error {
	c.cfg.DBName = "stress_test"
	c.tableName = "t_cop"

	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	exist := c.checkTableExist(db)
	if exist {
		return nil
	}
	prepareSQLs := []string{
		fmt.Sprintf("drop table if exists %v", c.tableName),
		fmt.Sprintf("create table %v (id int, name varchar(10), count bigint, age int, primary key (id))", c.tableName),
	}
	err := prepare(db, c.cfg.DBName, prepareSQLs)
	if err != nil {
		return err
	}
	// split region.
	if c.rows > 100000 {
		split := fmt.Sprintf("split table %v between (0) and (%v) regions %v;", c.queryTableName(), c.rows, c.rows/100000)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := db.ExecContext(ctx, split)
		if err != nil {
			fmt.Printf("split region error: %v\n", err)
		}
		cancel()
	}

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

func (c *StressCop) insertData(start, end int) error {
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
		query := fmt.Sprintf("insert into %v values (%v,'%v',%v,%v)", c.queryTableName(), i, string(name), i, i)
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
		atomic.AddInt64(&c.insertCount, 1)
	}
	return nil
}

func (c *StressCop) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	fmt.Println("finish prepare data")
	for i := 0; i < c.cfg.Concurrency; i++ {
		go func() {
			err := c.exec(func() string {
				return fmt.Sprintf("select sum(id*count*age) from %v", c.queryTableName())
			})
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	err = c.print()
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

func (c *StressCop) exec(genSQL func() string) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		sql := genSQL()
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
	}
}

func (c *StressCop) print() error {
	start := time.Now()
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		time.Sleep(time.Second * time.Duration(c.interval))
		query := fmt.Sprintf("select avg(query_time),count(*) from information_schema.cluster_slow_query where db='%s' and query like 'select sum(id%%' and time > '%s' and time < now()", c.cfg.DBName, util.FormatTimeForQuery(start))
		err := util.QueryAndPrintWithIgnoreZeroValue(db, query)
		if err != nil {
			return err
		}
		fmt.Println("------------------------")
		query = fmt.Sprintf("select * from information_schema.cluster_slow_query where db='%s' and query like 'select sum(id%%' and succ = true and time > '%s' and time < now() order by time desc limit 1", c.cfg.DBName, util.FormatTimeForQuery(start))
		err = util.QueryAndPrintWithIgnoreZeroValue(db, query)
		if err != nil {
			return err
		}
		fmt.Printf("---------------------------[ END ]-------------------------\n\n")
	}
}
