package test_case

import (
	"fmt"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

type ReadWriteConflict struct {
	cfg *config.Config

	probability int
	interval    int64
	conflictErr int64
}

func NewReadWriteConflict(cfg *config.Config) *ReadWriteConflict {
	return &ReadWriteConflict{
		cfg: cfg,
	}
}

func (c *ReadWriteConflict) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	c.cfg.DBName = "read_write_conflict"
	prepareSQLs := []string{
		"drop table if exists t",
		"create table t (id int, name varchar(10), count bigint, unique index (id))",
	}
	err := prepare(db, c.cfg.DBName, prepareSQLs)
	if err != nil {
		return err
	}
	for i := 0; i < c.cfg.Concurrency; i += 2 {
		go func() {
			err := c.update()
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
		go func() {
			err := c.read()
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

func (c *ReadWriteConflict) update() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		id := rand.Intn(c.probability)
		sql := fmt.Sprintf("insert into t values (%v,'aaa', %v) on duplicate key update count=count+1;", id, 1)
		_, err := db.Exec(sql)
		if err != nil {
			if strings.Contains(err.Error(), "Write conflict") {
				atomic.AddInt64(&c.conflictErr, 1)
				continue
			}
			return err
		}
	}
}

func (c *ReadWriteConflict) read() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		id := rand.Intn(c.probability)
		sql := fmt.Sprintf("select * from t where id = %v", id)
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
	}
}

func (c *ReadWriteConflict) print() error {
	start := time.Now()
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		time.Sleep(time.Second * time.Duration(c.interval))
		query := fmt.Sprintf("select avg(query_time),count(*) from information_schema.cluster_slow_query where db='%s' and query like 'select * from t where id%%' and time > '%s' and time < now()", c.cfg.DBName, util.FormatTimeForQuery(start))
		err := util.QueryAndPrint(db, query)
		if err != nil {
			return err
		}
		fmt.Println("------------------------")
		query = fmt.Sprintf("select Time, Query_time, Parse_time, Compile_time, Rewrite_time, Plan from information_schema.cluster_slow_query where db='%s' and query like 'select * from t where id%%' and succ = true and time > '%s' and time < now() order by time desc limit 1", c.cfg.DBName, util.FormatTimeForQuery(start))
		err = util.QueryAndPrint(db, query)
		if err != nil {
			return err
		}
		fmt.Println("------------------------")
		fmt.Printf("conflict error count: %v \n", atomic.LoadInt64(&c.conflictErr))
		fmt.Printf("---------------------------[ END ]-------------------------\n\n")
	}
}

func (c *ReadWriteConflict) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "read-write-conflict",
		Short:        "test read-write conflict case",
		Long:         `test for read-write conflict case`,
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.probability, "probability", "", 100, "conflict probability, rand( n )")
	cmd.Flags().Int64VarP(&c.interval, "interval", "", 1, "print message interval seconds")
	return cmd
}

func (c *ReadWriteConflict) RunE(cmd *cobra.Command, args []string) error {
	fmt.Printf("probability: %v\nconcurrency: %v\n", c.probability, c.cfg.Concurrency)
	return c.Run()
}
