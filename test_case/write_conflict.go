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

type WriteConflict struct {
	cfg *config.Config

	probability int
	interval    int64
	conflictErr int64
}

func NewWriteConflict(cfg *config.Config) *WriteConflict {
	return &WriteConflict{
		cfg: cfg,
	}
}

func (c *WriteConflict) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	c.cfg.DBName = "write_conflict"
	prepareSQLs := []string{
		"drop table if exists t",
		"create table t (id int, name varchar(10), count bigint, primary key (id))",
	}
	err := prepare(db, c.cfg.DBName, prepareSQLs)
	if err != nil {
		return err
	}
	for i := 0; i < c.cfg.Concurrency; i++ {
		go func() {
			err := c.update()
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

func (c *WriteConflict) update() error {
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

func (c *WriteConflict) print() error {
	start := time.Now()
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		time.Sleep(time.Second * time.Duration(c.interval))
		query := fmt.Sprintf("select avg(query_time),count(*),max(plan) from information_schema.cluster_slow_query where db='write_conflict' and query like 'insert into t%% on duplicate key update count%%' and time > '%s' and time < now()", util.FormatTimeForQuery(start))
		err := util.QueryAndPrint(db, query)
		if err != nil {
			return err
		}
		fmt.Println("------------------------")
		fmt.Printf("conflict error count: %v \n", atomic.LoadInt64(&c.conflictErr))
		fmt.Printf("---------------------------[ END ]-------------------------\n\n")
	}
}

func (c *WriteConflict) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-conflict",
		Short:        "test write conflict case",
		Long:         `test for write conflict case`,
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.probability, "probability", "", 100, "conflict probability, rand( n )")
	cmd.Flags().Int64VarP(&c.interval, "interval", "", 1, "print message interval seconds")
	return cmd
}

func (c *WriteConflict) RunE(cmd *cobra.Command, args []string) error {
	fmt.Printf("probability: %v\nconcurrency: %v\n", c.probability, c.cfg.Concurrency)
	return c.Run()
}
