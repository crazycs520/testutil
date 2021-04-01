package test_case

import (
	"fmt"
	"github.com/crazycs520/testutil/cmd"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

type TraverseGraph struct {
	cfg *config.Config

	probability int
	interval    int64
	conflictErr int64
	cmd         string
}

func NewTraverseGraph(cfg *config.Config) cmd.CMDGenerater {
	return &TraverseGraph{
		cfg: cfg,
	}
}

func (c *TraverseGraph) prepare() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	c.cfg.DBName = "test"
	prepareSQLs := []string{
		"drop table if exists p,f",
		"create tag  p (vertex_id bigint, name varchar(32), age int, unique index idx(name))",
		"create edge f (`from` bigint, `to` bigint, time year)",
	}
}

func (c *TraverseGraph) Run() error {

	if c.cmd == "prepare" {
		err := c.prepare(db, c.cfg.DBName, prepareSQLs)
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

func (c *TraverseGraph) update() error {
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

func (c *TraverseGraph) print() error {
	start := time.Now()
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		time.Sleep(time.Second * time.Duration(c.interval))
		query := fmt.Sprintf("select avg(query_time),count(*) from information_schema.cluster_slow_query where db='%s' and query like 'insert into t%% on duplicate key update count%%' and time > '%s' and time < now()", c.cfg.DBName, util.FormatTimeForQuery(start))
		err := util.QueryAndPrint(db, query)
		if err != nil {
			return err
		}
		fmt.Println("------------------------")
		query = fmt.Sprintf("select Time, Query_time, Parse_time, Compile_time, Rewrite_time, Prewrite_time, Resolve_lock_time, Commit_backoff_time, Backoff_types, Get_commit_ts_time, Commit_time, Txn_retry, Plan from information_schema.cluster_slow_query where db='%s' and query like 'insert into t%% on duplicate key update count%%' and succ = true and time > '%s' and time < now() order by time desc limit 1", c.cfg.DBName, util.FormatTimeForQuery(start))
		err = util.QueryAndPrint(db, query)
		if err != nil {
			return err
		}
		fmt.Println("------------------------")
		fmt.Printf("conflict error count: %v \n", atomic.LoadInt64(&c.conflictErr))
		fmt.Printf("---------------------------[ END ]-------------------------\n\n")
	}
}

func (c *TraverseGraph) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-conflict",
		Short:        "test write conflict case",
		Long:         `test for write conflict case`,
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.probability, "probability", "", 100, "conflict probability, rand( n )")
	cmd.Flags().Int64VarP(&c.interval, "interval", "", 1, "print message interval seconds")
	cmd.Flags().StringVarP(&c.cmd, "cmd", "", "prepare", "prepare/run")
	return cmd
}

func (c *TraverseGraph) RunE(cmd *cobra.Command, args []string) error {
	fmt.Printf("probability: %v\nconcurrency: %v\n", c.probability, c.cfg.Concurrency)
	return c.Run()
}
