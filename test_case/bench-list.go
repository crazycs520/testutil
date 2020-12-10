package test_case

import (
	"bytes"
	"fmt"
	"github.com/crazycs520/testutil/cmd"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type BenchListPartitionTable struct {
	cfg *config.Config

	partitionNum      int
	partitionValueNum int
	conditionNum      int
	maxNum            int

	qps int64
}

func NewBenchListPartitionTable(cfg *config.Config) cmd.CMDGenerater {
	return &BenchListPartitionTable{
		cfg: cfg,
	}
}

func (c *BenchListPartitionTable) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "bench-list-column",
		Short:        "bench test for select list columns partition",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.partitionNum, "partition-num", "", 1000, "the partition number")
	cmd.Flags().IntVarP(&c.partitionValueNum, "partition-value-num", "", 100, "the values of one partition")
	cmd.Flags().IntVarP(&c.conditionNum, "condition-num", "", 10, "the condition num of the bench sql")
	return cmd
}

func (c *BenchListPartitionTable) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *BenchListPartitionTable) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	c.cfg.DBName = "bench_test"
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
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.exec(func() string {
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
			})
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("qps: %v\n", atomic.LoadInt64(&c.qps))
			atomic.StoreInt64(&c.qps, 0)
		}
	}()
	wg.Wait()
	return nil
}

func (c *BenchListPartitionTable) exec(genSQL func() string) error {
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
		atomic.AddInt64(&c.qps, 1)
	}
}
