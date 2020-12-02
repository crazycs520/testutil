package test_case

import (
	"fmt"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
	"math/rand"
	"sync"
)

type WriteConflict struct {
	cfg config.Config

	probability int
}

func NewWriteConflict(cfg config.Config) *WriteConflict {
	return &WriteConflict{
		cfg: cfg,
	}
}

func (c *WriteConflict) Run() error {
	db := util.GetSQLCli(&c.cfg)
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
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.update()
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	wg.Wait()

	return nil
}

func (c *WriteConflict) update() error {
	db := util.GetSQLCli(&c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		id := rand.Intn(c.probability)
		sql := fmt.Sprintf("insert into t values (%v,'aaa', %v) on duplicate key update count=count+1;", id, 1)
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
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
	return cmd
}

func (c *WriteConflict) RunE(cmd *cobra.Command, args []string) error {
	fmt.Printf("probability: %v\nconcurrency: %v\n", c.probability, c.cfg.Concurrency)
	return c.Run()
}
