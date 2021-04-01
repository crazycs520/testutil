package test_case

import (
	"fmt"
	"github.com/crazycs520/testutil/cmd"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/data"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
	"time"
)

type IndexHashJoinPlan struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	query       string
	rows        int
	interval    int64
	insertCount int64
}

func NewIndexHashJoinPlan(cfg *config.Config) cmd.CMDGenerater {
	return &IndexHashJoinPlan{
		cfg: cfg,
	}
}

func (c *IndexHashJoinPlan) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "index-hash-join",
		Short:        "stress test for index hash join.",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&c.query, "sql", "", "", "execute query")
	cmd.Flags().IntVarP(&c.rows, "rows", "", 100000, "test table rows")
	cmd.Flags().Int64VarP(&c.interval, "interval", "", 1, "print message interval seconds")
	return cmd
}

func (c *IndexHashJoinPlan) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *IndexHashJoinPlan) prepare() error {
	c.cfg.DBName = "stress_test"
	c.tableName = "t"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
		{
			Name:     "a",
			Tp:       "bigint",
			MinValue: "0",
			MaxValue: "10000",
		},
		{
			Name: "b",
			Tp:   "bigint",
		},
		{
			Name: "c",
			Tp:   "timestamp(6)",
		},
		{
			Name: "d",
			Tp:   "varchar(100)",
		},
	}, []data.IndexInfo{
		{
			Tp:      data.NormalIndex,
			Columns: []string{"a"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuit(c.cfg)
	return load.Prepare(tblInfo, c.rows, 2000)
}

func (c *IndexHashJoinPlan) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	fmt.Println("finish prepare data")
	for i := 0; i < c.cfg.Concurrency; i++ {
		go func() {
			err := c.exec(func() string {
				if c.query != "" {
					return c.query
				}
				return fmt.Sprintf("select /*+ INL_HASH_JOIN(t2,t1) */ count(*) from %[1]v t1 join %[1]v t2 where t1.a=t2.b;", c.tblInfo.DBTableName())
			})
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	var likeCond string
	if c.query != "" {
		likeCond = c.query + "%%"
	} else {
		likeCond = fmt.Sprintf("select /*+ INL_HASH_JOIN(t2,t1) */ count(*) from %[1]v t1 join%%", c.tblInfo.DBTableName())
	}
	err = util.PrintSlowQueryInfo(likeCond, time.Second, c.cfg)
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

func (c *IndexHashJoinPlan) exec(genSQL func() string) error {
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
