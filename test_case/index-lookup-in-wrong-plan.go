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

type IndexLookUpWrongPlan struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	rows        int
	interval    int64
	insertCount int64
}

func NewIndexLookUpWrongPlan(cfg *config.Config) cmd.CMDGenerater {
	return &IndexLookUpWrongPlan{
		cfg: cfg,
	}
}

func (c *IndexLookUpWrongPlan) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "index-lookup",
		Short:        "stress test for index lookup in wrong plan.",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, "rows", "", 100000, "test table rows")
	cmd.Flags().Int64VarP(&c.interval, "interval", "", 1, "print message interval seconds")
	return cmd
}

func (c *IndexLookUpWrongPlan) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *IndexLookUpWrongPlan) prepare() error {
	c.cfg.DBName = "stress_test"
	c.tableName = "t_index_lookup"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
		{
			Name: "a",
			Tp:   "bigint",
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

func (c *IndexLookUpWrongPlan) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	fmt.Println("finish prepare data")
	for i := 0; i < c.cfg.Concurrency; i++ {
		go func() {
			err := c.exec(func() string {
				return fmt.Sprintf("select sum(a*b) from %v use index (idx0) where a < 1000000", c.tblInfo.DBTableName())
			})
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	err = c.print(fmt.Sprintf("select sum(a*b) from %v use index%%", c.tblInfo.DBTableName()))
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

func (c *IndexLookUpWrongPlan) exec(genSQL func() string) error {
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

func (c *IndexLookUpWrongPlan) print(queryLike string) error {
	start := time.Now()
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		time.Sleep(time.Second * time.Duration(c.interval))
		query := fmt.Sprintf("select avg(query_time),count(*) from information_schema.cluster_slow_query where db='%s' and query like '%v' and time > '%s' and time < now()", c.cfg.DBName, queryLike, util.FormatTimeForQuery(start))
		err := util.QueryAndPrintWithIgnoreZeroValue(db, query)
		if err != nil {
			return err
		}
		fmt.Println("------------------------")
		query = fmt.Sprintf("select * from information_schema.cluster_slow_query where db='%s' and query like '%v' and succ = true and time > '%s' and time < now() order by time desc limit 1", c.cfg.DBName, queryLike, util.FormatTimeForQuery(start))
		err = util.QueryAndPrintWithIgnoreZeroValue(db, query)
		if err != nil {
			return err
		}
		fmt.Printf("---------------------------[ END ]-------------------------\n\n")
	}
}
