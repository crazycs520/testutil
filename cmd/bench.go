package cmd

import (
	"database/sql"
	"fmt"
	"github.com/spf13/cobra"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type BenchSQL struct {
	*App
	query  string
	ignore bool

	valMin     int64
	valMax     int64
	currentVal int64

	totalQPS int64
}

const randValueStr = "#rand-val"
const seqValueStr = "#seq-val"

func (b *BenchSQL) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "bench",
		Short:        "bench the sql",
		Long:         `benchmark the sql statement`,
		RunE:         b.RunE,
		SilenceUsage: true,
	}

	//cmd.Flags().IntVar(&app.EstimateTableRows, "new-table-row", 0, "estimate need be split table rows")
	cmd.Flags().StringVarP(&b.query, "sql", "", "", "bench sql statement")
	cmd.Flags().BoolVarP(&b.ignore, "ignore", "", false, "should ignore error?")
	cmd.Flags().Int64VarP(&b.valMin, "valmin", "", 0, randValueStr +"/"+seqValueStr +" min val")
	cmd.Flags().Int64VarP(&b.valMax, "valmax", "", 0, randValueStr +"/"+seqValueStr + " max val")

	return cmd
}

func (b *BenchSQL) validateParas(cmd *cobra.Command) error {
	msg := "need specify `%s` parameter"
	var err error
	if b.query == "" {
		err = fmt.Errorf(msg, "sql")
	}
	return err
}

func (b *BenchSQL) RunE(cmd *cobra.Command, args []string) error {
	if err := b.validateParas(cmd); err != nil {
		fmt.Println(err.Error())
		fmt.Printf("-----------[ help ]-----------\n")
		return cmd.Help()
	}
	fmt.Printf("sql: %v\nconcurrency: %v\n", b.replaceSQL(b.query), b.cfg.Concurrency)
	//b.currentVal = b.valMin
	for i := 0; i < b.cfg.Concurrency; i++ {
		go b.benchSql()
	}
	start := time.Now()
	for {
		time.Sleep(1 * time.Second)
		fmt.Printf("qps: %v\n", int64(float64(atomic.LoadInt64(&b.totalQPS))/time.Since(start).Seconds()))
	}
}

func (b *BenchSQL) benchSql() {
	db := b.GetSQLCli()
	sqlStr := b.query
	for {
		batch := 20
		var err error
		var rows *sql.Rows
		for i := 0; i < batch; i++ {
			sqlStr = b.replaceSQL(b.query)
			if strings.HasPrefix(strings.ToLower(sqlStr), "select") {
				rows, err = db.Query(sqlStr)
			} else {
				_, err = db.Exec(sqlStr)
			}
			if err != nil && !b.ignore {
				fmt.Printf("exec: %v, err: %v\n", sqlStr, err)
				os.Exit(-1)
			}
			if rows != nil {
				for rows.Next() {
				}
				rows.Close()
			}
		}
		atomic.AddInt64(&b.totalQPS, int64(batch))
	}
}

func(b *BenchSQL) replaceSQL(sql string) string {
	if b.valMin == b.valMax {
		return sql
	}
	if strings.Contains(sql, randValueStr) {
		rand.Seed(time.Now().UnixNano())
		v := rand.Intn(int(b.valMax-b.valMin+1)) + int(b.valMin)
		return strings.Replace(sql,randValueStr,strconv.Itoa(v),-1)
	}
	if strings.Contains(sql, seqValueStr) {
		v := atomic.AddInt64(&b.currentVal,1)
		return strings.Replace(sql,seqValueStr,strconv.Itoa(int(v)),-1)
	}

	return sql
}
