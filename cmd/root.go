package cmd

import (
	"database/sql"
	"github.com/crazycs520/testutil/config"
	"github.com/crazycs520/testutil/util"
	"github.com/spf13/cobra"
)

type App struct {
	cfg config.Config
	//Host        string
	//Port        string
	//User        string
	//Password    string
	//DBName      string
	//Concurrency int
}

func (app *App) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "testutil",
		Short:        "testutil uses to do bench and case test",
		SilenceUsage: true,
	}

	cmd.PersistentFlags().StringVarP(&app.cfg.Host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().IntVarP(&app.cfg.Port, "port", "P", 4000, "database service port")
	cmd.PersistentFlags().StringVarP(&app.cfg.User, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.cfg.Password, "password", "p", "", "database user password")
	cmd.PersistentFlags().StringVarP(&app.cfg.DBName, "db", "d", "test", "database name")
	cmd.PersistentFlags().IntVarP(&app.cfg.Concurrency, "concurrency", "f", 5, "app concurrency")

	bench := BenchSQL{App: app}
	cmd.AddCommand(bench.Cmd())

	caseTest := CaseTest{App: app}
	cmd.AddCommand(caseTest.Cmd())
	return cmd
}

func (app *App) GetSQLCli() *sql.DB {
	return util.GetSQLCli(&app.cfg)
}
