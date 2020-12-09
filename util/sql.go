package util

import (
	"database/sql"
	"fmt"
	"github.com/crazycs520/testutil/config"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

func GetSQLCli(cfg *config.Config) *sql.DB {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		fmt.Println("can not connect to database. err: " + err.Error())
		os.Exit(1)
	}
	db.SetMaxOpenConns(1)
	db.Exec("use " + cfg.DBName)
	return db
}

func QueryRows(Engine *sql.DB, SQL string, fn func(row, cols []string) error) (err error) {
	rows, err := Engine.Query(SQL)
	if err == nil {
		defer rows.Close()
	}

	if err != nil {
		return err
	}

	cols, err1 := rows.Columns()
	if err1 != nil {
		return err1
	}
	// Read all rows.
	var actualRows [][]string
	for rows.Next() {

		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return err1
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				val := string(raw)
				result[i] = val
			}
		}

		actualRows = append(actualRows, result)
	}
	if err = rows.Err(); err != nil {
		return err
	}

	for _, row := range actualRows {
		err := fn(row, cols)
		if err != nil {
			return err
		}
	}
	return nil
}

func QueryAllRows(Engine *sql.DB, SQL string) ([]string, [][]string, error) {
	rows, err := Engine.Query(SQL)
	if err == nil {
		defer rows.Close()
	}

	if err != nil {
		return nil, nil, err
	}

	cols, err1 := rows.Columns()
	if err1 != nil {
		return nil, nil, err1
	}
	// Read all rows.
	var actualRows [][]string
	for rows.Next() {

		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return nil, nil, err1
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				val := string(raw)
				result[i] = "'" + val + "'"
			}
		}

		actualRows = append(actualRows, result)
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}
	return cols, actualRows, nil
}

func QueryAndPrint(db *sql.DB, sql string) error {
	cols, rows, err := QueryAllRows(db, sql)
	if err != nil {
		return err
	}
	length := 0
	for _, row := range rows {
		for _, c := range row {
			length += len(c)
		}
		break
	}
	if length < 250 {
		// print short rows
		fmt.Println(strings.Join(cols, "\t\t"))
		for _, row := range rows {
			fmt.Println(strings.Join(row, " "))
		}
		fmt.Println()
		return nil
	}
	for i, row := range rows {
		fmt.Printf("***************************[ %v. row ]***************************\n", i)
		for j, c := range row {
			for {
				s := strings.Replace(c, "\t", "  ", -1)
				s = strings.Replace(s, "    ", "  ", -1)
				if s == c {
					break
				}
				c = s
			}
			if strings.HasPrefix(c, "'") && strings.HasSuffix(c, "'") && len(c) >= 2 {
				l := len(c)
				c = c[1 : l-1]
			}
			fmt.Printf("%v: ", cols[j])
			if len(c) > 200 {
				fmt.Printf("\n%v\n", c)
			} else {
				fmt.Printf("%v\n", c)
			}
		}
	}
	fmt.Println()
	return nil
}

const TimeFSPFormat = "2006-01-02 15:04:05.000000"

func FormatTimeForQuery(t time.Time) string {
	return t.Format(TimeFSPFormat)
}
