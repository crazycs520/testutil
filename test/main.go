package main

import (
	"fmt"
	"github.com/crazycs520/testutil/config"
	"log"
	"os"
	"time"
)

func main() {
	cfg := &config.DBConfig{
		Host:     "127.0.0.1",
		Port:     4000,
		User:     "root",
		Password: "",
		DBName:   "test",
	}
	_ = cfg

	file, err := os.OpenFile("/Users/cs/code/goread/src/github.com/pingcap/tidb/tidb-slow-3.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	assertErr(err)
	stat, err := file.Stat()
	assertErr(err)
	fmt.Printf("%v \n", stat.ModTime())
	var t time.Time
	fmt.Printf("%v \n", t.Equal(zeroTime))

	t = time.Now()
	ns := t.UnixNano()
	t1 := time.Unix(0, ns)
	fmt.Println(t.String(), t1.String())
}

var zeroTime = time.Time{}

func assertErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
