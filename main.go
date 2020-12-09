package main

import (
	"github.com/crazycs520/testutil/cmd"
	_ "github.com/crazycs520/testutil/test_case"
	"log"
)

func main() {
	app := cmd.NewApp()
	err := app.Cmd().Execute()
	if err != nil {
		log.Fatalln(err)
	}
}
