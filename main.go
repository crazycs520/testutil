package main

import (
	"github.com/crazycs520/testutil/cmd"
	"log"
)

func main() {
	app := cmd.NewApp()
	err := app.Cmd().Execute()
	if err != nil {
		log.Fatalln(err)
	}
}
