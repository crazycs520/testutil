package main

import (
	"log"
	"github.com/crazycs520/testutil/cmd"

)

func main() {
	app := &cmd.App{}
	err := app.Cmd().Execute()
	if err != nil {
		log.Fatalln(err)
	}
}

