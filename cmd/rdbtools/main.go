package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/codegangsta/cli"
	"github.com/vrischmann/rdbtools"
)

var (
	wg  sync.WaitGroup
	p   *rdbtools.Parser
	ctx rdbtools.ParserContext
)

func makeContext() {
	ctx = rdbtools.ParserContext{
		DbCh:                make(chan int),
		StringObjectCh:      make(chan rdbtools.StringObject),
		ListMetadataCh:      make(chan rdbtools.ListMetadata),
		ListDataCh:          make(chan interface{}),
		SetMetadataCh:       make(chan rdbtools.SetMetadata),
		SetDataCh:           make(chan interface{}),
		HashMetadataCh:      make(chan rdbtools.HashMetadata),
		HashDataCh:          make(chan rdbtools.StringObject),
		SortedSetMetadataCh: make(chan rdbtools.SortedSetMetadata),
		SortedSetEntriesCh:  make(chan rdbtools.SortedSetEntry),
	}
}

func launchParsing(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	p = rdbtools.NewParser(ctx)

	return p.Parse(f)
}

func memoryStatistics(c *cli.Context) {
	var format string
	if c.String("format") != "" {
		format = c.String("format")
	}

	if !c.Args().Present() {
		fmt.Println("No RDB filename provided")
		os.Exit(1)
	}

	// Make the context
	makeContext()

	wg.Add(1)
	go computeMemoryStatistics(format)

	if err := launchParsing(c.Args().Get(0)); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}

	wg.Wait()
}

func main() {
	app := cli.NewApp()
	app.Name = "rdbtools"
	app.Usage = "Extract information from Redis RDB snapshots"
	app.Commands = []cli.Command{
		{
			Name:  "memstats",
			Usage: "Compute memory usage statistics",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "format", Usage: "The output format"},
			},
			Action: memoryStatistics,
		},
	}

	app.Run(os.Args)
}
