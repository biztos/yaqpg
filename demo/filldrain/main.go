// filldrain - fill and then batch-drain the queue
//
// Uses same queue name as fillerup.go so can use this to drain those items.
package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/biztos/yaqpg"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("need count arg for filldrain")
	}
	count, err := strconv.ParseInt(os.Args[1], 0, 64)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("FILL/DRAIN:", count)

	queue := yaqpg.MustStartNamedQueue("filler")
	queue.LogCounts()

	log.Println("FILLING")
	queue.Silent = true

	// have everything be ready, nothing be pending
	if err := queue.Fill(int(count), 2048, 1); err != nil {
		log.Fatal(err)
	}
	queue.Silent = false
	queue.LogCounts()
	queue.Silent = true

	log.Println("DRAINING")
	fn := func(ctx context.Context, item *yaqpg.Item) error {
		return nil
	}
	proc := yaqpg.FunctionProcessor{Function: fn}
	if err := queue.ProcessReady(yaqpg.MaxProcessLimit, proc); err != nil {
		log.Fatal(err)
	}
	queue.Silent = false
	queue.LogCounts()

}
