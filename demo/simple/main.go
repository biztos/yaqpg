// simple demo

package main

import (
	"context"
	"log"
	"time"

	"github.com/biztos/yaqpg"
)

func main() {

	queue := yaqpg.MustStartNamedQueue("example")
	if err := queue.Add("ex1", []byte("any payload")); err != nil {
		log.Fatal(err)
	}
	if err := queue.AddDelayed("ex2", []byte("later payload"), time.Second); err != nil {
		log.Fatal(err)
	}
	proc := yaqpg.FunctionProcessor{
		Function: func(ctx context.Context, item *yaqpg.Item) error {
			log.Println("processing", item.Ident, string(item.Payload))
			return nil
		}}
	queue.LogCounts()       // shows 1 ready, 1 pending
	time.Sleep(time.Second) // wait out the pending item
	if err := queue.Process(2, proc); err != nil {
		log.Fatal(err)
	}
	queue.LogCounts() // shows queue is empty

}
