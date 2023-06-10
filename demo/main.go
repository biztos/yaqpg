// yaqpg queue demo

package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/biztos/yaqpg"
)

func main() {
	log.Println("DEMO")
	queue := yaqpg.MustStartNamedQueue("demo")
	if err := queue.Add("demo item 1", []byte("any payload")); err != nil {
		log.Fatal(err)
	}
	if err := queue.Add("demo item 2", []byte("any other payload")); err != nil {
		log.Fatal(err)
	}
	if err := queue.AddDelayed("demo item 3", []byte("later payload"), time.Second*3); err != nil {
		log.Fatal(err)
	}
	if err := queue.LogCounts(); err != nil {
		log.Fatal(err)
	}
	if err := queue.Fill(10, 20, time.Second); err != nil {
		log.Fatal(err)
	}
	if err := queue.LogCounts(); err != nil {
		log.Fatal(err)
	}
	log.Println("Processing, 10 at a time, with 50-50 chance of rejection.")
	log.Println("...but not much delay and low tolerance for attempts...")
	queue.MaxAttempts = 2
	proc := yaqpg.FunctionProcessor{
		Function: func(ctx context.Context, item *yaqpg.Item) error {
			good := rand.Intn(2)
			if good == 0 {
				return errors.New("rejected")
			}
			return nil
		}}
	for count := 1; count > 0; count = queue.MustCount() {
		log.Println("--- NEXT BATCH ---")
		err := queue.Process(10, proc)
		if err != nil {
			log.Fatal(err)
		}
		if err := queue.LogCounts(); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second / 2)
	}

}
