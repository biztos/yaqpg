// yaqpg.go

// Package yaqpg proivides a simple locking queue for PostgreSQL databases.
// It is intended for light, local workloads.
package yaqpg

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
)

const MaxProcessLimit = 1000

var DefaultTableName = "yaqpg_queue"
var DefaultQueueName = "jobs"
var DefaultMaxConnections = 8

var DefaultProcessContextTimeout = 1 * time.Minute
var DefaultReprocessDelay = 2 * time.Second
var DefaultMaxReprocessDelay = 1 * time.Minute
var DefaultMaxAttempts = 5

var StartWithPlaceholder = true

var FillBatchSize = 100

// Logger is the simplest logger used by Queue.
type Logger interface {
	Println(...interface{})
}

var DefaultLogger Logger = log.Default()

// Processor is the thing that processes an item, returning an error if not
// completed successfully.
type Processor interface {
	// Process processes an item.  Returning error will invoke the re-queueing
	// logic, subject to MaxAttempts in the Queue.
	Process(context.Context, *Item) error
}

// FunctionProcessor is a Processor that uses a simple function to process
// each item.
type FunctionProcessor struct {
	Function func(context.Context, *Item) error
}

// Process implements Processor for FunctionProcessor using a thin wrapper.
func (p FunctionProcessor) Process(ctx context.Context, item *Item) error {
	return p.Function(ctx, item)
}

// DefaultBackoffDelay returns DefaultReprocessDelay that doubles for every
// attempt over one, up to DefaultMaxReprocessDelay.
func DefaultBackoffDelay(attempts int) time.Duration {

	if DefaultReprocessDelay == 0 || attempts < 2 {
		return DefaultReprocessDelay
	}

	max := int(DefaultMaxReprocessDelay)
	delay := int(DefaultReprocessDelay)
	for i := 0; i < attempts; i++ {
		delay = delay * 2
		if delay > max {
			return DefaultMaxReprocessDelay
		}
	}
	return time.Duration(delay)
}

// DefaultStableDelay returns DefaultReprocessDelay regardless of attempts.
func DefaultStableDelay(attempts int) time.Duration {
	return DefaultReprocessDelay
}

// Queue represents a named queue in a specific database table.
type Queue struct {
	Name                  string
	TableName             string
	MaxAttempts           int
	ProcessContextTimeout time.Duration
	ReprocessDelayFunc    func(attempts int) time.Duration
	Pool                  *pgxpool.Pool
	Logger                Logger
	Silent                bool
}

func (q *Queue) tableSql(sqlf string) string {
	return fmt.Sprintf(sqlf, q.TableName)
}

// Log writes v to the logger, with the queue [Name] as a prefix.
func (q *Queue) Log(v ...interface{}) {
	if q.Silent == false {
		msg := fmt.Sprintf("[%s] %s", q.Name, fmt.Sprint(v...))
		q.Logger.Println(msg)
	}

}

// Logf calls Log in Printf style.
func (q *Queue) Logf(f string, v ...interface{}) {

	q.Log(fmt.Sprintf(f, v...))
}

// Connect creates connection pool for the database defined in the
// DATABASE_URL environment variable and stores the connection in q.Pool.
func (q *Queue) Connect(max_connections int) error {

	dburl := os.Getenv("DATABASE_URL")
	if dburl == "" {
		return errors.New("DATABASE_URL not defined in environment")
	}

	connstr := fmt.Sprintf("%s?pool_max_conns=%d", dburl, max_connections)
	pool, err := pgxpool.New(context.Background(), connstr)
	if err != nil {
		log.Fatal(err)
	}
	q.Pool = pool
	return nil

}

// CreateSchema connects and creates the queue table and indexes if needed.
// Existing relations are NOT dropped!  The queue must already have connected
// to the database.
func (q *Queue) CreateSchema() error {
	_, err := q.Pool.Exec(context.Background(), Schema(q))
	return err
}

// Add adds an item to the queue.  The item will be immediately available.
func (q *Queue) Add(ident string, payload []byte) error {

	q.Logf("adding %s", ident)
	err := q.addWithReadyAt(ident, payload, time.Now())
	if err != nil {
		q.Logf("adding %s: %s", ident, err)
		return err
	}

	q.Logf("adding %s: success", ident)
	return nil

}

// AddDelayed adds an item to the queue with the specified delay.
func (q *Queue) AddDelayed(ident string, payload []byte, delay time.Duration) error {

	ready_at := time.Now().Add(delay)
	ready_at_str := ready_at.Format(time.DateTime)
	q.Logf("adding %s for %s", ident, ready_at_str)
	err := q.addWithReadyAt(ident, payload, ready_at)
	if err != nil {
		q.Logf("adding %s for %s: %s", ident, ready_at_str, err)
		return err
	}

	q.Logf("adding %s for %s: success", ident, ready_at_str)
	return nil

}

// AddBatch adds a set of items in a transaction for immediate availability
// when finished.  Every *Item in items will be given the BatchId unique to
// this call.
func (q *Queue) AddBatch(items []*Item) error {

	batch_id := ulid.Make()
	ready_at := time.Now()
	q.Logf("adding batch %s: %d items",
		batch_id, len(items))

	err := q.addBatchWithReadyAt(batch_id, items, ready_at)
	if err != nil {
		q.Logf("adding batch %s: %d items: %s",
			batch_id, len(items), err)
		return err
	}

	q.Logf("adding batch %s: %d items: success",
		batch_id, len(items))
	return nil

}

// AddBatchDelayed adds a set of items in a transaction for the given delay.
// The delay will be the same for all items. Every *Item in items will be
// given the BatchId unique to this call.
func (q *Queue) AddBatchDelayed(items []*Item, delay time.Duration) error {
	batch_id := ulid.Make()
	ready_at := time.Now().Add(delay)
	ready_at_str := ready_at.Format(time.DateTime)
	q.Logf("adding batch %s: %d items at %s",
		batch_id, len(items), ready_at_str)
	err := q.addBatchWithReadyAt(batch_id, items, ready_at)
	if err != nil {
		q.Logf("adding batch %s: %d items at %s: %s",
			batch_id, len(items), ready_at_str, err)
		return err
	}

	q.Logf("adding batch %s: %d items at %s: success",
		batch_id, len(items), ready_at_str)
	return nil
}

func (q *Queue) addWithReadyAt(ident string, payload []byte, ready_at time.Time) error {

	ctx := context.Background()
	sql := insertQueueItemSQL(q)
	if _, err := q.Pool.Exec(ctx, sql, q.Name, ident, payload, ready_at); err != nil {
		return err
	}
	return nil
}

func (q *Queue) addBatchWithReadyAt(batch_id ulid.ULID, items []*Item, ready_at time.Time) error {

	// The point of adding the BatchId, by the way, is so that in tricky cases
	// you can log further actions on the failed items and find your way back
	// to the batch execution in the logs. Overkill? Maybe! And this is done
	// here because we may not make it through the whole batch in the tx.
	for _, item := range items {
		item.BatchId = batch_id
	}

	ctx := context.Background() // should not hang, so let it hang...
	tx, err := q.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // safe after commit!

	sql := insertQueueItemSQL(q)

	for _, item := range items {

		_, err := tx.Exec(ctx, sql, q.Name, item.Ident, item.Payload, ready_at)
		if err != nil {
			// For the same reason we include the actual error encountered.
			// This will be the first error, since the transaction is toast
			// at that point anyway.
			item.err = err
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

// Count returns the total number of items in the queue, regardless of ready_at
// values.
func (q *Queue) Count() (int, error) {

	ctx := context.Background()
	sql := selectCountSQL(q)
	var count int
	if err := q.Pool.QueryRow(ctx, sql, q.Name).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// MustCount calls Count and panics on error.
func (q *Queue) MustCount() int {
	count, err := q.Count()
	if err != nil {
		panic(err)
	}
	return count
}

// CountReady returns the total number of ready items in the queue, including
// items currently being processed. For obvious reasons, this number may be
// inaccurate by the time you consume it.
//
// (Excluding items in flight at the db level is too inefficient and anyway
// none of this remains accurate. If we find a need to know the total number
// of items being processed, this can be handled on the code side easily
// enough, but is probably not worth the trouble.)
func (q *Queue) CountReady() (int, error) {

	ctx := context.Background()
	sql := selectCountReadySQL(q)
	var count int
	if err := q.Pool.QueryRow(ctx, sql, q.Name).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// MustCountReady calls CountReady and panics on error.
func (q *Queue) MustCountReady() int {
	count, err := q.CountReady()
	if err != nil {
		panic(err)
	}
	return count
}

// CountPending returns the total number of items not yet ready in the queue,
// i.e. those with a ready_at later than the current time.  For obvious
// reasons, this number may be inaccurate by the time you consume it.
func (q *Queue) CountPending() (int, error) {

	ctx := context.Background()
	sql := selectCountPendingSQL(q)
	var count int
	if err := q.Pool.QueryRow(ctx, sql, q.Name).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// MustCountPending calls CountPending and panics on error.
func (q *Queue) MustCountPending() int {
	count, err := q.CountPending()
	if err != nil {
		panic(err)
	}
	return count
}

// LogCounts retrieves and logs the counts, and panics if any count returns an
// error.
func (q *Queue) LogCounts() {
	q.Logf("item count: %d (ready: %d, pending: %d)",
		q.MustCount(), q.MustCountReady(), q.MustCountPending())
}

// Fill fills the queue with count items of test data, with a randomized delay
// up to delay_max. The payload will be payload_size bytes of random data.
// Every item will have the same payload.
//
// Fill concurrently adds batches of up to FillBatchSize items, all of which
// will have the same delay time.  More randomness can be obtained by setting
// this to a lower value.
//
// This is (arguably) useful for testing!
//
// NOTE: variable payload size is not supported at this time.
func (q *Queue) Fill(count int, payload_size int, delay_max time.Duration) error {

	payload := make([]byte, payload_size)
	_, err := rand.Read(payload)
	if err != nil {
		return err

	}

	if count <= FillBatchSize {
		delay := time.Duration(rand.Intn(int(delay_max)))
		err := q.fillBatch(count, 0, payload, delay)
		if err != nil {
			return err
		}
		return nil
	}

	for i := 0; i < count; i += FillBatchSize {
		delay := time.Duration(rand.Intn(int(delay_max)))
		err := q.fillBatch(FillBatchSize, i, payload, delay)
		if err != nil {
			return err
		}
	}
	remain := count % FillBatchSize
	if remain > 0 {
		delay := time.Duration(rand.Intn(int(delay_max)))
		err := q.fillBatch(remain, count-remain, payload, delay)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Queue) fillBatch(count int, offset int, payload []byte, delay time.Duration) error {

	items := make([]*Item, count)
	for i := 0; i < count; i++ {
		ident := fmt.Sprintf("fill_%016d_%024d", i+offset, rand.Uint64())
		items[i] = NewItem(ident, payload)
	}
	return q.AddBatchDelayed(items, delay)

}

// Process gets up to limit items from the queue and processes them in
// goroutines.  The items are updated after processing completes, and the
// transaction (batch) is committed if there were no database errors, or
// rolled back if there were.  The Processor's Process function is passed a
// ready_at context which it should respect.  Returning an error from that
// function will result in the item being released back into the queue with
// its Attempts incremented and its ReadyAt pushed out by ReadyAtDelay; or
// deleted if there MaxAttempts are exceeded.
func (q *Queue) Process(limit int, proc Processor) error {

	if limit > MaxProcessLimit {
		return fmt.Errorf("limit %d > MaxProcessLimit %d",
			limit, MaxProcessLimit)
	}
	// Assign a unique ID to this batch so we can catch any errors in the
	// future.
	ulid := ulid.Make()

	// First we get the items.
	q.Logf("processing <= %d in batch: %s", limit, ulid)

	checkout_sql := selectCheckoutSQL(q)

	bg := context.Background()
	tx, err := q.Pool.Begin(bg)
	if err != nil {
		return err
	}
	defer tx.Rollback(bg) // safe after commit!

	rows, err := tx.Query(bg, checkout_sql, q.Name, limit)
	if err != nil {
		return err
	}

	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Item, error) {
		var i Item
		// err := i.Scan(row) // NOTE: inverse of documentation! Bug?
		return &i, i.Scan(row)
	})

	if len(items) == 0 {
		q.Logf("batch: %s nothing to process", ulid)
		return nil
	}
	q.Logf("processing %d in batch: %s", len(items), ulid)

	var wg sync.WaitGroup
	for _, item := range items {
		item.BatchId = ulid // same for all.
		wg.Add(1)
		go func(it *Item) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(bg, q.ProcessContextTimeout)
			defer cancel()
			err := proc.Process(ctx, it)
			if err != nil {
				it.err = err
			}
		}(item)
	}
	q.Logf("batch %s: waiting", ulid)
	wg.Wait()
	q.Logf("batch %s: done waiting", ulid)

	// Now all items have been processed and we can delete or update them.
	// Batch group these.
	delete_ids := make([]int64, 0, len(items))
	retry_items := make([]*Item, 0, len(items))
	for _, item := range items {
		if item.err == nil {
			// redoing logs anyway, be lazy here
			q.Logf("batch %s id %d ident %s: completed, will delete", ulid, item.id, item.Ident)
			delete_ids = append(delete_ids, item.id)
		} else {
			q.Logf("batch %s id %d ident %s: error: %s", ulid, item.id, item.Ident, item.err)
			if item.Attempts+1 >= q.MaxAttempts {
				q.Logf("batch %s id %d ident %s: max attempts %d reached, will delete", ulid, item.id, item.Ident, q.MaxAttempts)
				delete_ids = append(delete_ids, item.id)

			} else {
				q.Logf("batch %s id %d ident %s: will retry", ulid, item.id, item.Ident)
				retry_items = append(retry_items, item)

			}
		}
	}
	if len(delete_ids) > 0 {

		q.Logf("batch %s deleting %d items by id", ulid, len(delete_ids))
		del_sql := selectDeleteByIdListSQL(q)
		del_tag, err := tx.Exec(bg, del_sql, delete_ids)
		if err != nil {
			tx.Rollback(bg)
			q.Logf("batch %s delete: tag %s, error: %s", ulid, del_tag, err)
			return err
		}
	}

	// TODO (maybe) -- check rows affected, error out if wrong. Impossible?
	if len(retry_items) > 0 {

		// We have no idea what the delay is -- its input is Attempts but it
		// could for instance be tracking them over time, etc.  So we ask for
		// every one, and because we know that _usually_ there will not be
		// much variance we group them by the delay values.
		delay_groups := map[time.Duration][]int64{}
		for _, item := range retry_items {
			delay := q.ReprocessDelayFunc(item.Attempts + 1) // counting this one
			delay_groups[delay] = append(delay_groups[delay], item.id)
		}
		now := time.Now() // same for all groups.
		for delay, group := range delay_groups {

			retry_at := now.Add(delay)
			q.Logf("batch %s updating %d items by id delay %s",
				ulid, len(group), delay)
			upd_sql := updateReadyAtByIdListSQL(q)
			upd_tag, err := tx.Exec(bg,
				upd_sql, retry_at, group)
			if err != nil {
				tx.Rollback(bg)
				q.Logf("batch %s update: tag %s, error: %s", ulid, upd_tag, err)
				return err
			}
		}

	}

	err = tx.Commit(bg)
	if err != nil {
		return err // testing nightmare... kill connection or what?
	}

	q.Logf("processing batch %s: complete", ulid)

	return nil
}

// ProcessReady calls Process with limit concurrently as many times as
// needed for the currently ready set of items.  The number of ready items
// may not be zero after completion: pending items may become available, and
// new items may be added by other processes.
//
// WARNING: this can be dangerous if you have a large queue!
func (q *Queue) ProcessReady(limit int, proc Processor) error {
	count, err := q.CountReady()
	if err != nil {
		return err
	}
	batches := (count / limit)
	if count%limit > 0 {
		batches++
	}
	q.Logf("processing %d in %d batches", count, batches)

	var had_errs = false
	var wg sync.WaitGroup
	for i := 0; i < batches; i++ {
		wg.Add(1)
		go func() {
			batch_no := i + 1
			defer wg.Done()
			err := q.Process(limit, proc)
			if err != nil {
				had_errs = true
				q.Logf("batch %d returned error: %s", batch_no, err)
			}
		}()
	}
	q.Log("waiting...")
	wg.Wait()
	q.Log("...done waiting")

	if had_errs {
		return errors.New("processors returned errors")
	}
	return nil
}

// NewDefaultQueue returns a new queue with all defaults.
func NewDefaultQueue() *Queue {
	return &Queue{
		Name:                  DefaultQueueName,
		TableName:             DefaultTableName,
		ProcessContextTimeout: DefaultProcessContextTimeout,
		ReprocessDelayFunc:    DefaultStableDelay,
		MaxAttempts:           DefaultMaxAttempts,
		Logger:                DefaultLogger,
	}
}

// StartDefaultQueue creates a NewDefaultQueue and calls Connect with
// DefaultMaxConnections.
func StartDefaultQueue() (*Queue, error) {
	q := NewDefaultQueue()
	err := q.Connect(DefaultMaxConnections)
	if err != nil {
		return nil, err

	}
	return q, err

}

// MustStartDefaultQueue calls StartDefaultQueue and panics on error.
func MustStartDefaultQueue() *Queue {
	q, err := StartDefaultQueue()
	if err != nil {
		panic(err)
	}

	return q

}

// NewNamedQueue returns a new queue with all defaults except Name.
func NewNamedQueue(name string) *Queue {
	q := NewDefaultQueue()
	q.Name = name
	return q
}

// StartNamedQueue creates a NewNamedQueue and calls Connect with
// DefaultMaxConnections.
func StartNamedQueue(name string) (*Queue, error) {
	q := NewNamedQueue(name)
	err := q.Connect(DefaultMaxConnections)
	if err != nil {
		return nil, err

	}
	return q, err

}

// MustStartNamedQueue calls StartNamedQueue and panics on error.
func MustStartNamedQueue(name string) *Queue {
	q, err := StartNamedQueue(name)
	if err != nil {
		panic(err)
	}

	return q

}

// Item represents a queue item as used in Process and batch adding.
type Item struct {
	id       int64     // id and err are opaque to the Processor.
	err      error     //
	BatchId  ulid.ULID // *should* be included in logs by the Processor.
	Ident    string
	Attempts int
	Payload  []byte
}

// NewItem returns an item for AddBatch processing.
func NewItem(ident string, payload []byte) *Item {
	return &Item{Ident: ident, Payload: payload}
}

// Scan puts a database row into the item values.
func (it *Item) Scan(row pgx.Row) error {
	return row.Scan(
		&it.id,
		&it.Ident,
		&it.Attempts,
		&it.Payload,
	)
}
