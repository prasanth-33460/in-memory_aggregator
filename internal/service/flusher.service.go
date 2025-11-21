package aggregator

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/prasanth-33460/in-memory_aggregator/internal/database"
)

type Flusher struct {
	aggregator     *Aggregator
	db             *database.Database
	timeTicker     *time.Ticker //5 seconds
	done           chan struct{}
	wg             sync.WaitGroup
	flushInterval  time.Duration
	flushThreshold int
}

func NewFlusher(agg *Aggregator, db *database.Database, interval time.Duration, threshold int) *Flusher {
	return &Flusher{
		aggregator:     agg,
		db:             db,
		timeTicker:     time.NewTicker(interval),
		done:           make(chan struct{}),
		flushInterval:  interval,
		flushThreshold: threshold,
	}
}

func (f *Flusher) Start(ctx context.Context) {
	log.Println("Flusher started")
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			select {
			case <-f.timeTicker.C:
				f.flush(ctx, "time-based")

			case <-f.aggregator.FlushSignal():
				f.flush(ctx, "count-based")

			case <-f.done:
				log.Println("timer based flusher stop.")
				return
			}
		}
	}()
}

func (f *Flusher) flush(ctx context.Context, reason string) {
	log.Printf("Flushing data to DB due to %s...\n", reason)
	records := f.aggregator.Flush()

	if len(records) == 0 {
		return
	}
	start := time.Now()

	err := f.db.BatchInsertRecords(ctx, records)

	if err != nil {
		log.Printf("failed to flush %d records to database: %v", len(records), err)
		log.Printf("%d records were lost!", len(records))
		return
	}

	log.Printf("successfully flushed %d records in %v", len(records), time.Since(start))
}

func (f *Flusher) Stop(ctx context.Context) {
	log.Println("Flusher shutting down...")

	f.timeTicker.Stop()
	close(f.done)
	f.wg.Wait()
	f.flush(ctx, "shutdown")

	log.Println("Flusher stopped")
}
