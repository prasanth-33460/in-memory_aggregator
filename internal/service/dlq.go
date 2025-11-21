package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/prasanth-33460/in-memory_aggregator/internal/database"
	"github.com/prasanth-33460/in-memory_aggregator/internal/models"
)

type FailedBatch struct {
	Records     []models.DBRecord
	FailedAt    time.Time
	Attempts    int
	LastError   string
	NextRetryAt time.Time
}

type DeadLetterQueue struct {
	queue         []*FailedBatch
	mu            sync.Mutex
	db            *database.Database
	maxRetries    int
	retryInterval time.Duration
	ticker        *time.Ticker
	done          chan struct{}
}

func NewDeadLetterQueue(db *database.Database, maxRetries int, retryInterval time.Duration) *DeadLetterQueue {
	return &DeadLetterQueue{
		queue:         make([]*FailedBatch, 0),
		db:            db,
		maxRetries:    maxRetries,
		retryInterval: retryInterval,
		ticker:        time.NewTicker(retryInterval),
		done:          make(chan struct{}),
	}
}

func (dlq *DeadLetterQueue) Add(records []models.DBRecord, err error) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	failedBatch := &FailedBatch{
		Records:     records,
		FailedAt:    time.Now(),
		Attempts:    0,
		LastError:   err.Error(),
		NextRetryAt: time.Now().Add(dlq.retryInterval),
	}

	dlq.queue = append(dlq.queue, failedBatch)

	log.Printf("Added %d records to DLQ (total queue size: %d)", len(records), len(dlq.queue))
}

func (dlq *DeadLetterQueue) Start(ctx context.Context) {
	log.Println("Dead Letter Queue retry worker started")

	go func() {
		for {
			select {
			case <-dlq.ticker.C:
				dlq.processRetries(ctx)

			case <-dlq.done:
				log.Println("DLQ retry worker stopping...")
				return
			}
		}
	}()
}

func (dlq *DeadLetterQueue) processRetries(ctx context.Context) {
	dlq.mu.Lock()
	queueSnapshot := make([]*FailedBatch, len(dlq.queue))
	copy(queueSnapshot, dlq.queue)
	dlq.queue = make([]*FailedBatch, 0)
	dlq.mu.Unlock()

	now := time.Now()
	remainingBatches := make([]*FailedBatch, 0, len(queueSnapshot))

	for _, batch := range queueSnapshot {
		if now.Before(batch.NextRetryAt) {
			remainingBatches = append(remainingBatches, batch)
			continue
		}

		batch.Attempts++

		log.Printf("Retrying batch (attempt %d/%d, %d records)",
			batch.Attempts, dlq.maxRetries, len(batch.Records))

		err := dlq.db.BatchInsertRecords(ctx, batch.Records)

		if err == nil {
			log.Printf("Retry successful! Recovered %d records", len(batch.Records))
			continue
		}

		batch.LastError = err.Error()
		log.Printf("Retry failed: %v", err)

		if batch.Attempts >= dlq.maxRetries {
			log.Printf("Max retries exceeded, writing to disk for manual recovery")
			dlq.writeToDisk(batch)
			continue
		}

		backoff := dlq.retryInterval * time.Duration(1<<batch.Attempts)
		batch.NextRetryAt = now.Add(backoff)

		log.Printf("Will retry again in %v", backoff)

		remainingBatches = append(remainingBatches, batch)
	}

	dlq.mu.Lock()
	dlq.queue = append(dlq.queue, remainingBatches...)
	dlq.mu.Unlock()
}

func (dlq *DeadLetterQueue) writeToDisk(batch *FailedBatch) {
	if err := os.MkdirAll("dlq_failed", 0755); err != nil {
		log.Printf("ERROR: Failed to create dlq_failed directory: %v", err)
		return
	}

	filename := fmt.Sprintf("dlq_failed/batch_%s_%d.json",
		time.Now().Format("20060102_150405"), time.Now().Nanosecond())

	data, err := json.MarshalIndent(batch, "", "  ")
	if err != nil {
		log.Printf("ERROR: Failed to marshal batch: %v", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Printf("ERROR: Failed to write to disk: %v", err)
		return
	}

	log.Printf("Batch written to disk: %s (%d records)", filename, len(batch.Records))
}

func (dlq *DeadLetterQueue) Stop(ctx context.Context) {
	log.Println("DLQ shutting down...")

	dlq.ticker.Stop()
	close(dlq.done)

	time.Sleep(100 * time.Millisecond)
	
	dlq.mu.Lock()
	queueLen := len(dlq.queue)
	dlq.mu.Unlock()

	if queueLen > 0 {
		log.Printf("Warning: %d batches remain in DLQ, attempting final flush", queueLen)
		dlq.processRetries(ctx)
	}

	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	for _, batch := range dlq.queue {
		log.Printf("Writing remaining batch to disk during shutdown")
		dlq.writeToDisk(batch)
	}

	log.Println("DLQ stopped")
}

func (dlq *DeadLetterQueue) Stats() map[string]any {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	totalRecords := 0
	for _, batch := range dlq.queue {
		totalRecords += len(batch.Records)
	}

	return map[string]any{
		"queue_size":    len(dlq.queue),
		"total_records": totalRecords,
	}
}
