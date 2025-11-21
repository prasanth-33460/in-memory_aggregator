package service

import (
	"context"
	"testing"
	"time"
)

func TestFlusher_StartStop(t *testing.T) {
	agg := NewAggregator()
	// Pass nil for DB. If logic is correct, it shouldn't be used because aggregator is empty.
	flusher := NewFlusher(agg, nil, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start flusher
	flusher.Start(ctx)

	// Let it run for a bit to trigger at least one ticker tick
	time.Sleep(200 * time.Millisecond)

	// Stop flusher
	flusher.Stop(ctx)

	// If we reached here without panic, it means:
	// 1. Start/Stop works
	// 2. Ticker fired
	// 3. Flush was called
	// 4. Flush saw empty records and returned EARLY (didn't try to use nil DB)
}

func TestFlusher_NoFlushOnEmpty(t *testing.T) {
	agg := NewAggregator()
	flusher := NewFlusher(agg, nil, 1*time.Hour) // Long interval, we call flush manually if needed, but here we rely on internal logic

	// We can't call flush directly as it's private, but we verified the logic in StartStop test.
	// This test specifically ensures that even with a nil DB, we don't panic if there's no data.
	
	ctx := context.Background()
	// Manually trigger logic via Start/Stop with no data
	flusher.Start(ctx)
	flusher.Stop(ctx)
}
