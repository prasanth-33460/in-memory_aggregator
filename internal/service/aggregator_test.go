package service

import (
	"testing"
	"time"

	"github.com/prasanth-33460/in-memory_aggregator/internal/models"
)

func TestAggregator_Add(t *testing.T) {
	agg := NewAggregator()
	date := "2024-01-01"

	// Test Case 1: Add AdRequest
	req1 := models.SignalRequest{
		App:     "com.test.app",
		Country: "IND",
		Signal:  models.AdRequest,
		UserID:  "user1",
	}
	agg.Add(req1, date)

	// Verify
	key := models.DimensionKey{Date: date, App: "com.test.app", Country: "IND"}
	agg.mu.RLock()
	metrics := agg.data[key]
	agg.mu.RUnlock()

	if metrics == nil {
		t.Fatal("Expected metrics to be created")
	}
	if metrics.AdRequest != 1 {
		t.Errorf("Expected AdRequest count 1, got %d", metrics.AdRequest)
	}
	if len(metrics.UniqueUsers) != 1 {
		t.Errorf("Expected 1 unique user, got %d", len(metrics.UniqueUsers))
	}

	// Test Case 2: Add another AdRequest for same key, different user
	req2 := models.SignalRequest{
		App:     "com.test.app",
		Country: "IND",
		Signal:  models.AdRequest,
		UserID:  "user2",
	}
	agg.Add(req2, date)

	agg.mu.RLock()
	metrics = agg.data[key]
	agg.mu.RUnlock()

	if metrics.AdRequest != 2 {
		t.Errorf("Expected AdRequest count 2, got %d", metrics.AdRequest)
	}
	if len(metrics.UniqueUsers) != 2 {
		t.Errorf("Expected 2 unique users, got %d", len(metrics.UniqueUsers))
	}

	// Test Case 3: Add AdClick for same key, existing user
	req3 := models.SignalRequest{
		App:     "com.test.app",
		Country: "IND",
		Signal:  models.AdClick,
		UserID:  "user1",
	}
	agg.Add(req3, date)

	agg.mu.RLock()
	metrics = agg.data[key]
	agg.mu.RUnlock()

	if metrics.AdClick != 1 {
		t.Errorf("Expected AdClick count 1, got %d", metrics.AdClick)
	}
	if len(metrics.UniqueUsers) != 2 {
		t.Errorf("Expected 2 unique users (no change), got %d", len(metrics.UniqueUsers))
	}
}

func TestAggregator_Flush(t *testing.T) {
	agg := NewAggregator()
	date := "2024-01-01"

	req := models.SignalRequest{
		App:     "com.test.app",
		Country: "IND",
		Signal:  models.AdRequest,
		UserID:  "user1",
	}
	agg.Add(req, date)

	// Flush
	records := agg.Flush()

	// Verify returned records
	if len(records) != 1 {
		t.Fatalf("Expected 1 record flushed, got %d", len(records))
	}
	r := records[0]
	if r.App != "com.test.app" || r.Country != "IND" || r.Date != date {
		t.Errorf("Record data mismatch: %+v", r)
	}
	if r.AdRequest != 1 {
		t.Errorf("Expected AdRequest 1, got %d", r.AdRequest)
	}
	if r.DAU != 1 {
		t.Errorf("Expected DAU 1, got %d", r.DAU)
	}

	// Verify aggregator is empty
	if agg.GetCurrentSize() != 0 {
		t.Errorf("Expected aggregator to be empty after flush, got size %d", agg.GetCurrentSize())
	}
}

func TestAggregator_FlushSignal(t *testing.T) {
	agg := NewAggregator()
	date := "2024-01-01"

	// Add 1000 unique requests to trigger flush signal
	for i := 0; i < 1000; i++ {
		agg.Add(models.SignalRequest{
			App:     "com.test.app" + string(rune(i)), // Unique app to create new record
			Country: "IND",
			Signal:  models.AdRequest,
		}, date)
	}

	select {
	case <-agg.FlushSignal():
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Expected flush signal to be sent")
	}
}
