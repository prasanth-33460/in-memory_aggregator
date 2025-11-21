package service

import (
	"errors"
	"testing"
	"time"

	"github.com/prasanth-33460/in-memory_aggregator/internal/models"
)

func TestDLQ_Add(t *testing.T) {
	// Pass nil for database as we are only testing in-memory Add
	dlq := NewDeadLetterQueue(nil, 3, 1*time.Second)

	records := []models.DBRecord{
		{App: "app1", Country: "IND"},
	}
	err := errors.New("db error")

	dlq.Add(records, err)

	stats := dlq.Stats()
	if stats["queue_size"] != 1 {
		t.Errorf("Expected queue size 1, got %d", stats["queue_size"])
	}
	if stats["total_records"] != 1 {
		t.Errorf("Expected total records 1, got %d", stats["total_records"])
	}
}
