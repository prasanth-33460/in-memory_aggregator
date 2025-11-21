package aggregator

import (
	"sync"
	"time"

	"github.com/prasanth-33460/in-memory_aggregator/internal/models"
)

type Aggregator struct {
	data map[models.DimensionKey]*models.AggregatedMetrics

	mu    sync.RWMutex
	stats Stats
}

type Stats struct {
	TotalRequests int64
	CurrentSize   int
	LastFlushTime time.Time
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		data: make(map[models.DimensionKey]*models.AggregatedMetrics),
		stats: Stats{
			LastFlushTime: time.Now(),
		},
	}
}

func (agg *Aggregator) Add(req models.SignalRequest, date string) {
	key := models.DimensionKey{
		Date:    date,
		App:     req.App,
		Country: req.Country,
	}

	agg.mu.Lock()
	defer agg.mu.Unlock()

	if agg.data[key] == nil {
		agg.data[key] = &models.AggregatedMetrics{
			UniqueUsers: make(map[string]struct{}),
		}
	}

	metrics := agg.data[key]

	switch req.Signal {
	case models.AdRequest:
		metrics.AdRequest++

	case models.AdResponse:
		metrics.AdResponse++

	case models.AdImpression:
		metrics.AdImpression++

	case models.AdClick:
		metrics.AdClick++
	}

	if req.UserID != "" {
		metrics.UniqueUsers[req.UserID] = struct{}{}
	}
	agg.stats.TotalRequests++
	agg.stats.CurrentSize = len(agg.data)
}

func (agg *Aggregator) Flush() []models.DBRecord {
	agg.mu.Lock()
	defer agg.mu.Unlock()

	if len(agg.data) == 0 {
		return nil
	}

	records := make([]models.DBRecord, 0, len(agg.data))
	for key, metrics := range agg.data {
		record := models.DBRecord{
			Date:         key.Date,
			App:          key.App,
			Country:      key.Country,
			AdRequest:    metrics.AdRequest,
			AdResponse:   metrics.AdResponse,
			AdImpression: metrics.AdImpression,
			AdClick:      metrics.AdClick,
			DAU:          int64(len(metrics.UniqueUsers)),
			CreatedAt:    time.Now(),
		}

		records = append(records, record)
	}
	agg.data = make(map[models.DimensionKey]*models.AggregatedMetrics)
	agg.stats.CurrentSize = 0
	agg.stats.LastFlushTime = time.Now()

	return records
}

func (agg *Aggregator) GetStats() Stats {
	agg.mu.RLock()
	defer agg.mu.RUnlock()
	return agg.stats
}

func (agg *Aggregator) GetCurrentSize() int {
	agg.mu.RLock()
	defer agg.mu.RUnlock()
	return len(agg.data)
}
