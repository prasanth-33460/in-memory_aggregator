package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	service "github.com/prasanth-33460/in-memory_aggregator/internal/aggregator"
	"github.com/prasanth-33460/in-memory_aggregator/internal/models"
)

type Handler struct {
	aggregator *service.Aggregator
	flusher    *service.Flusher
}

func NewHandler(agg *service.Aggregator, flusher *service.Flusher) *Handler {
	return &Handler{
		aggregator: agg,
		flusher:    flusher,
	}
}

func (h *Handler) IngestSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed. ", http.StatusMethodNotAllowed)
		return
	}

	var req models.SignalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("failed to decode JSON: %v", err)
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	if req.App == "" {
		http.Error(w, "missing required field: app", http.StatusBadRequest)
		return
	}
	if req.Country == "" {
		http.Error(w, "missing required field: country", http.StatusBadRequest)
		return
	}
	if req.Signal == "" {
		http.Error(w, "missing required field: signal", http.StatusBadRequest)
		return
	}

	validSignals := map[models.SignalType]bool{
		models.AdRequest:    true,
		models.AdResponse:   true,
		models.AdImpression: true,
		models.AdClick:      true,
	}

	if !validSignals[req.Signal] {
		http.Error(w, "invalid signal type. must be one type of the 4 signals.", http.StatusBadRequest)
		return
	}

	date := time.Now().Format("2006-01-02") //year-mm-dd

	h.aggregator.Add(req, date)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	stats := h.aggregator.GetStats()
	flusherStats := h.flusher.Stats()

	var dlqQueueSize, dlqTotalRecords any
	if dlqStats, ok := flusherStats["dlq"].(map[string]any); ok {
		dlqQueueSize = dlqStats["queue_size"]
		dlqTotalRecords = dlqStats["total_records"]
	} else {
		dlqQueueSize = 0
		dlqTotalRecords = 0
	}

	response := map[string]any{
		"status":            "healthy",
		"total_requests":    stats.TotalRequests,
		"current_size":      stats.CurrentSize,
		"last_flush_time":   stats.LastFlushTime.Format(time.RFC3339),
		"uptime_seconds":    time.Since(stats.LastFlushTime).Seconds(),
		"dlq_queue_size":    dlqQueueSize,
		"dlq_total_records": dlqTotalRecords,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
