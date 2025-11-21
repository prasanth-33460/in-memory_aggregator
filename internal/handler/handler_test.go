package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	service "github.com/prasanth-33460/in-memory_aggregator/internal/aggregator"
	"github.com/prasanth-33460/in-memory_aggregator/internal/models"
)

func TestHandler_IngestSignal(t *testing.T) {
	agg := service.NewAggregator()
	// We can pass nil for flusher as IngestSignal doesn't use it
	h := NewHandler(agg, nil)

	tests := []struct {
		name           string
		method         string
		body           models.SignalRequest
		expectedStatus int
	}{
		{
			name:   "Valid Request",
			method: http.MethodPost,
			body: models.SignalRequest{
				App:     "com.test.app",
				Country: "IND",
				Signal:  models.AdRequest,
				UserID:  "user1",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Invalid Method",
			method:         http.MethodGet,
			body:           models.SignalRequest{},
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:   "Missing App",
			method: http.MethodPost,
			body: models.SignalRequest{
				Country: "IND",
				Signal:  models.AdRequest,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:   "Missing Country",
			method: http.MethodPost,
			body: models.SignalRequest{
				App:    "com.test.app",
				Signal: models.AdRequest,
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:   "Missing Signal",
			method: http.MethodPost,
			body: models.SignalRequest{
				App:     "com.test.app",
				Country: "IND",
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:   "Invalid Signal Type",
			method: http.MethodPost,
			body: models.SignalRequest{
				App:     "com.test.app",
				Country: "IND",
				Signal:  "InvalidSignal",
			},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest(tt.method, "/v1/signal/ingest", bytes.NewBuffer(bodyBytes))
			w := httptest.NewRecorder()

			h.IngestSignal(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, w.Code)
			}
		})
	}
}
