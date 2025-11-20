package models

import "time"

type SignalType string

const (
	AdRequest    SignalType = "adRequest"
	AdResponse   SignalType = "adResponse"
	AdImpression SignalType = "adImpression"
	AdClick      SignalType = "adClick"
)

type SignalRequest struct {
	App     string     `json:"app" validate:"required"`
	Country string     `json:"country" validate:"required"`
	Signal  SignalType `json:"signal" validate:"required,oneof=adRequest adResponse adImpression adClick"`
	UserID  string     `json:"user_id,omitempty"`
}

type DimensionKey struct {
	Date    string
	App     string
	Country string
}

type AggregatedMetrics struct {
	AdRequest    int64
	AdResponse   int64
	AdImpression int64
	AdClick      int64
	UniqueUsers  map[string]struct{}
}

type DBRecord struct {
	Date         string
	App          string
	Country      string
	AdRequest    int64
	AdResponse   int64
	AdImpression int64
	AdClick      int64
	DAU          int64 //changing from set to int64 for DB.
	CreatedAt    time.Time
}
