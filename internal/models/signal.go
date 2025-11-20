package models

type SignalType string

const (
	AdRequest    SignalType = "AdRequest"
	AdResponse   SignalType = "AdResponse"
	AdImpression SignalType = "AdImpression"
	AdClick      SignalType = "AdClick"
)

type SignalRequest struct {
	App     string     `json:"app" validate:"required"`
	Country string     `json:"country" validate:"required"`
	Signal  SignalType `json:"signal" validate:"required"`
	UserID  string     `json:"user_id,omitempty"`
}

type DimensionKey struct {
	Date    string
	App     string
	Country string
}

type AggregatedMetrics struct {
	AdRequest    int64
	Adresponse   int64
	AdImpression int64
	AdClick      int64
	UniqueUsers  map[string]struct{}
}

type DBRecord struct {
	Date         string
	App          string
	Coutnry      string
	AdRequest    int64
	AdResponse   int64
	AdImpression int64
	AdClick      int64
	DAU          int64
	CreatedAt    int64
}
