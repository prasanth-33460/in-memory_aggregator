package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/prasanth-33460/in-memory_aggregator/internal/models"
)

type Database struct {
	db *sql.DB
}

func NewDatabase(connStr string) (*Database, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database, check connection: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Database{db: db}, nil
}

func (d *Database) InitSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS ad_signals (
	id SERIAL PRIMARY KEY,
	date DATE NOT NULL,
	app VARCHAR(255) NOT NULL,
	country VARCHAR(3) NOT NULL, 
	ad_requests BIGINT DEFAULT 0, 
	ad_responses BIGINT DEFAULT 0,
	ad_impressions BIGINT DEFAULT 0,
	ad_clicks BIGINT DEFAULT 0,
	dau BIGINT DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
	UNIQUE(date, app, country)
	);

	CREATE INDEX IF NOT EXISTS idx_date_app_country 
    ON ad_signals(date, app, country);
	`

	_, err := d.db.ExecContext(ctx, schema)

	return err
}

func (d *Database) BatchInsertRecords(ctx context.Context, records []models.DBRecord) error {
	if len(records) == 0 {
		return nil
	}

	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]any, 0, len(records)*9)

	for i, record := range records {
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9,
		))

		valueArgs = append(valueArgs,
			record.Date,
			record.App,
			record.Country,
			record.AdRequest,
			record.AdResponse,
			record.AdImpression,
			record.AdClick,
			record.DAU,
			record.CreatedAt,
		)
	}
	query := fmt.Sprintf(`
        INSERT INTO ad_signals 
        (date, app, country, ad_request, ad_response, ad_impression, ad_click, dau, created_at)
        VALUES %s
        ON CONFLICT (date, app, country)
        DO UPDATE SET
            ad_request = ad_signals.ad_request + EXCLUDED.ad_request,
            ad_response = ad_signals.ad_response + EXCLUDED.ad_response,
            ad_impression = ad_signals.ad_impression + EXCLUDED.ad_impression,
            ad_click = ad_signals.ad_click + EXCLUDED.ad_click,
            dau = ad_signals.dau + EXCLUDED.dau,
            created_at = EXCLUDED.created_at
    `, strings.Join(valueStrings, ","))

	_, err := d.db.ExecContext(ctx, query, valueArgs...)
	return err
}

func (d *Database) Close() error {
	return d.db.Close()
}
