package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prasanth-33460/in-memory_aggregator/internal/database"
	"github.com/prasanth-33460/in-memory_aggregator/internal/handler"
	aggregator "github.com/prasanth-33460/in-memory_aggregator/internal/service"
)

func main() {
	log.Println("Starting In-Memory Aggregator Service...")

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables or defaults")
	}

	dbConnStr := os.Getenv("DB_CONN_STR")
	if dbConnStr == "" {
		dbConnStr = "postgres://postgres:postgres@localhost:5432/aggregator?sslmode=disable"
		log.Printf("DB_CONN_STR not set, using default: %s", dbConnStr)
	}

	var db *database.Database
	var err error
	maxRetries := 10
	retryInterval := 3 * time.Second

	for i := 0; i < maxRetries; i++ {
		db, err = database.NewDatabase(dbConnStr)
		if err == nil {
			log.Println("Successfully connected to database")
			break
		}
		log.Printf("Failed to connect to database (attempt %d/%d): %v. Retrying in %v...", i+1, maxRetries, err, retryInterval)
		time.Sleep(retryInterval)
	}

	if err != nil {
		log.Fatalf("Failed to connect to database after %d attempts: %v", maxRetries, err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.InitSchema(ctx); err != nil {
		log.Fatalf("Failed to initialize database schema: %v", err)
	}

	agg := aggregator.NewAggregator()
	flusher := aggregator.NewFlusher(agg, db, 5*time.Second)
	flusher.Start(ctx)
	h := handler.NewHandler(agg, flusher)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/signal/ingest", h.IngestSignal)
	mux.HandleFunc("/health", h.HealthCheck)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Printf("Server listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	flusher.Stop(shutdownCtx)

	log.Println("Server exited properly")
}
