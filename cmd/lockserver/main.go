package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"lockserver/internal/api"
	"lockserver/internal/model"
	"lockserver/internal/obs"
	"lockserver/internal/storage"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Cancel context on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbPath := getenv("LOCKSERVER_DB", "./lockserver.db")
	addr := getenv("LOCKSERVER_ADDR", ":8080")

	db, err := storage.Open(ctx, storage.Config{
		Path:         dbPath,
		BusyTimeout:  5 * time.Second,
		MaxOpenConns: 20,
		MaxIdleConns: 20,
	})
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	logger := obs.NewLogger()
	metrics := obs.NewMetrics()

	svc := model.NewService(db.DB, logger, metrics)
	apiServer := api.NewServer(svc)

	// Expiration monitor
	mon := model.NewExpirationMonitor(db.DB, logger, metrics, 500*time.Millisecond)

	mux := http.NewServeMux()
	mux.Handle("/", apiServer.Handler())
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup

	// Start expiration monitor
	wg.Add(1)
	go func() {
		defer wg.Done()
		mon.Run(ctx) // exits when ctx is cancelled
	}()

	// Start HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("lockserver up addr=%s db=%s", addr, dbPath)
		// ListenAndServe returns http.ErrServerClosed on graceful shutdown.
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
			// If server fails unexpectedly, trigger shutdown.
			stop()
		}
	}()

	// Wait for signal
	<-ctx.Done()
	log.Printf("shutdown signal received")

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	// Wait for goroutines to finish
	wg.Wait()
	log.Printf("lockserver stopped")
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}