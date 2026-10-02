// Gateway is the high-performance Go HTTP ingestion service.
// It handles POST /v1/jobs with sub-millisecond Redis idempotency deduplication
// and connection-pooled PostgreSQL writes via pgxpool.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mandarborkar2304/orchestrator/gateway/internal/cache"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/db"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/idempotency"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbDSN := envOr("DATABASE_DSN", "postgres://postgres:postgres@localhost:5432/orchestrator")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	listenAddr := envOr("GATEWAY_ADDR", ":8080")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Connect to PostgreSQL.
	pool, err := db.New(ctx, dbDSN)
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("PostgreSQL pool ready", "dsn", maskPassword(dbDSN))

	// Connect to Redis.
	rc := cache.New(redisAddr)
	if err := rc.Ping(ctx); err != nil {
		logger.Error("failed to ping Redis", "err", err)
		os.Exit(1)
	}
	defer rc.Close()
	logger.Info("Redis client ready", "addr", redisAddr)

	mux := http.NewServeMux()

	// Ingestion endpoint.
	mux.HandleFunc("POST /v1/jobs", idempotency.NewHandler(pool, rc, logger))

	// Prometheus metrics.
	mux.Handle("GET /metrics", promhttp.Handler())

	// Health probe (used by Docker / k8s readiness probes).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
	})

	// Real-time architecture visualizer & simulation console.
	mux.HandleFunc("GET /demo", func(w http.ResponseWriter, r *http.Request) {
		candidates := []string{
			"web/index.html",
			"../../web/index.html",
			"../web/index.html",
			"/workspaces/Distributed-Transaction-Task-Orchestration-Engine/web/index.html",
		}
		for _, p := range candidates {
			if data, err := os.ReadFile(p); err == nil {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.Write(data) //nolint:errcheck
				return
			}
		}
		http.Error(w, "Demo visualizer not found", http.StatusNotFound)
	})

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in a goroutine so signal handling doesn't block.
	go func() {
		logger.Info("Go gateway listening", "addr", listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("ListenAndServe error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	logger.Info("gateway stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func maskPassword(dsn string) string {
	// Very lightweight masking — just for log safety.
	return fmt.Sprintf("%s...", dsn[:min(20, len(dsn))])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ensure strconv is used (linter)
var _ = strconv.Itoa
