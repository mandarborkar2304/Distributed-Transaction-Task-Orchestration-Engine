// Gateway is the high-performance Go HTTP ingestion service.
// It handles POST /v1/jobs with sub-millisecond Redis idempotency deduplication
// and connection-pooled PostgreSQL writes via pgxpool.
package main

import (
	"context"
	"encoding/json"
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
	"github.com/mandarborkar2304/orchestrator/gateway/internal/metrics"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/model"
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
	mux.HandleFunc("POST /v1/jobs", makeJobHandler(pool, rc, logger))

	// Prometheus metrics.
	mux.Handle("GET /metrics", promhttp.Handler())

	// Health probe (used by Docker / k8s readiness probes).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
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

// makeJobHandler returns the POST /v1/jobs handler closed over the pool and redis client.
func makeJobHandler(pool *db.Pool, rc *cache.Client, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Use a detached per-request context with its own timeout.
		// This prevents end-of-load-test program-level context cancellation
		// from poisoning in-flight PostgreSQL transactions with context.Canceled.
		reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer reqCancel()

		idempKey := r.Header.Get("Idempotency-Key")
		if idempKey == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Idempotency-Key header is missing"})
			metrics.RequestsTotal.WithLabelValues("POST", "400").Inc()
			return
		}

		// ── Tier 1: Redis fast-path ──────────────────────────────────────────────
		if cached, hit, err := rc.GetIdempotencyKey(reqCtx, idempKey); err == nil && hit {
			var resp model.JobResponse
			if jsonErr := json.Unmarshal([]byte(cached), &resp); jsonErr == nil {
				resp.Cached = true
				metrics.IdempotencyHitsTotal.Inc()
				metrics.RequestsTotal.WithLabelValues("POST", "202").Inc()
				metrics.LatencySeconds.WithLabelValues("create_job").Observe(time.Since(start).Seconds())
				writeJSON(w, http.StatusAccepted, resp)
				return
			}
		} else if err != nil {
			logger.Warn("redis fast-path error", "key", idempKey, "err", err)
		}

		// ── Parse request body ───────────────────────────────────────────────────
		var req model.JobCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "invalid JSON body"})
			metrics.RequestsTotal.WithLabelValues("POST", "400").Inc()
			return
		}

		// ── Tier 2: Transactional outbox (PostgreSQL) ────────────────────────────
		job, isNew, err := pool.UpsertJob(reqCtx, idempKey, req.JobType, req.HandlerName, req.Payload)
		if err != nil {
			logger.Error("UpsertJob failed", "key", idempKey, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "internal error"})
			metrics.RequestsTotal.WithLabelValues("POST", "500").Inc()
			metrics.DBOperationsTotal.WithLabelValues("upsert_job", "error").Inc()
			return
		}
		if isNew {
			metrics.DBOperationsTotal.WithLabelValues("upsert_job", "inserted").Inc()
		} else {
			metrics.DBOperationsTotal.WithLabelValues("upsert_job", "conflict").Inc()
		}

		resp := model.JobResponse{
			JobID:  job.ID.String(),
			Status: job.Status,
			Cached: false,
		}

		// Populate Redis cache for subsequent requests.
		if payload, err := json.Marshal(resp); err == nil {
			if err := rc.SetIdempotencyKey(reqCtx, idempKey, string(payload)); err != nil {
				logger.Warn("redis cache write error", "key", idempKey, "err", err)
			}
		}

		metrics.RequestsTotal.WithLabelValues("POST", "202").Inc()
		metrics.LatencySeconds.WithLabelValues("create_job").Observe(time.Since(start).Seconds())
		writeJSON(w, http.StatusAccepted, resp)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
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
