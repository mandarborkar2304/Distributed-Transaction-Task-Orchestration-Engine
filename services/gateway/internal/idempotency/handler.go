// Package idempotency implements the HTTP ingestion handler with Redis fast-path
// deduplication and PostgreSQL transactional outbox persistence.
package idempotency

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/mandarborkar2304/orchestrator/gateway/internal/cache"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/db"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/metrics"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/model"
)

// NewHandler creates an http.HandlerFunc for POST /v1/jobs with two-tier idempotency.
func NewHandler(pool *db.Pool, rc *cache.Client, logger *slog.Logger) http.HandlerFunc {
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
		} else if err != nil && logger != nil {
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
			if logger != nil {
				logger.Error("UpsertJob failed", "key", idempKey, "err", err)
			}
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
			Cached: !isNew,
		}

		// Populate Redis cache for subsequent requests.
		if payload, err := json.Marshal(resp); err == nil {
			if err := rc.SetIdempotencyKey(reqCtx, idempKey, string(payload)); err != nil && logger != nil {
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
