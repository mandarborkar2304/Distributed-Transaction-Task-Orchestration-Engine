// Package metrics declares all Prometheus instruments for the Go gateway.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestsTotal counts all HTTP requests to the gateway by status code and method.
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_requests_total",
		Help: "Total HTTP requests handled by the Go gateway.",
	}, []string{"method", "status_code"})

	// LatencySeconds tracks end-to-end request latency including DB and Redis round-trips.
	LatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_latency_seconds",
		Help:    "End-to-end request latency of the Go gateway.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5, 5.0},
	}, []string{"handler"})

	// IdempotencyHitsTotal counts requests served from the Redis fast-path (no DB touch).
	IdempotencyHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_idempotency_hits_total",
		Help: "Total requests deduplicated by the Redis idempotency fast-path.",
	})

	// DBOperationsTotal counts PostgreSQL operations by type and result.
	DBOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_db_operations_total",
		Help: "Total PostgreSQL operations performed by the gateway.",
	}, []string{"operation", "result"})

	// WorkerTasksClaimed counts tasks claimed by the Go worker pool.
	WorkerTasksClaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_worker_tasks_claimed_total",
		Help: "Total tasks claimed by the Go worker via SKIP LOCKED.",
	})

	// WorkerTasksCompleted counts tasks completed by the Go worker pool, by outcome.
	WorkerTasksCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_worker_tasks_completed_total",
		Help: "Total tasks completed by the Go worker, labeled by outcome.",
	}, []string{"outcome"}) // outcome: completed | failed | dead_letter

	// ActiveWorkers tracks the number of goroutines actively processing tasks.
	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_active_workers",
		Help: "Number of goroutines currently processing tasks in the Go worker pool.",
	})
)
