// Package model defines shared domain types used across gateway and worker.
package model

import (
	"time"

	"github.com/google/uuid"
)

// TaskStatus mirrors the Python TaskStatus enum stored in PostgreSQL.
type TaskStatus string

const (
	StatusPending    TaskStatus = "PENDING"
	StatusRunning    TaskStatus = "RUNNING"
	StatusCompleted  TaskStatus = "COMPLETED"
	StatusFailed     TaskStatus = "FAILED"
	StatusDeadLetter TaskStatus = "DEAD_LETTER"
)

// Job represents a row in the jobs table.
type Job struct {
	ID             uuid.UUID  `json:"job_id"`
	IdempotencyKey string     `json:"idempotency_key"`
	JobType        string     `json:"job_type"`
	Status         TaskStatus `json:"status"`
	CreatedAt      time.Time  `json:"created_at"`
}

// JobTask represents a row in the job_tasks table.
type JobTask struct {
	ID          uuid.UUID  `json:"id"`
	JobID       uuid.UUID  `json:"job_id"`
	HandlerName string     `json:"handler_name"`
	Status      TaskStatus `json:"status"`
	Payload     []byte     `json:"payload"` // raw JSONB bytes
	RetryCount  int        `json:"retry_count"`
	MaxRetries  int        `json:"max_retries"`
	LockedBy    *string    `json:"locked_by"`
	HeartbeatAt *time.Time `json:"heartbeat_at"`
	LastError   *string    `json:"last_error"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// JobCreateRequest is the JSON body for POST /v1/jobs.
type JobCreateRequest struct {
	JobType     string         `json:"job_type"`
	HandlerName string         `json:"handler_name"`
	Payload     map[string]any `json:"payload"`
}

// JobResponse is the JSON body returned by POST /v1/jobs.
type JobResponse struct {
	JobID  string     `json:"job_id"`
	Status TaskStatus `json:"status"`
	Cached bool       `json:"cached"`
}
