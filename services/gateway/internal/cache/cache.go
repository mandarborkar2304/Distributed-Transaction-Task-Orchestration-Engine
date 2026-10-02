// Package cache implements Redis-based idempotency fast-path and distributed locking.
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idempotencyTTL = 24 * time.Hour // mirrors Python's ex=86400
	lockTTL        = 60 * time.Second
)

// releaseLockLua is the atomic Lua compare-and-swap script for lock release.
// Mirrors the Python RELEASE_LOCK_LUA script exactly.
const releaseLockLua = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end`

// Client wraps go-redis with idempotency and distributed lock helpers.
type Client struct {
	rdb *redis.Client
}

// New creates a Redis client with connection pool tuned for high concurrency.
func New(addr string) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     500,  // goroutine-per-request model needs larger pool than Python asyncio
		MinIdleConns: 25,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
	return &Client{rdb: rdb}
}

// Close shuts down the Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Ping validates connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// GetIdempotencyKey returns the cached JSON value for an idempotency key, or ("", false, nil) on miss.
func (c *Client) GetIdempotencyKey(ctx context.Context, key string) (string, bool, error) {
	val, err := c.rdb.Get(ctx, idempKeyName(key)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("redis GET: %w", err)
	}
	return val, true, nil
}

// SetIdempotencyKey caches the JSON response for an idempotency key with a 24-hour TTL.
func (c *Client) SetIdempotencyKey(ctx context.Context, key, value string) error {
	return c.rdb.Set(ctx, idempKeyName(key), value, idempotencyTTL).Err()
}

// AcquireLock attempts to acquire a distributed lock using SET NX EX.
// Returns true if the lock was successfully acquired.
func (c *Client) AcquireLock(ctx context.Context, taskID, ownerID string) (bool, error) {
	ok, err := c.rdb.SetNX(ctx, lockKey(taskID), ownerID, lockTTL).Result()
	return ok, err
}

// RenewLock extends the TTL of a held lock. Called every heartbeat tick.
func (c *Client) RenewLock(ctx context.Context, taskID string) error {
	return c.rdb.Expire(ctx, lockKey(taskID), lockTTL).Err()
}

// ReleaseLock releases the lock atomically via Lua CAS — only if ownerID matches.
// Returns true if the lock was held by this owner and was released.
func (c *Client) ReleaseLock(ctx context.Context, taskID, ownerID string) (bool, error) {
	res, err := c.rdb.Eval(ctx, releaseLockLua, []string{lockKey(taskID)}, ownerID).Int()
	if err != nil {
		return false, fmt.Errorf("lua release lock: %w", err)
	}
	return res == 1, nil
}

// DeleteLock forcibly deletes a lock key (used by watchdog-equivalent cleanup).
func (c *Client) DeleteLock(ctx context.Context, taskID string) error {
	return c.rdb.Del(ctx, lockKey(taskID)).Err()
}

func idempKeyName(key string) string { return "idemp:" + key }
func lockKey(taskID string) string   { return "lock:task:" + taskID }
