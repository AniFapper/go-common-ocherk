package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/AniFapper/go-common-ocherk/contracts/kvstore"
	"github.com/AniFapper/go-common-ocherk/contracts/pubsub"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// InvalidationMessage represents a broadcast event sent to all distributed nodes
// to notify them that a specific cache key has been mutated and must be evicted from L1.
type InvalidationMessage struct {
	Key      string `json:"key"`
	SenderID string `json:"sender_id"` // Used to prevent the sender from processing its own invalidation
}

// DistributedCache implements a two-tier (L1/L2) caching strategy designed for high-throughput,
// distributed microservices.
//
// Architecture Overview:
//   - Tier 1 (Local): Fast, in-memory KV store. Acts as a hot data cache to eliminate network hops.
//   - Tier 2 (Shared): Centralized distributed KV store (e.g., Redis). Acts as the single source of truth.
//   - Pub/Sub: Message bus (e.g., NATS) used to broadcast L1 eviction events across the cluster.
//
// Concurrency Protections:
//   - Cache Stampede: Mitigated using golang.org/x/sync/singleflight for concurrent Get requests.
//   - Split-Brain / Stale Cache: Mitigated using a strict Write-Around pattern (Write to L2 -> Broadcast -> Delete L1).
type DistributedCache[T any] struct {
	local       kvstore.KVStore[T]
	shared      kvstore.KVStore[T]
	ps          pubsub.PubSub[InvalidationMessage]
	topic       string
	instanceID  string
	unsub       pubsub.Unsubscriber
	backfillTTL time.Duration
	sfg         singleflight.Group
}

// NewDistributedCache initializes a new DistributedCache instance, generating a unique Node ID
// and immediately subscribing to the Pub/Sub invalidation topic.
//
// Parameters:
//   - backfillTTL: A mandatory upper-bound duration for how long data can reside in the L1 cache.
//     This serves as the ultimate failsafe against network partitions (split-brain) missing invalidation events.
func NewDistributedCache[T any](
	local kvstore.KVStore[T],
	shared kvstore.KVStore[T],
	ps pubsub.PubSub[InvalidationMessage],
	topic string,
	backfillTTL time.Duration,
) (*DistributedCache[T], error) {
	dc := &DistributedCache[T]{
		local:       local,
		shared:      shared,
		ps:          ps,
		topic:       topic,
		instanceID:  uuid.New().String(),
		backfillTTL: backfillTTL,
	}

	if err := dc.listen(); err != nil {
		return nil, fmt.Errorf("failed to init distributed cache invalidation: %w", err)
	}

	return dc, nil
}

// listen starts the background subscriber for invalidation events.
func (c *DistributedCache[T]) listen() error {
	unsub, err := c.ps.Subscribe(context.Background(), c.topic, func(ctx context.Context, msg InvalidationMessage) error {
		// Ignore self-published events since local eviction is handled synchronously in Set/Delete
		if msg.SenderID == c.instanceID {
			return nil
		}
		// Gracefully evict the key from the local L1 cache
		return c.local.Delete(ctx, msg.Key)
	})

	if err != nil {
		return err
	}

	c.unsub = unsub
	return nil
}

// Close gracefully terminates the Pub/Sub invalidation listener.
// It must be called during application shutdown to prevent resource leaks.
func (c *DistributedCache[T]) Close() error {
	if c.unsub != nil {
		return c.unsub()
	}
	return nil
}

// Get retrieves a value from the cache.
// It follows a read-through pattern with Singleflight protection:
//  1. Check fast-path (L1).
//  2. On miss, group concurrent requests for the same key.
//  3. Fetch from source of truth (L2).
//  4. Backfill L1 with a safety TTL.
func (c *DistributedCache[T]) Get(ctx context.Context, key string) (T, error) {
	// Fast-path: Local L1 read
	val, err := c.local.Get(ctx, key)
	if err == nil {
		return val, nil
	}

	// Slow-path: Coalesce concurrent requests (Cache Stampede Protection)
	result, err, _ := c.sfg.Do(key, func() (interface{}, error) {
		sharedVal, sharedErr := c.shared.Get(ctx, key)
		if sharedErr != nil {
			return nil, sharedErr
		}

		// Backfill L1.
		// context.WithoutCancel ensures the backfill completes even if the original
		// caller times out or disconnects mid-flight.
		_ = c.local.Set(context.WithoutCancel(ctx), key, sharedVal, kvstore.WithTTL(c.backfillTTL))
		return sharedVal, nil
	})

	if err != nil {
		var zero T
		return zero, err
	}

	return result.(T), nil
}

// Set mutates a value in the cache cluster using a Write-Around strategy.
//
// Execution Flow:
//  1. Write to Shared L2 (Source of Truth).
//  2. Broadcast Invalidation Event to all cluster nodes.
//  3. Always evict from Local L1 (Defer block).
//
// Note on Write-Around: We do NOT write the new value directly to L1.
// Instead, we delete it from L1, forcing the next Get() to safely pull the
// most authoritative state from L2, bypassing race conditions between concurrent writers.
func (c *DistributedCache[T]) Set(ctx context.Context, key string, value T, opts ...kvstore.SetOption) error {
	// Isolate execution from caller cancellation to prevent partial writes
	safeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)

	// LIFO Defer Stack:
	// 1. cancel() is pushed first (executes last).
	// 2. The cleanup func is pushed second (executes first).
	defer cancel()

	defer func() {
		// Guaranteed L1 eviction regardless of network panics or errors.
		_ = c.local.Delete(safeCtx, key)
		// Release the singleflight lock, allowing future Get() calls to refetch
		c.sfg.Forget(key)
	}()

	// Step 1: Commit to Source of Truth
	if err := c.shared.Set(safeCtx, key, value, opts...); err != nil {
		return fmt.Errorf("distributed cache: shared set failed: %w", err)
	}

	// Step 2: Fail-Fast Invalidations
	err := c.ps.Publish(safeCtx, c.topic, InvalidationMessage{
		Key:      key,
		SenderID: c.instanceID,
	})
	if err != nil {
		return fmt.Errorf("distributed cache: invalidation broadcast failed: %w", err)
	}

	return nil
}

// Delete removes a key from the cache cluster entirely.
// Similar to Set, it strictly enforces the L2 -> Broadcast -> L1 flow.
func (c *DistributedCache[T]) Delete(ctx context.Context, key string, opts ...kvstore.DeleteOption) error {
	safeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	defer func() {
		// Guaranteed local cleanup
		_ = c.local.Delete(safeCtx, key)
		c.sfg.Forget(key)
	}()

	if err := c.shared.Delete(safeCtx, key); err != nil {
		return fmt.Errorf("distributed cache: shared delete failed: %w", err)
	}

	err := c.ps.Publish(safeCtx, c.topic, InvalidationMessage{
		Key:      key,
		SenderID: c.instanceID,
	})
	if err != nil {
		return fmt.Errorf("distributed cache: invalidation broadcast failed: %w", err)
	}

	return nil
}

// Exists performs a lightweight check to determine if a key exists.
// It skips the heavy backfill process (no singleflight/writeback) to optimize performance.
func (c *DistributedCache[T]) Exists(ctx context.Context, key string) (bool, error) {
	ok, err := c.local.Exists(ctx, key)
	if err == nil && ok {
		return true, nil
	}
	return c.shared.Exists(ctx, key)
}

func (c *DistributedCache[T]) Mutate(ctx context.Context, key string, fn func(current *T) (T, error)) error {
	// 1. Pre-check capability to fail fast
	sharedMutator, ok := c.shared.(kvstore.Mutator[T])
	if !ok {
		return fmt.Errorf("distributed cache: shared store %T does not support Mutator interface", c.shared)
	}

	// 2. Detach from caller context to ensure cluster-wide consistency
	// We use a slightly longer timeout for the whole orchestration
	safeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 7*time.Second)
	defer cancel()

	// 3. Local cleanup must ALWAYS happen to prevent stale local state
	defer func() {
		_ = c.local.Delete(safeCtx, key)
		c.sfg.Forget(key)
	}()

	// 4. Atomic mutation in L2 (Source of Truth)
	if err := sharedMutator.Mutate(safeCtx, key, fn); err != nil {
		return fmt.Errorf("distributed cache: shared mutation failed: %w", err)
	}

	// 5. Cluster-wide invalidation
	// If this fails, we have eventual consistency issues until L1 TTL expires.
	err := c.ps.Publish(safeCtx, c.topic, InvalidationMessage{
		Key:      key,
		SenderID: c.instanceID,
	})
	if err != nil {
		// Log this as a critical sync error!
		return fmt.Errorf("distributed cache: L2 updated but invalidation failed: %w", err)
	}

	return nil
}
