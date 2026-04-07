package cache

import (
	"context"
	"errors"

	"github.com/AniFapper/go-common-ocherk/contracts/kvstore"
	"github.com/AniFapper/go-common-ocherk/contracts/pubsub"
	"github.com/google/uuid"
)

// InvalidationMessage represents a command sent across the microservice cluster
// to evict a specific key from local L1 caches.
type InvalidationMessage struct {
	Key      string `json:"key"`
	SenderID string `json:"sender_id"` // Prevents the sender from deleting its own newly set key
}

// DistributedCache is a coordinator between a fast local cache (e.g., In-Memory)
// and a reliable shared cache (e.g., NATS KV, Redis, or a Chain of them).
// It automatically handles L1 cache invalidation across multiple instances via PubSub.
type DistributedCache[T any] struct {
	local      kvstore.KVStore[T]
	shared     kvstore.KVStore[T]
	ps         pubsub.PubSub[InvalidationMessage]
	topic      string
	instanceID string
}

// NewDistributedCache initializes a new distributed cache coordinator.
// 'local' should be the fast L1 in-memory store.
// 'shared' should be the remote L2 store (or a ChainKV of multiple remote stores).
func NewDistributedCache[T any](
	local kvstore.KVStore[T],
	shared kvstore.KVStore[T],
	ps pubsub.PubSub[InvalidationMessage],
	topic string,
) *DistributedCache[T] {
	dc := &DistributedCache[T]{
		local:      local,
		shared:     shared,
		ps:         ps,
		topic:      topic,
		instanceID: uuid.New().String(), // Unique ID for this specific microservice instance
	}

	// Start listening for invalidation events from other instances
	go dc.listen()

	return dc
}

// listen subscribes to the invalidation topic and removes keys from the local cache
// when another instance modifies them.
func (c *DistributedCache[T]) listen() {
	_, _ = c.ps.Subscribe(context.Background(), c.topic, func(ctx context.Context, msg InvalidationMessage) error {
		// Ignore messages sent by this very instance
		if msg.SenderID == c.instanceID {
			return nil
		}
		// Evict the key ONLY from the local cache. The shared cache is already updated.
		return c.local.Delete(ctx, msg.Key)
	})
}

// Get retrieves a value, prioritizing the local cache.
// If a cache miss occurs locally, it fetches from the shared cache and safely backfills the local one.
func (c *DistributedCache[T]) Get(ctx context.Context, key string) (T, error) {
	// 1. Try to fetch from the fast local layer
	val, err := c.local.Get(ctx, key)
	if err == nil {
		return val, nil
	}

	// 2. If not found locally, fetch from the shared layer(s)
	val, err = c.shared.Get(ctx, key)
	if err == nil {
		// 3. Silent Backfill: promote the data to the local cache without broadcasting invalidation.
		// We use context.WithoutCancel to ensure the backfill completes even if the original
		// client request times out or is canceled exactly at this millisecond.
		safeCtx := context.WithoutCancel(ctx)
		_ = c.local.Set(safeCtx, key, val)
		return val, nil
	}

	return val, err
}

// Set writes the value to both local and shared caches, then broadcasts an invalidation event.
// It uses a detached context to prevent partial updates (split-brain) on client cancellation.
func (c *DistributedCache[T]) Set(ctx context.Context, key string, value T, opts ...kvstore.SetOption) error {
	// Create a safe context detached from client cancellation (timeout/abort),
	// ensuring the distributed transaction finishes atomically.
	safeCtx := context.WithoutCancel(ctx)

	// Write to both layers synchronously
	if err := c.local.Set(safeCtx, key, value, opts...); err != nil {
		return err
	}
	if err := c.shared.Set(safeCtx, key, value, opts...); err != nil {
		return err
	}

	// Broadcast to other instances: "I updated this key, drop it from your local memory!"
	return c.ps.Publish(safeCtx, c.topic, InvalidationMessage{
		Key:      key,
		SenderID: c.instanceID,
	})
}

// Delete removes the key from both caches and broadcasts the deletion safely.
func (c *DistributedCache[T]) Delete(ctx context.Context, key string) error {
	var errs error

	// Create a safe context to ensure both caches and the pubsub broadcast
	// are executed completely, regardless of client timeouts.
	safeCtx := context.WithoutCancel(ctx)

	if err := c.local.Delete(safeCtx, key); err != nil {
		errs = errors.Join(errs, err)
	}
	if err := c.shared.Delete(safeCtx, key); err != nil {
		errs = errors.Join(errs, err)
	}

	// Broadcast to other instances: "I deleted this key, drop it from your local memory too!"
	// We ignore the error here to avoid masking the potential cache deletion errors,
	// but it executes safely.
	_ = c.ps.Publish(safeCtx, c.topic, InvalidationMessage{
		Key:      key,
		SenderID: c.instanceID,
	})

	return errs
}

// Exists checks if the key is present in either cache.
func (c *DistributedCache[T]) Exists(ctx context.Context, key string) (bool, error) {
	// We delegate to Get to take advantage of the backfill mechanism.
	// If it exists in L2 but not L1, checking existence will silently cache it in L1.
	_, err := c.Get(ctx, key)
	return err == nil, nil
}
