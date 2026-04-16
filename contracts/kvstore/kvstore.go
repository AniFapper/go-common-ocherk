package kvstore

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when the requested key does not exist in the store.
var ErrNotFound = errors.New("key not found")

// --- Set Options ---

type SetOptions struct {
	TTL   *time.Duration
	Async bool
}

type SetOption func(*SetOptions)

func WithTTL(ttl time.Duration) SetOption {
	return func(o *SetOptions) {
		o.TTL = &ttl
	}
}

func WithAsync() SetOption {
	return func(o *SetOptions) {
		o.Async = true
	}
}

// --- Delete Options ---

type DeleteOptions struct {
	Async bool
}

type DeleteOption func(*DeleteOptions)

// WithAsyncDelete returns a DeleteOption that tells the cache to perform
// remote deletion and pubsub broadcasting in the background.
func WithAsyncDelete() DeleteOption {
	return func(o *DeleteOptions) {
		o.Async = true
	}
}

// --- Interface ---

// KVStore defines a generic, type-safe interface for key-value storage.
// It abstracts backends like Redis, NATS Jetstream KV, or in-memory maps.
type KVStore[T any] interface {
	// Set stores a value by key with optional configurations.
	Set(ctx context.Context, key string, value T, opts ...SetOption) error

	// Get retrieves the value associated with the key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (T, error)

	// Delete removes the value associated with the key with optional configurations.
	Delete(ctx context.Context, key string, opts ...DeleteOption) error

	// Exists checks for the presence of a key without retrieving its value.
	Exists(ctx context.Context, key string) (bool, error)
}
