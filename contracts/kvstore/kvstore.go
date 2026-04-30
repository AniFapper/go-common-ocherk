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

// Getter defines the read-only capabilities of the store.
type Getter[T any] interface {
	// Get retrieves the value for the given key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (T, error)
	// Exists checks for the presence of a key without retrieving its value.
	Exists(ctx context.Context, key string) (bool, error)
}

// Setter defines the "blind write" capabilities of the store.
type Setter[T any] interface {
	// Set unconditionally overwrites the value for the given key.
	Set(ctx context.Context, key string, value T, opts ...SetOption) error
}

// Deleter defines the removal capabilities of the store.
type Deleter interface {
	// Delete removes the key from the store.
	// Should be idempotent (return nil if the key is already gone).
	Delete(ctx context.Context, key string, opts ...DeleteOption) error
}

// Mutator defines the Atomic Compare-And-Swap (CAS) capabilities.
// This is essential for distributed counters, rate limiting, and
// concurrent entity updates.
type Mutator[T any] interface {
	// Mutate performs an atomic read-modify-write operation.
	// The provided function 'fn' takes the current state (nil if key doesn't exist)
	// and returns the desired new state or an error to abort the operation.
	//
	// CRITICAL: Since Mutate uses optimistic locking, 'fn' may be called multiple
	// times in case of write conflicts. It MUST be a pure function without side effects.
	Mutate(ctx context.Context, key string, fn func(current *T) (T, error)) error
}

// KVStore aggregates all specialized interfaces into a single contract.
type KVStore[T any] interface {
	Getter[T]
	Setter[T]
	Deleter
	Mutator[T]

	// Keys returns all existing keys in the store.
	Keys(ctx context.Context) ([]string, error)
}
