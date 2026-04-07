package kvstore

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when the requested key does not exist in the store.
var ErrNotFound = errors.New("key not found")

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

// KVStore defines a generic, type-safe interface for key-value storage.
// It abstracts backends like Redis, NATS Jetstream KV, or in-memory maps.
//
// Example of usage with a custom struct:
//
//	type User struct {
//	    ID    string
//	    Email string
//	}
//
//	func Example(ctx context.Context, store KVStore[User]) {
//	    user := User{ID: "1", Email: "test@example.com"}
//
//	    // Set with TTL
//	    _ = store.Set(ctx, "user:1", user, WithTTL(10*time.Minute))
//
//	    // Get typed data
//	    val, err := store.Get(ctx, "user:1")
//	    if err == nil {
//	        fmt.Println(val.Email)
//	    }
//	}
type KVStore[T any] interface {
	// Set stores a value by key with optional configurations.
	Set(ctx context.Context, key string, value T, opts ...SetOption) error

	// Get retrieves the value associated with the key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (T, error)

	// Delete removes the value associated with the key.
	Delete(ctx context.Context, key string) error

	// Exists checks for the presence of a key without retrieving its value.
	Exists(ctx context.Context, key string) (bool, error)
}
