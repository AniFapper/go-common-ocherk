package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/AniFapper/go-common-ocherk/contracts/kvstore"
	"github.com/AniFapper/go-common-ocherk/contracts/marshaller"
	"github.com/nats-io/nats.go/jetstream"
)

// NatsKVStore provides a NATS JetStream implementation of the kvstore.KVStore interface.
// It delegates data serialization to a provided Marshaller, allowing flexible encoding (JSON, Proto).
//
// Note on TTL: In NATS JetStream KV, TTL is configured at the bucket level (MaxAge)
// during bucket creation, rather than per-key. The WithTTL option is ignored here.
type NatsKVStore[T any] struct {
	kv    jetstream.KeyValue
	codec marshaller.Marshaller
}

// NewNatsKVStore creates a new instance of NatsKVStore.
// It requires a JetStream KeyValue bucket instance and a Marshaller.
func NewNatsKVStore[T any](kv jetstream.KeyValue, codec marshaller.Marshaller) *NatsKVStore[T] {
	return &NatsKVStore[T]{
		kv:    kv,
		codec: codec,
	}
}

// Set serializes the value using the configured codec and stores it in the JetStream KV bucket.
func (s *NatsKVStore[T]) Set(ctx context.Context, key string, value T, opts ...kvstore.SetOption) error {
	// Delegate serialization to the Marshaller
	data, err := s.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("nats kv set: encoding failed: %w", err)
	}

	// We ignore kvstore.WithTTL options here, because NATS manages TTL at the bucket level.
	_, err = s.kv.Put(ctx, key, data)
	if err != nil {
		return fmt.Errorf("nats kv set: %w", err)
	}

	return nil
}

// Get retrieves a key from JetStream KV and deserializes it back into type T.
// Returns kvstore.ErrNotFound if the key does not exist.
func (s *NatsKVStore[T]) Get(ctx context.Context, key string) (T, error) {
	var result T

	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		// Map NATS-specific error to our domain-agnostic interface error
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return result, kvstore.ErrNotFound
		}
		return result, fmt.Errorf("nats kv get: %w", err)
	}

	// Delegate deserialization to the Marshaller
	if err := s.codec.Unmarshal(entry.Value(), &result); err != nil {
		return result, fmt.Errorf("nats kv get: decoding failed: %w", err)
	}

	return result, nil
}

// Delete removes the value associated with the key from the bucket.
func (s *NatsKVStore[T]) Delete(ctx context.Context, key string, opts ...kvstore.DeleteOption) error {
	err := s.kv.Delete(ctx, key)
	// If the key is already gone, we don't treat it as an error for idempotency
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("nats kv delete: %w", err)
	}
	return nil
}

// Exists checks if a key is present in the bucket without downloading its full payload.
func (s *NatsKVStore[T]) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("nats kv exists: %w", err)
	}
	return true, nil
}

// Mutate implements the kvstore.Mutator interface using NATS JetStream optimistic locking.
// It automatically handles version conflicts by retrying the read-modify-write cycle.
func (s *NatsKVStore[T]) Mutate(ctx context.Context, key string, mutateFunc func(current *T) (T, error)) error {
	const maxRetries = 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 1. Check if the operation was cancelled before starting a new cycle
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("nats kv mutate aborted: %w", err)
		}

		var current T
		var revision uint64

		// 2. Fetch the current state and its revision (version)
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			if !errors.Is(err, jetstream.ErrKeyNotFound) {
				return fmt.Errorf("nats kv get failed: %w", err)
			}
			// Key doesn't exist; current remains zero value and revision remains 0
		} else {
			revision = entry.Revision()
			if err := s.codec.Unmarshal(entry.Value(), &current); err != nil {
				return fmt.Errorf("nats kv unmarshal failed: %w", err)
			}
		}

		var currentPtr *T
		if revision > 0 {
			currentPtr = &current
		}

		// 3. Execute the transformation logic
		newValue, err := mutateFunc(currentPtr)
		if err != nil {
			return err // Business logic requested an abort
		}

		data, err := s.codec.Marshal(newValue)
		if err != nil {
			return fmt.Errorf("nats kv marshal failed: %w", err)
		}

		// 4. Attempt an atomic write based on the fetched revision
		if revision == 0 {
			// Ensure we only create the key if it still doesn't exist
			_, err = s.kv.Create(ctx, key, data)
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue // Conflict: someone else created it. Retry.
			}
		} else {
			// Update only if the revision matches what we read
			_, err = s.kv.Update(ctx, key, data, revision)

			// Check for JetStream sequence mismatch (standard CAS error)
			var apiErr *jetstream.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				continue // Conflict: someone else updated it. Retry.
			}
		}

		if err != nil {
			return fmt.Errorf("nats kv commit failed: %w", err)
		}

		return nil // Success
	}

	return fmt.Errorf("nats kv mutate: failed to resolve conflict after %d attempts", maxRetries)
}

// Keys returns all keys in the bucket.
func (s *NatsKVStore[T]) Keys(ctx context.Context) ([]string, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		return nil, fmt.Errorf("nats kv keys: %w", err)
	}
	return keys, nil
}
