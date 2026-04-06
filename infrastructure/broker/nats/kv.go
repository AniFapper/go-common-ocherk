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
func (s *NatsKVStore[T]) Delete(ctx context.Context, key string) error {
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
