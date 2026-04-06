package cache

import (
	"context"
	"errors"
	"log/slog"

	"github.com/AniFapper/go-common-ocherk/contracts/kvstore"
)

// ChainKV implements kvstore.KVStore[T] by chaining multiple storage layers (e.g., L1 and L2).
// It handles automatic fallback and "backfilling" (promoting data to faster layers).
type ChainKV[T any] struct {
	layers []kvstore.KVStore[T]
}

// NewChainKV creates a multi-level cache.
// Pass stores in order of speed: NewChainKV(inMemoryStore, natsJetStreamStore)
func NewChainKV[T any](layers ...kvstore.KVStore[T]) *ChainKV[T] {
	return &ChainKV[T]{
		layers: layers,
	}
}

func (c *ChainKV[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	var missedLayers []kvstore.KVStore[T]

	for i, layer := range c.layers {
		val, err := layer.Get(ctx, key)

		if err == nil {
			// Backfill logic: If we found the value in L2, we should save it back to L1
			for _, missed := range missedLayers {
				// We ignore errors on backfill to not break the successful read
				if err := missed.Set(ctx, key, val); err != nil {
					slog.Warn("cache chain backfill failed", "key", key, "layer_index", i)
				}
			}
			return val, nil
		}

		if errors.Is(err, kvstore.ErrNotFound) {
			// Record that this layer missed, so we can backfill it later
			missedLayers = append(missedLayers, layer)
			continue
		}

		// If a real error occurred (network down), we return immediately
		return zero, err
	}

	// If we looped through all layers and didn't return, it means everyone returned ErrNotFound
	return zero, kvstore.ErrNotFound
}

func (c *ChainKV[T]) Set(ctx context.Context, key string, value T, opts ...kvstore.SetOption) error {
	// Write-through pattern: Save to ALL layers simultaneously
	for _, layer := range c.layers {
		if err := layer.Set(ctx, key, value, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (c *ChainKV[T]) Delete(ctx context.Context, key string) error {
	var errs error
	for _, layer := range c.layers {
		if err := layer.Delete(ctx, key); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (c *ChainKV[T]) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.Get(ctx, key)
	return err == nil, nil
}
