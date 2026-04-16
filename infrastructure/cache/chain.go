package cache

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/AniFapper/go-common-ocherk/contracts/kvstore"
)

type ChainKV[T any] struct {
	layers      []kvstore.KVStore[T]
	backfillTTL time.Duration // <--- ДОБАВЛЕНО
}

// Pass stores in order of speed: NewChainKV(time.Minute, inMemoryStore, natsJetStreamStore)
func NewChainKV[T any](backfillTTL time.Duration, layers ...kvstore.KVStore[T]) *ChainKV[T] {
	return &ChainKV[T]{
		layers:      layers,
		backfillTTL: backfillTTL,
	}
}

func (c *ChainKV[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	missedLayers := make([]kvstore.KVStore[T], 0, len(c.layers))

	for i, layer := range c.layers {
		val, err := layer.Get(ctx, key)

		if err == nil {
			for _, missed := range missedLayers {
				// ИСПРАВЛЕНО: Добавлен TTL для возвращаемых данных
				if err := missed.Set(ctx, key, val, kvstore.WithTTL(c.backfillTTL)); err != nil {
					slog.Warn("cache chain backfill failed", "key", key, "layer_index", i)
				}
			}
			return val, nil
		}

		if errors.Is(err, kvstore.ErrNotFound) {
			missedLayers = append(missedLayers, layer)
			continue
		}

		return zero, err
	}

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

func (c *ChainKV[T]) Delete(ctx context.Context, key string, opts ...kvstore.DeleteOption) error {
	var errs error
	for _, layer := range c.layers {
		if err := layer.Delete(ctx, key); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (c *ChainKV[T]) Exists(ctx context.Context, key string) (bool, error) {
	for _, layer := range c.layers {
		exists, err := layer.Exists(ctx, key)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}
