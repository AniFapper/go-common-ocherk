package cache

import (
	"context"
	"sync"
	"time"

	"github.com/AniFapper/go-common-ocherk/contracts/kvstore"
)

type item[T any] struct {
	value     T
	expiresAt *time.Time
}

// InMemoryKV implements kvstore.KVStore[T] for local memory (L1 Cache).
type InMemoryKV[T any] struct {
	mu   sync.RWMutex
	data map[string]item[T]
}

func NewInMemoryKV[T any]() *InMemoryKV[T] {
	return &InMemoryKV[T]{
		data: make(map[string]item[T]),
	}
}

func (m *InMemoryKV[T]) Set(ctx context.Context, key string, value T, opts ...kvstore.SetOption) error {
	options := &kvstore.SetOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var expiresAt *time.Time
	if options.TTL != nil {
		t := time.Now().Add(*options.TTL)
		expiresAt = &t
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = item[T]{
		value:     value,
		expiresAt: expiresAt,
	}
	return nil
}

func (m *InMemoryKV[T]) Get(ctx context.Context, key string) (T, error) {
	m.mu.RLock()
	it, exists := m.data[key]
	m.mu.RUnlock()

	var zero T
	if !exists {
		return zero, kvstore.ErrNotFound
	}

	// Check TTL
	if it.expiresAt != nil && time.Now().After(*it.expiresAt) {
		m.Delete(ctx, key) // Clean up expired item
		return zero, kvstore.ErrNotFound
	}

	return it.value, nil
}

func (m *InMemoryKV[T]) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *InMemoryKV[T]) Exists(ctx context.Context, key string) (bool, error) {
	_, err := m.Get(ctx, key)
	return err == nil, nil
}
