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

type InMemoryKV[T any] struct {
	mu        sync.RWMutex
	data      map[string]item[T]
	stop      chan struct{}
	closeOnce sync.Once // Защита от panic: close of closed channel
}

func NewInMemoryKV[T any](cleanupInterval time.Duration) *InMemoryKV[T] {
	m := &InMemoryKV[T]{
		data: make(map[string]item[T]),
		stop: make(chan struct{}),
	}
	go m.startSweeper(cleanupInterval)
	return m
}

func (m *InMemoryKV[T]) Close() error {
	close(m.stop)
	return nil
}

func (m *InMemoryKV[T]) startSweeper(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.evictExpired()
		case <-m.stop:
			return
		}
	}
}

func (m *InMemoryKV[T]) evictExpired() {
	now := time.Now()
	const batchSize = 100

	expiredKeys := make([]string, 0, batchSize)

	m.mu.RLock()
	for k, v := range m.data {
		if v.expiresAt != nil && now.After(*v.expiresAt) {
			expiredKeys = append(expiredKeys, k)
		}
		if len(expiredKeys) >= batchSize {
			break
		}
	}
	m.mu.RUnlock()

	if len(expiredKeys) == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range expiredKeys {

		if it, ok := m.data[k]; ok && it.expiresAt != nil && now.After(*it.expiresAt) {
			delete(m.data, k)
		}
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

	now := time.Now()
	if it.expiresAt != nil && now.After(*it.expiresAt) {
		m.mu.Lock()
		if current, ok := m.data[key]; ok && current.expiresAt != nil && now.After(*current.expiresAt) {
			delete(m.data, key)
		}
		m.mu.Unlock()
		return zero, kvstore.ErrNotFound
	}

	return it.value, nil
}

func (m *InMemoryKV[T]) Delete(ctx context.Context, key string, opts ...kvstore.DeleteOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *InMemoryKV[T]) Exists(ctx context.Context, key string) (bool, error) {
	_, err := m.Get(ctx, key)
	return err == nil, nil
}
