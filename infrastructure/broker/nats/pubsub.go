package nats

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/AniFapper/go-common-ocherk/contracts/marshaller"
	"github.com/AniFapper/go-common-ocherk/contracts/pubsub"
	natsio "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// NatsPubSub provides a NATS-based implementation of the contracts.PubSub interface.
type NatsPubSub[T any] struct {
	nc    *natsio.Conn
	codec marshaller.Marshaller
}

// NewNatsPubSub returns a new instance of NatsPubSub.
func NewNatsPubSub[T any](nc *natsio.Conn, codec marshaller.Marshaller) *NatsPubSub[T] {
	return &NatsPubSub[T]{
		nc:    nc,
		codec: codec,
	}
}

// Publish encodes the message using the configured codec and sends it to NATS.
// It also injects the distributed trace context into the NATS message headers.
func (p *NatsPubSub[T]) Publish(ctx context.Context, topic string, message T) error {
	data, err := p.codec.Marshal(message)
	if err != nil {
		return fmt.Errorf("nats publish: encoding failed: %w", err)
	}

	msg := &natsio.Msg{
		Subject: topic,
		Data:    data,
		Header:  make(natsio.Header),
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))

	if err := p.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("nats publish: %w", err)
	}

	return nil
}

// Subscribe listens to a topic, unpacks the trace context, decodes incoming data,
// and passes it to the handler in an isolated goroutine to prevent Slow Consumers.
func (p *NatsPubSub[T]) Subscribe(ctx context.Context, topic string, handler pubsub.Handler[T], opts ...pubsub.SubscribeOption) (pubsub.Unsubscriber, error) {
	options := &pubsub.SubscribeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.MaxWorkers <= 0 {
		options.MaxWorkers = 1
	}

	// Буфер канала: сглаживает микро-спайки.
	// Делаем его с запасом относительно воркеров (например, x10).
	queueSize := options.MaxWorkers * 10
	msgQueue := make(chan *natsio.Msg, queueSize)
	var wg sync.WaitGroup

	// 1. ЗАПУСК ВОРКЕРОВ (Worker Pool)
	for i := 0; i < options.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Воркер читает сообщения из канала, пока он не будет закрыт
			for msg := range msgQueue {
				// Защита от потенциальной паники при работе с заголовками NATS
				if msg.Header == nil {
					msg.Header = make(natsio.Header)
				}

				// Извлекаем контекст трассировки для распределенного логгирования
				traceCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(msg.Header))

				// Изолируем обработку каждого сообщения, чтобы паника не уронила воркер
				func() {
					defer func() {
						if r := recover(); r != nil {
							slog.ErrorContext(traceCtx, "panic in nats handler", "panic", r, "topic", topic)
						}
					}()

					var payload T
					if err := p.codec.Unmarshal(msg.Data, &payload); err != nil {
						slog.ErrorContext(traceCtx, "nats unmarshal failed", "error", err, "topic", topic)
						return
					}

					if err := handler(traceCtx, payload); err != nil {
						slog.ErrorContext(traceCtx, "nats handler failed", "error", err, "topic", topic)
					}
				}()
			}
		}()
	}

	// 2. НЕБЛОКИРУЮЩИЙ КОЛЛБЭК ДЛЯ NATS
	cb := func(msg *natsio.Msg) {
		select {
		case msgQueue <- msg:
			// Успех: сообщение помещено в очередь на обработку
		default:
			// БЭКПРЕШЕР: Очередь переполнена. Воркеры не справляются.
			// Дропаем сообщение, спасая NATS соединение от ошибки Slow Consumer.
			slog.Warn("system overloaded, dropping incoming NATS Core message", "topic", topic)
		}
	}

	// 3. ПОДПИСКА
	var sub *natsio.Subscription
	var err error

	if options.QueueGroup != "" {
		sub, err = p.nc.QueueSubscribe(topic, options.QueueGroup, cb)
	} else {
		sub, err = p.nc.Subscribe(topic, cb)
	}

	if err != nil {
		// Если подписка не удалась, корректно гасим запущенные горутины
		close(msgQueue)
		wg.Wait()
		return nil, fmt.Errorf("nats subscribe failed: %w", err)
	}

	// 4. УМНЫЙ UNSUBSCRIBER (Graceful Shutdown)
	unsub := func() error {
		// Отключаемся от NATS: сервер перестает слать нам новые сообщения
		drainErr := sub.Drain()

		// Закрываем очередь. Воркеры обработают то, что уже в канале, и завершатся.
		close(msgQueue)

		waitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			slog.Info("nats subscription closed gracefully", "topic", topic)
			return drainErr
		case <-waitCtx.Done():
			return fmt.Errorf("nats unsubscribe timeout: workers didn't finish in time for topic %s", topic)
		}
	}

	return unsub, nil
}
