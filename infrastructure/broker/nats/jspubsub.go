package nats

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/AniFapper/go-common-ocherk/contracts/marshaller"
	"github.com/AniFapper/go-common-ocherk/contracts/pubsub"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// JSPubSub implements a generic, type-safe message exchange using the NATS JetStream V2 API.
// It provides reliable At-Least-Once delivery guarantees, built-in OpenTelemetry tracing,
// and sophisticated consumer lifecycle management (graceful shutdown, backpressure, and poison pill detection).
type JSPubSub[T any] struct {
	js    jetstream.JetStream
	codec marshaller.Marshaller
}

// consumerState manages the concurrency and lifecycle of a JetStream consumer.
// It uses a WaitGroup to track active background workers and an RWMutex to safely
// prevent new workers from starting during a graceful shutdown sequence.
type consumerState struct {
	mu       sync.RWMutex
	stopping bool
	wg       sync.WaitGroup
}

// NewJSPubSub initializes a new JetStream publisher/subscriber instance.
// It requires an active NATS connection and a Marshaller (e.g., JSON or Protobuf)
// to handle payload serialization.
func NewJSPubSub[T any](nc *nats.Conn, codec marshaller.Marshaller) (*JSPubSub[T], error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create jetstream v2: %w", err)
	}
	return &JSPubSub[T]{js: js, codec: codec}, nil
}

// Publish serializes the strongly-typed message, injects the current OpenTelemetry
// trace context into the NATS headers, and publishes the message to the specified topic.
//
// It uses JetStream V2's PublishMsg, which respects the provided context.Context
// (e.g., timeouts and cancellations) while waiting for the server's Publish Acknowledgment.
func (p *JSPubSub[T]) Publish(ctx context.Context, topic string, message T) error {
	data, err := p.codec.Marshal(message)
	if err != nil {
		return fmt.Errorf("js v2 publish: encoding failed: %w", err)
	}

	msg := &nats.Msg{
		Subject: topic,
		Data:    data,
		Header:  make(nats.Header),
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))

	_, err = p.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("js v2 publish failed: %w", err)
	}

	return nil
}

// Subscribe starts an asynchronous message consumer for the given topic.
//
// Nuances & Protections:
//  1. Server-Side Backpressure: Uses MaxAckPending based on options.MaxWorkers. NATS will not
//     deliver more concurrent messages than the workers can handle, inherently protecting against OOM.
//  2. Split-Brain Cache Protection: If a QueueGroup is NOT provided, it acts as an Ephemeral consumer
//     (e.g., for Cache Invalidation) and uses DeliverNewPolicy. This prevents "Cache Stampedes" where
//     restarting a pod would download hours of historical cache invalidation events.
//  3. Graceful Shutdown: The returned Unsubscriber safely stops the NATS pull cycle and waits up to
//     15 seconds for all in-flight handlers to finish before returning.
//
// WARNING:
// The provided handler MUST respect `ctx.Done()`. The broker enforces `TaskTimeout` by
// canceling the context. If the handler makes blocking network/DB calls without passing
// this context, the goroutine will leak, and the worker slot will be permanently lost.
//
// Example Usage (Heavy Task with Load Balancing):
//
//	unsub, err := ps.Subscribe(
//	    ctx,
//	    "epub.generate",
//	    epubHandler,
//	    pubsub.WithQueueGroup("epub-workers"),    // Load balance across pods
//	    pubsub.WithMaxWorkers(4),                 // Max 4 concurrent tasks per pod
//	    pubsub.WithTaskTimeout(10 * time.Minute), // Allow up to 10 mins per task
//	    pubsub.WithMaxRetries(2),                 // Retry twice before Terminating
//	)
//
// Example Usage (Fast Broadcast / Cache Invalidation):
//
//	unsub, err := ps.Subscribe(
//	    ctx,
//	    "cache.invalidate",
//	    cacheHandler,
//	    // No QueueGroup -> Broadcast to all pods. DeliverNewPolicy is auto-applied.
//	    pubsub.WithMaxWorkers(50),
//	    pubsub.WithTaskTimeout(5 * time.Second),
//	    pubsub.WithMaxRetries(0), // Fire and forget
//	)
func (p *JSPubSub[T]) Subscribe(ctx context.Context, topic string, handler pubsub.Handler[T], opts ...pubsub.SubscribeOption) (pubsub.Unsubscriber, error) {
	options := &pubsub.SubscribeOptions{
		MaxWorkers:  1,
		TaskTimeout: 30 * time.Second,
		MaxRetries:  3,
	}
	for _, opt := range opts {
		opt(options)
	}

	streamName, err := p.js.StreamNameBySubject(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to find stream: %w", err)
	}

	stream, err := p.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream: %w", err)
	}

	cfg := jetstream.ConsumerConfig{
		FilterSubject: topic,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: options.MaxWorkers, // Native JetStream Backpressure
		MaxDeliver:    options.MaxRetries + 1,
	}

	if options.QueueGroup != "" {
		cfg.Durable = options.QueueGroup
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	} else {
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	state := &consumerState{}

	consumeContext, err := cons.Consume(func(msg jetstream.Msg) {
		state.mu.RLock()
		if state.stopping {
			state.mu.RUnlock()
			// Return message to NATS immediately if we are shutting down
			_ = msg.Nak()
			return
		}
		state.wg.Add(1)
		state.mu.RUnlock()

		go func() {
			defer state.wg.Done()
			p.processMessage(msg, topic, options, handler)
		}()
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start consume: %w", err)
	}

	return func() error {
		// Stop requesting new messages
		consumeContext.Stop()

		// Lock state to prevent new handlers from registering
		state.mu.Lock()
		state.stopping = true
		state.mu.Unlock()

		waitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			state.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			return nil
		case <-waitCtx.Done():
			return fmt.Errorf("js v2 unsubscribe timeout: workers stuck or ignoring context cancellation")
		}
	}, nil
}

// processMessage orchestrates a single message's lifecycle, including OpenTelemetry tracing,
// poison pill termination, background heartbeats, and synchronous handler execution.
func (p *JSPubSub[T]) processMessage(msg jetstream.Msg, topic string, options *pubsub.SubscribeOptions, handler pubsub.Handler[T]) {
	tracer := otel.Tracer("nats-pubsub")

	headers := msg.Headers()
	if headers == nil {
		headers = make(nats.Header)
	}

	parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(headers))
	traceCtx, span := tracer.Start(
		parentCtx,
		fmt.Sprintf("consume %s", topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	// Evaluate Poison Pill state
	meta, err := msg.Metadata()
	isLastAttempt := err == nil && meta.NumDelivered >= uint64(options.MaxRetries+1)

	var payload T
	if err := p.codec.Unmarshal(msg.Data(), &payload); err != nil {
		slog.ErrorContext(traceCtx, "js decoding failed, terminating message", "error", err)
		_ = msg.Term()
		return
	}

	taskCtx, cancel := context.WithTimeout(traceCtx, options.TaskTimeout)
	defer cancel()

	// Start Heartbeat to keep the message leased during long operations
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ticker.C:
				_ = msg.InProgress()
			}
		}
	}()

	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(taskCtx, "panic in js handler", "panic", r)
				handlerErr = fmt.Errorf("panic: %v", r)
			}
		}()
		handlerErr = handler(taskCtx, payload)
	}()

	close(heartbeatDone)

	// State machine for Ack/Nak/Term
	if handlerErr != nil {
		if isLastAttempt {
			slog.ErrorContext(taskCtx, "poison pill: final attempt failed, terminating", "error", handlerErr, "topic", topic)
			_ = msg.Term() // Unrecoverable after max retries, drop the message
		} else {
			slog.ErrorContext(taskCtx, "js handler failed, scheduling retry", "error", handlerErr)
			_ = msg.NakWithDelay(2 * time.Second) // Delay retry to prevent rapid failure loops
		}
	} else {
		_ = msg.Ack()
	}
}
