package broker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AniFapper/go-common-ocherk/contracts/marshaller"
	"github.com/AniFapper/go-common-ocherk/contracts/pubsub"
	"github.com/nats-io/nats.go"
)

// NatsPubSub provides a NATS-based implementation of the contracts.PubSub interface.
// It abstracts the messaging logic and delegates data serialization to a provided Marshaller.
//
// Usage Example:
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//	jsonCodec := codec.JSONCodec{} // Your Marshaller implementation
//
//	// 1. Initialize for a specific message type
//	ps := broker.NewNatsPubSub[UserEvent](nc, jsonCodec)
//
//	// 2. Publish a message
//	ps.Publish(ctx, "user.created", UserEvent{ID: "123", Name: "Ivan"})
//
//	// 3. Simple Subscribe (Broadcast - every instance gets the message)
//	unsub, _ := ps.Subscribe(ctx, "user.>", func(ctx context.Context, msg UserEvent) error {
//	    fmt.Printf("Received: %s\n", msg.Name)
//	    return nil
//	})
//
//	// 4. Queue Group Subscribe (Load Balancing - only one instance in group gets the message)
//	ps.Subscribe(ctx, "user.created", handler, contracts.WithQueueGroup("auth-service"))
type NatsPubSub[T any] struct {
	nc    *nats.Conn
	codec marshaller.Marshaller
}

// NewNatsPubSub returns a new instance of NatsPubSub.
// It requires a NATS connection and a Marshaller (e.g., JSON, Protobuf) to handle type conversion.
func NewNatsPubSub[T any](nc *nats.Conn, codec marshaller.Marshaller) *NatsPubSub[T] {
	return &NatsPubSub[T]{
		nc:    nc,
		codec: codec,
	}
}

// Publish encodes the message using the configured codec and sends the raw bytes to NATS.
// Returns an error if encoding fails or if the NATS server is unreachable.
func (p *NatsPubSub[T]) Publish(ctx context.Context, topic string, message T) error {
	data, err := p.codec.Marshal(message)
	if err != nil {
		return fmt.Errorf("nats publish: encoding failed: %w", err)
	}

	if err := p.nc.Publish(topic, data); err != nil {
		return fmt.Errorf("nats publish: %w", err)
	}

	return nil
}

// Subscribe listens to a topic and decodes incoming data before passing it to the handler.
// It supports functional options such as QueueGroups for distributed load balancing.
func (p *NatsPubSub[T]) Subscribe(ctx context.Context, topic string, handler pubsub.Handler[T], opts ...pubsub.SubscribeOption) (pubsub.Unsubscriber, error) {
	// Initialize default options and apply functional modifiers
	options := &pubsub.SubscribeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Internal callback to bridge NATS raw messages with typed handlers
	cb := func(msg *nats.Msg) {
		var payload T

		// Use the injected codec to unmarshal the raw byte slice back into type T
		if err := p.codec.Unmarshal(msg.Data, &payload); err != nil {
			slog.Error("nats subscribe: decoding failed", "error", err, "topic", topic)
			return
		}

		// NATS subscriptions run in their own goroutines.
		// We use a fresh context.Background() as the original Subscribe context might have expired.
		if err := handler(context.Background(), payload); err != nil {
			slog.Error("nats subscribe: handler failed", "error", err, "topic", topic)
		}
	}

	var sub *nats.Subscription
	var err error

	// If a QueueGroup is specified, NATS ensures only one instance in the group receives each message.
	if options.QueueGroup != "" {
		sub, err = p.nc.QueueSubscribe(topic, options.QueueGroup, cb)
	} else {
		// Default fan-out (broadcast) behavior
		sub, err = p.nc.Subscribe(topic, cb)
	}

	if err != nil {
		return nil, fmt.Errorf("nats subscribe failed: %w", err)
	}

	// Returns an Unsubscriber closure to properly close the subscription when no longer needed.
	unsub := func() error {
		return sub.Unsubscribe()
	}

	return unsub, nil
}
