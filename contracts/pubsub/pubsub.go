package pubsub

import (
	"context"
)

// Handler defines a function type for processing incoming typed messages.
type Handler[T any] func(ctx context.Context, message T) error

// Unsubscriber defines a function type to stop a subscription and release resources.
type Unsubscriber func() error

// SubscribeOptions holds configuration for a specific subscription.
type SubscribeOptions struct {
	// QueueGroup, if provided, ensures that only one member of the group
	// receives each message (load balancing/competing consumers).
	QueueGroup string
}

// SubscribeOption defines a functional option for configuring a subscription.
type SubscribeOption func(*SubscribeOptions)

// WithQueueGroup returns a SubscribeOption that sets a queue group name.
func WithQueueGroup(name string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.QueueGroup = name
	}
}

// PubSub defines a generic, type-safe interface for asynchronous message exchange.
//
// Topic Wildcards Support:
// This interface supports hierarchical topics separated by dots (e.g., "orders.us.created").
// Depending on the implementation (NATS, RabbitMQ), you can use wildcards:
//
//   - "*" (Asterisk): Matches exactly one token.
//     Example: "orders.*.created" matches "orders.us.created" and "orders.eu.created".
//
//   - ">" (Full Wildcard/Greater-than): Matches one or more tokens at the end of a subject.
//     Example: "orders.>" matches "orders.us.created", "orders.us.shipped", etc.
//
// Usage Example:
//
//	func StartAuditWorker(ps PubSub[AuditLog]) {
//	    handler := func(ctx context.Context, log AuditLog) error {
//	        fmt.Printf("Audit: %s action by %s\n", log.Action, log.UserID)
//	        return nil
//	    }
//
//	    // Listen to ALL user-related events using a wildcard
//	    unsub, _ := ps.Subscribe(context.Background(), "auth.user.>", handler)
//	    defer unsub()
//	}
type PubSub[T any] interface {
	// Publish sends a typed message to the specified topic.
	Publish(ctx context.Context, topic string, message T) error

	// Subscribe creates a subscription to a topic.
	// Topics can include wildcards like "system.*" or "logs.>".
	Subscribe(ctx context.Context, topic string, handler Handler[T], opts ...SubscribeOption) (Unsubscriber, error)
}
