package rabbitmq

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Exchange types.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// Exchange is a single declarative exchange definition.
type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Args       amqp.Table
}

// Queue is a single declarative queue definition.
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Args       amqp.Table
}

// Binding connects a queue to an exchange via a routing key.
type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Args       amqp.Table
}

// Topology is the complete, data-driven set of declarations for a scenario.
// Regenerating per incident means swapping out this slice rather than
// scattering ExchangeDeclare/Bind calls through imperative code.
type Topology struct {
	Name      string
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}

// LumenTopology is retained as a convenience for tests/tools: the wired
// topology of the startup registry (§5.1). Prefer Registry.WiredTopology() so
// stub services stay excluded.
func LumenTopology() Topology {
	return AllServices().WiredTopology()
}

// Declare creates every exchange, queue, and binding in the topology on the
// given channel. All declarations are idempotent (declare-if-exists), so this
// is safe to call on reconnect and across restarts.
func (t Topology) Declare(ctx context.Context, ch *amqp.Channel) error {
	for _, ex := range t.Exchanges {
		if err := ch.ExchangeDeclare(
			ex.Name, ex.Type, ex.Durable, ex.AutoDelete, false, false, ex.Args,
		); err != nil {
			return fmt.Errorf("declare exchange %q: %w", ex.Name, err)
		}
	}
	for _, q := range t.Queues {
		if _, err := ch.QueueDeclare(
			q.Name, q.Durable, q.AutoDelete, q.Exclusive, false, q.Args,
		); err != nil {
			return fmt.Errorf("declare queue %q: %w", q.Name, err)
		}
	}
	for _, b := range t.Bindings {
		if err := ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, false, b.Args); err != nil {
			return fmt.Errorf("bind queue %q to %q via %q: %w", b.Queue, b.Exchange, b.RoutingKey, err)
		}
	}
	log.Printf("topology %q declared (%d exchanges, %d queues, %d bindings)",
		t.Name, len(t.Exchanges), len(t.Queues), len(t.Bindings))
	return nil
}
