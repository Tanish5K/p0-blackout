package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Broker owns the single AMQP connection to RabbitMQ and hands out channels.
// Channels are cheap relative to connections, but not free — one per consumer
// pool / publisher.
//
// A drop of the underlying connection invalidates every channel opened from
// it, so on reconnect callers must reopen channels and re-declare topology.
type Broker struct {
	url string

	mu   sync.Mutex
	conn *amqp.Connection
}

func NewBroker(url string) *Broker {
	return &Broker{url: url}
}

// Connect establishes the connection and blocks until the broker is ready or
// the context is done. It keeps retrying so the backend can come up before
// RabbitMQ (e.g. right after docker compose up).
func (b *Broker) Connect() error {
	for {
		if b.url == "" {
			return fmt.Errorf("empty AMQP URL")
		}
		conn, err := amqp.Dial(b.url)
		if err == nil {
			b.mu.Lock()
			if b.conn != nil {
				_ = b.conn.Close()
			}
			b.conn = conn
			b.mu.Unlock()
			log.Printf("connected to rabbitmq %s", b.url)
			return nil
		}
		log.Printf("rabbitmq not ready (%v); retrying in 2s", err)
		time.Sleep(2 * time.Second)
	}
}

// ReconnectIfNeeded re-establishes the connection only if it has dropped.
// Returns immediately (no-op) when the current connection is still usable.
// Retries until success or the context is done.
func (b *Broker) ReconnectIfNeeded(ctx context.Context) error {
	for {
		b.mu.Lock()
		up := b.conn != nil && !b.conn.IsClosed()
		if up {
			b.mu.Unlock()
			return nil
		}
		if b.conn != nil {
			_ = b.conn.Close()
			b.conn = nil
		}
		b.mu.Unlock()

		conn, err := amqp.Dial(b.url)
		if err == nil {
			b.mu.Lock()
			b.conn = conn
			b.mu.Unlock()
			log.Printf("reconnected to rabbitmq %s", b.url)
			return nil
		}
		log.Printf("rabbitmq reconnect failed (%v); retrying in 2s", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Channel opens a fresh channel on the shared connection.
func (b *Broker) Channel() (*amqp.Channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == nil {
		return nil, fmt.Errorf("not connected")
	}
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return ch, nil
}

// NotifyClose surfaces the underlying connection's closure so callers can
// react (reopen channels, re-declare topology, restart consumers).
func (b *Broker) NotifyClose() <-chan *amqp.Error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == nil {
		ch := make(chan *amqp.Error, 1)
		return ch
	}
	return b.conn.NotifyClose(make(chan *amqp.Error, 1))
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}
}
