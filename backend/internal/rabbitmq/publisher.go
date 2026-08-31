package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher sends messages to an exchange with publisher confirms enabled.
// Confirms mean we know when the broker has actually taken ownership of a
// message — essential later when the tick loop must not lose tracked work.
type Publisher struct {
	broker *Broker

	mu       sync.Mutex
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
}

// NewPublisher opens a dedicated confirms-enabled channel on the broker.
func NewPublisher(broker *Broker) (*Publisher, error) {
	ch, err := broker.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1024))
	return &Publisher{broker: broker, channel: ch, confirms: confirms}, nil
}

// Publish publishes a message to exchange with routingKey and waits for its
// broker confirmation. Data-race-safe; reopens its channel if it was
// invalidated by a connection drop.
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	p.mu.Lock()
	if err := p.ensureChannelLocked(); err != nil {
		p.mu.Unlock()
		return err
	}
	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}
	seq := p.channel.GetNextPublishSeqNo()
	err := p.channel.PublishWithContext(ctx, exchange, routingKey, false, false, pub)
	p.mu.Unlock()
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	select {
	case conf, ok := <-p.confirms:
		if !ok {
			return fmt.Errorf("publish confirm channel closed")
		}
		if conf.DeliveryTag == seq && conf.Ack {
			return nil
		}
		return fmt.Errorf("publish nack (delivery %d)", conf.DeliveryTag)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ensureChannelLocked replaces a dead channel. Caller must hold p.mu.
func (p *Publisher) ensureChannelLocked() error {
	if p.channel.IsClosed() {
		ch, err := p.broker.Channel()
		if err != nil {
			return err
		}
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return fmt.Errorf("enable publisher confirms: %w", err)
		}
		p.channel = ch
		p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1024))
		log.Println("publisher channel re-established")
	}
	return nil
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
	}
}
