package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher sends messages to an exchange with publisher confirms enabled.
// Confirms mean we know when the broker has actually taken ownership of a
// message — essential later when the tick loop must not lose tracked work.
//
// Messages are Transient (non-persistent) by design: durable-queue Persistent
// delivery forces RabbitMQ to fsync every message before confirming, which on
// a dev/Docker broker makes ack latency multi-millisecond and caps a pool's
// real throughput far below the DB model's assumptions. Transient removes that
// disk-bound ceiling so publish→ack latency means queue backlog, not fsync.
// TRADE-OFF: in-flight messages no longer survive a broker restart — revisit
// if a later incident (e.g. Incident 2's worker-crash recovery) needs message
// durability back.
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
		ContentType: "application/json",
		MessageId:   idFromBody(body),
		Body:        body,
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

// PublishBatch publishes a batch of messages to exchange with routingKey and
// waits until every one of them is confirmed. RabbitMQ may ack a whole range
// with a single confirmation carrying multiple=true, so the drain tracks the
// highest confirmed delivery tag rather than counting confirmations one-to-one.
// The batch completes once that tag is >= the last sequence number in the batch.
//
// Batching the confirm wait keeps the tick loop from round-tripping to the
// broker once per message at stampede scale (per-message confirm-and-wait would
// stall a 10k msgs/sec run).
func (p *Publisher) PublishBatch(ctx context.Context, exchange, routingKey string, bodies [][]byte) error {
	if len(bodies) == 0 {
		return nil
	}
	p.mu.Lock()
	if err := p.ensureChannelLocked(); err != nil {
		p.mu.Unlock()
		return err
	}
	firstSeq := p.channel.GetNextPublishSeqNo()
	for _, body := range bodies {
		pub := amqp.Publishing{
			ContentType: "application/json",
			MessageId:   idFromBody(body),
			Body:        body,
		}
		if err := p.channel.PublishWithContext(ctx, exchange, routingKey, false, false, pub); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("publish batch: %w", err)
		}
	}
	lastSeq := firstSeq + uint64(len(bodies)) - 1
	p.mu.Unlock()

	// Drain confirms until the highest confirmed tag covers the whole batch.
	highest := uint64(0)
	for highest < lastSeq {
		select {
		case conf, ok := <-p.confirms:
			if !ok {
				return fmt.Errorf("publish batch: confirm channel closed")
			}
			if !conf.Ack {
				return fmt.Errorf("publish batch: nack (delivery %d)", conf.DeliveryTag)
			}
			if conf.DeliveryTag > highest {
				highest = conf.DeliveryTag
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
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

type bodyID struct {
	ID string `json:"id"`
}

// idFromBody extracts the "id" field from a JSON body so it can be set as the
// AMQP MessageId, making traceability visible in the management UI and consumer
// logs. On failure the empty string is returned; AMQP then carries no
// MessageId.
func idFromBody(body []byte) string {
	var b bodyID
	if err := json.Unmarshal(body, &b); err == nil && b.ID != "" {
		return b.ID
	}
	return ""
}
