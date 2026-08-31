package rabbitmq

import (
	"context"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Delivery is the consumer message type; a thin alias on the AMQP delivery so
// pool users don't import amqp directly.
type Delivery = amqp.Delivery

// Handler processes one message. It must call d.Ack / d.Nack / d.Reject itself
// so the pool never auto-acks — manual acknowledgement is the whole point.
type Handler func(ctx context.Context, d amqp.Delivery) error

// WorkerPool consumes a single queue with N competing consumers. Every message
// is delivered with manual acknowledgement; the handler decides what to do.
type WorkerPool struct {
	broker   *Broker
	topology Topology
	queue    string
	workers  int
	handler  Handler

	mu   sync.Mutex
	stop context.CancelFunc
}

func NewWorkerPool(broker *Broker, top Topology, queue string, workers int, handler Handler) *WorkerPool {
	return &WorkerPool{
		broker:   broker,
		topology: top,
		queue:    queue,
		workers:  workers,
		handler:  handler,
	}
}

// Run supervises consumption, surviving broker drops. On a connection loss the
// consumer channel is invalidated, so the supervisor reconnects, re-declares
// topology, and starts a fresh consumer channel. Returns when ctx is done.
func (p *WorkerPool) Run(ctx context.Context) {
	p.mu.Lock()
	childCtx, cancel := context.WithCancel(ctx)
	p.stop = cancel
	p.mu.Unlock()

	for childCtx.Err() == nil {
		err := p.consumeLoop(childCtx)
		if childCtx.Err() != nil {
			return
		}
		log.Printf("worker pool %q ended (%v); reconnecting", p.queue, err)
		if rerr := p.broker.ReconnectIfNeeded(childCtx); rerr != nil {
			if childCtx.Err() != nil {
				return
			}
			log.Printf("worker pool %q reconnect failed: %v", p.queue, rerr)
			return
		}
	}
}

// Stop signals the supervisor to shut down cleanly.
func (p *WorkerPool) Stop() {
	p.mu.Lock()
	if p.stop != nil {
		p.stop()
	}
	p.mu.Unlock()
}

// consumeLoop runs consumers on one channel until it dies. On return the
// supervisor decides whether to retry.
func (p *WorkerPool) consumeLoop(ctx context.Context) error {
	ch, err := p.broker.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Re-declare so the queue/bindings exist if this is a fresh broker.
	if err := p.topology.Declare(ctx, ch); err != nil {
		return err
	}

	// Fair dispatch: one message to each worker before the next round.
	if err := ch.Qos(p.workers, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(p.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Printf("worker pool consuming %q with %d workers", p.queue, p.workers)

	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	// Dispatch deliveries to worker goroutines.
	workerCtx, workerCancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					p.handle(workerCtx, id, d)
				}
			}
		}(i)
	}

	// Block until channel dies or we're told to stop.
	var done error
	select {
	case <-ctx.Done():
		done = ctx.Err()
	case err, ok := <-closed:
		if ok {
			done = err
		}
	}

	workerCancel()
	wg.Wait()
	return done
}

func (p *WorkerPool) handle(ctx context.Context, id int, d amqp.Delivery) {
	log.Printf("[%s worker %d] received         %q (id=%s)", p.queue, id, d.Body, d.MessageId)
	if err := p.handler(ctx, d); err != nil {
		log.Printf("[%s worker %d] handler failed:  %v (nacked, redelivery=%v)",
			p.queue, id, err, d.Redelivered)
		_ = d.Nack(false, true)
		return
	}
	if err := d.Ack(false); err != nil {
		log.Printf("[%s worker %d] ack failed:      %v", p.queue, id, err)
		return
	}
	log.Printf("[%s worker %d] completed & acked %q (id=%s)", p.queue, id, d.Body, d.MessageId)
}
