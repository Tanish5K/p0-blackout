package rabbitmq

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// debugLogging gates per-message "received" / "completed & acked" logs.
// Set BLACKOUT_DEBUG=1 to enable; failures, reconnects, and pool lifecycle
// are always logged regardless of this flag.
var debugLogging = os.Getenv("BLACKOUT_DEBUG") == "1"

// Delivery is the consumer message type; a thin alias on the AMQP delivery so
// pool users don't import amqp directly.
type Delivery = amqp.Delivery

// Handler processes one message. It receives the queue name (so a shared
// handler can tailor behaviour per queue) and the delivery; it must call
// d.Ack / d.Nack / d.Reject itself — manual acknowledgement is the whole point.
type Handler func(ctx context.Context, queue string, d amqp.Delivery) error

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

	// Failure-summary state: failures are always surfaced, but as a rate-limited
	// per-second roll-up rather than one line per dropped message. Per-message
	// detail stays gated behind BLACKOUT_DEBUG=1 (the established logging split);
	// this roll-up exists so a failure storm is visible WITHOUT flipping the
	// flag, not instead of it. Reconnects / pool lifecycle keep their own logs.
	failMu     sync.Mutex
	failTotal  uint64
	failWindow uint64
	failLogged time.Time
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
	//
	// The prefetch is generous (not "= workers") so the channel pipelines: a
	// small prefetch makes the whole pool roughly single-threaded, bound by the
	// ack round-trip instead of the worker's DB work, which silently caps
	// throughput at ~workers/ack-RTT. With a large prefetch, aggregate drain
	// scales with worker count and the DB model's latency curve stays honest.
	if err := ch.Qos(500, 0, false); err != nil {
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

// handle runs one delivery through the pool's handler.
func (p *WorkerPool) handle(ctx context.Context, id int, d amqp.Delivery) {
	if debugLogging {
		log.Printf("[%s worker %d] received         %q (id=%s)", p.queue, id, d.Body, d.MessageId)
	}
	if err := p.handler(ctx, p.queue, d); err != nil {
		// Failures are always surfaced, but as a rate-limited per-second
		// roll-up so a storm stays visible without flooding the console;
		// per-message detail is gated behind BLACKOUT_DEBUG like the rest of
		// the routine chatter. This is a SUMMARY, deliberately not a
		// suppression: it must not be reused to mute a genuinely interesting
		// failure (e.g. Incident 3's poison-message storm).
		if debugLogging {
			log.Printf("[%s worker %d] handler failed:  %v (nacked, redelivery=%v)",
				p.queue, id, err, d.Redelivered)
		} else {
			p.rollFailure()
		}
		// Dropped, not requeued: a simulated failure counts exactly once. The
		// Nack(false, true) that used to be here requeued the message, so a
		// persistent failure looped forever, re-counted both failed AND (on a
		// later successful pass) acked, and flooded the log. requeue=false
		// keeps the loss honest and the counters single-count.
		_ = d.Nack(false, false)
		return
	}
	if err := d.Ack(false); err != nil {
		log.Printf("[%s worker %d] ack failed:      %v", p.queue, id, err)
		return
	}
	if debugLogging {
		log.Printf("[%s worker %d] completed & acked %q (id=%s)", p.queue, id, d.Body, d.MessageId)
	}
}

// rollFailure accounts for one failed handler return and, at most once per
// second, prints a per-queue summary of how many failures landed.
func (p *WorkerPool) rollFailure() {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	p.failTotal++
	p.failWindow++
	if time.Since(p.failLogged) < time.Second {
		return
	}
	log.Printf("[%s worker] %d failures in the last 1s (total %d): simulated worker failure, message dropped",
		p.queue, p.failWindow, p.failTotal)
	p.failWindow = 0
	p.failLogged = time.Now()
}
