package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"blackout/pkg/events"
)

// debugLogging gates per-message "received" / "completed & acked" logs.
// Set BLACKOUT_DEBUG=1 to enable; failures, reconnects, crashes, and pool
// lifecycle are always logged regardless of this flag.
var debugLogging = os.Getenv("BLACKOUT_DEBUG") == "1"

// Delivery is the consumer message type; a thin alias on the AMQP delivery so
// pool users don't import amqp directly.
type Delivery = amqp.Delivery

// AckPolicy is a queue's acknowledgement mode. Manual = the handler acks each
// message (crash → channel close requeues the unacked batch → redelivery).
// Auto = the broker removes the message at delivery (crash → the message is
// gone; absence, not a redelivery, is the evidence).
type AckPolicy string

const (
	AckManual AckPolicy = "manual"
	AckAuto   AckPolicy = "auto"
)

// Handler processes one message. It receives the queue name (so a shared
// handler can tailor behaviour per queue) and the delivery; it must call
// d.Ack / d.Nack / d.Reject itself in manual mode — manual acknowledgement is
// the whole point. In auto mode there is nothing to ack: the broker removed
// the message at delivery.
type Handler func(ctx context.Context, queue string, d amqp.Delivery) error

// WorkerReport is one consumer's live view for the inspector: the worker id
// and the message currently being processed ("" = idle).
type WorkerReport struct {
	ID  int    `json:"id"`
	Msg string `json:"msg"`
}

// errSessionRecycled is returned by consumeLoop when the session was ended by a
// worker crash (deliberate kill), so the supervisor reconnects WITHOUT treating
// it as a broker failure. Sequence: Kill cancels the worker's context (the
// in-flight handler returns with no ack) and breaks the session; channel close
// makes RabbitMQ requeue every unacked message; the next session redelivers
// them (Redelivered=true).
var errSessionRecycled = errors.New("consumer session recycled after worker crash")

// WorkerPool consumes a single queue with N competing consumers. Every message
// is delivered per the pool's ack policy; in manual mode the handler decides
// what to do (this pool owns the Ack/Nack policy split — Incident 2).
type WorkerPool struct {
	broker    *Broker
	topology  Topology
	queue     string
	workers   int
	handler   Handler
	autoAck   bool
	restartAt atomic.Int64 // unix-nanos when a crashed worker slot respawns (0 = none)

	mu     sync.Mutex
	stop   context.CancelFunc
	emit   func(events.Event)
	session context.CancelFunc

	// Per-session worker bookkeeping: live worker contexts and the message each
	// is currently processing, plus who was deliberately killed this session.
	wm         sync.Mutex
	workersNow map[int]context.CancelFunc
	processing map[int]string
	killed     map[int]bool

	// Failure-summary state: failures are always surfaced, but as a rate-limited
	// per-second roll-up rather than one line per dropped message. Per-message
	// detail stays gated behind BLACKOUT_DEBUG=1 (the established logging split);
	// this roll-up exists so a failure storm is visible WITHOUT flipping the
	// flag, not instead of it. Reconnects / pool lifecycle keep their own logs.
	failMu     sync.Mutex
	failTotal  uint64
	failWindow uint64
	failLogged time.Time

	// crashAuto counts messages lost to a hard worker death in AUTO mode (the
	// delivery was already removed, so there is no redelivery). The pool
	// manager drains this into the simulation's failure metrics each tick.
	crashAuto atomic.Int64

	// lastRecycleLog rate-limits the "session recycled" reconnect line.
	recycleMu  sync.Mutex
	lastRecycle time.Time
}

// NewWorkerPool builds a pool. autoAck selects the acknowledgement policy
// (false = manual, true = auto). A nil emitter is fine — the pool simply emits
// no events.
func NewWorkerPool(broker *Broker, top Topology, queue string, workers int, handler Handler, autoAck ...bool) *WorkerPool {
	ack := len(autoAck) > 0 && autoAck[0]
	return &WorkerPool{
		broker:      broker,
		topology:    top,
		queue:       queue,
		workers:     workers,
		handler:     handler,
		autoAck:     ack,
		workersNow:  make(map[int]context.CancelFunc),
		processing:  make(map[int]string),
		killed:      make(map[int]bool),
	}
}

// SetEmitter installs the event-log hook (crash / redelivery events). Safe to
// call between runs; the simulation hands its per-run log.
func (p *WorkerPool) SetEmitter(emit func(events.Event)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.emit = emit
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
		if errors.Is(err, errSessionRecycled) {
			// A worker died and the channel closed to requeue its in-flight
			// batch. This is the Incident 2 crash churn: reconnect promptly,
			// but keep the log rate-limited so a storm stays legible.
			p.logRecycle()
		} else {
			log.Printf("worker pool %q ended (%v); reconnecting", p.queue, err)
		}
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

// consumeLoop runs consumers on one channel until it dies or the session is
// broken.
func (p *WorkerPool) consumeLoop(ctx context.Context) error {
	// Session context: workers derive from it, and breaking it (worker crash)
	// ends the session so the channel closes and RabbitMQ requeues unacked
	// messages.
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.session = sessionCancel
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.session = nil
		p.mu.Unlock()
	}()

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

	deliveries, err := ch.Consume(p.queue, "", p.autoAck, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Printf("worker pool consuming %q with %d workers (ack=%s)",
		p.queue, p.workerCount(), ackPolicyLabel(p.autoAck))

	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	// Dispatch deliveries to worker goroutines. Each worker gets its own
	// cancelable context so a targeted kill (Incident 2) can interrupt its
	// in-flight handler without touching the other workers' pipeline.
	var wg sync.WaitGroup
	n := p.workerCount()
	for i := 0; i < n; i++ {
		wctx, wcancel := context.WithCancel(sessionCtx)
		p.registerWorker(i, wcancel)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer p.unregisterWorker(id)
			for {
				select {
				case <-wctx.Done():
					return
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					p.handle(wctx, id, d)
				}
			}
		}(i)
	}

	// Block until the session is broken, the channel dies, or we're told to stop.
	var done error
	select {
	case <-sessionCtx.Done():
		if ctx.Err() == nil {
			done = errSessionRecycled // deliberate crash; reconnect, don't panic
		} else {
			done = ctx.Err()
		}
	case err, ok := <-closed:
		if ok {
			done = err
		}
	}

	sessionCancel()
	wg.Wait()
	return done
}

// workerCount returns the number of consumer goroutines this session should
// run: the configured count, minus one while a crashed slot is resting.
func (p *WorkerPool) workerCount() int {
	n := p.workers
	if n < 1 {
		n = 1
	}
	if p.restartAt.Load() != 0 && p.restartAt.Load() > time.Now().UnixNano() {
		if n > 1 {
			return n - 1
		}
	}
	return n
}

// registerWorker records a live worker's cancel func (map is reset per session).
func (p *WorkerPool) registerWorker(id int, cancel context.CancelFunc) {
	p.wm.Lock()
	p.workersNow[id] = cancel
	p.wm.Unlock()
}

func (p *WorkerPool) unregisterWorker(id int) {
	p.wm.Lock()
	delete(p.workersNow, id)
	delete(p.killed, id)
	p.wm.Unlock()
}

func (p *WorkerPool) setProcessing(id int, msg string) {
	p.wm.Lock()
	p.processing[id] = msg
	p.wm.Unlock()
}

func (p *WorkerPool) clearProcessing(id int) {
	p.wm.Lock()
	delete(p.processing, id)
	p.wm.Unlock()
}

func (p *WorkerPool) markKilled(id int) {
	p.wm.Lock()
	p.killed[id] = true
	p.wm.Unlock()
}

func (p *WorkerPool) wasKilled(id int) bool {
	p.wm.Lock()
	k := p.killed[id]
	p.wm.Unlock()
	return k
}

// Kill crashes the given worker: it cancels the worker's context (interrupting
// the in-flight handler, which returns without acking/nacking) and ends the
// session so the channel closes and RabbitMQ requeues the unacked batch. In
// auto mode the in-flight delivery was already removed; the manager records the
// loss via DrainCrashedAuto. Returns an error for unknown worker ids.
func (p *WorkerPool) Kill(id int) error {
	p.wm.Lock()
	cancel, ok := p.workersNow[id]
	p.wm.Unlock()
	if !ok {
		return fmt.Errorf("worker %d not running on %q", id, p.queue)
	}
	// Mark killed so the handler's early return knows to credit the crash (auto
	// loss) and the event log gets one worker_crashed line.
	p.markKilled(id)
	cancel()

	// Break the session → consumeLoop returns errSessionRecycled → the channel
	// closes and the supervisor reconnects. RabbitMQ requeues every unacked
	// message, which is the redelivery chain the player sees.
	p.mu.Lock()
	if p.session != nil {
		p.session()
	}
	p.mu.Unlock()
	return nil
}

// Restart delays the respawn of a crashed worker slot by restartHoldTicks-like
// wall time (the pool runs one fewer consumer until then). Call after Kill to
// model "worker slot offline ~30s".
func (p *WorkerPool) Restart(hold time.Duration) {
	p.restartAt.Store(time.Now().Add(hold).UnixNano())
}

// Report returns the pool's current workers and what each is processing.
func (p *WorkerPool) Report() []WorkerReport {
	p.wm.Lock()
	defer p.wm.Unlock()
	out := make([]WorkerReport, 0, len(p.workersNow))
	for id := range p.workersNow {
		out = append(out, WorkerReport{ID: id, Msg: p.processing[id]})
	}
	return out
}

// ProcessingMessage returns the message a specific worker is processing ("" if
// idle/unknown).
func (p *WorkerPool) ProcessingMessage(id int) string {
	p.wm.Lock()
	defer p.wm.Unlock()
	return p.processing[id]
}

// handle runs one delivery through the pool's handler.
func (p *WorkerPool) handle(ctx context.Context, id int, d amqp.Delivery) {
	p.setProcessing(id, d.MessageId)
	defer p.clearProcessing(id)

	// A redelivered manual-ack message means a prior worker died mid-processing
	// and RabbitMQ requeued it (Incident 2's crash chain). Surface it as a log
	// + event line: in auto mode redelivery never happens (the loss is silent).
	if d.Redelivered && !p.autoAck {
		p.emitEvent(events.Event{
			Type: "redelivered", Subject: p.queue,
			Data: "msg=" + d.MessageId,
		})
		if debugLogging {
			log.Printf("[%s worker %d] redelivered        %q (id=%s)", p.queue, id, d.Body, d.MessageId)
		}
	}
	if debugLogging {
		log.Printf("[%s worker %d] received         %q (id=%s)", p.queue, id, d.Body, d.MessageId)
	}

	err := p.handler(ctx, p.queue, d)

	// A cancelled context = the worker was killed (or the pool is being torn
	// down). Never ack, never nack: the broker decides via channel close
	// (manual: requeue) or auto-ack removal (auto: gone). A deliberately killed
	// worker in auto mode has lost its message — credit the failure. The
	// worker_crashed EVENT is emitted once by the pool manager at the kill
	// site (which knows queue + worker + msg without racing the worker exit).
	if ctx.Err() != nil {
		if p.wasKilled(id) {
			if p.autoAck {
				p.crashAuto.Add(1)
			}
			log.Printf("[%s worker %d] CRASHED mid-process (msg=%s); channel recycled, unacked requeued", p.queue, id, d.MessageId)
		}
		return
	}

	if err != nil {
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
		// keeps the loss honest and the counters single-count. In auto mode
		// the message is already gone — nothing to nack.
		if !p.autoAck {
			_ = d.Nack(false, false)
		}
		return
	}

	if !p.autoAck {
		if err := d.Ack(false); err != nil {
			log.Printf("[%s worker %d] ack failed:      %v", p.queue, id, err)
			return
		}
	}
	if debugLogging {
		log.Printf("[%s worker %d] completed %q (id=%s, ack=%s)", p.queue, id, d.Body, d.MessageId, ackPolicyLabel(p.autoAck))
	}
}

// DrainCrashedAuto returns and clears the count of messages lost to hard worker
// deaths in AUTO mode since the last drain (the simulation turns it into
// failure metrics — the success rate must reflect the loss).
func (p *WorkerPool) DrainCrashedAuto() int64 {
	return p.crashAuto.Swap(0)
}

func (p *WorkerPool) emitEvent(e events.Event) {
	// Safety net if no emitter is installed (unit tests): keep the marker in
	// the returned event harmless — the caller's log covers observability.
	if p.emit == nil {
		return
	}
	p.emit(e)
}

// logRecycle rate-limits the crash-reconnect console line to once per second.
func (p *WorkerPool) logRecycle() {
	p.recycleMu.Lock()
	defer p.recycleMu.Unlock()
	if time.Since(p.lastRecycle) < time.Second {
		return
	}
	p.lastRecycle = time.Now()
	log.Printf("worker pool %q recycled after worker crash (unacked batch requeued)", p.queue)
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

func ackPolicyLabel(auto bool) string {
	if auto {
		return string(AckAuto)
	}
	return string(AckManual)
}

// itoa is a tiny strconv-free integer formatter (worker ids are small).
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [6]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}