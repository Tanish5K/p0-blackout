package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"blackout/pkg/events"
)

// restartHold is how long a restart_worker slot stays down before respawning
// (Incident 2: "costs ~30s throughput"). The pool simply runs one fewer
// consumer for the rest period — the patient is the worker, not the pool.
const restartHold = 30 * time.Second

// PoolManager owns one WorkerPool per wired queue and reacts to registry
// changes. When a service goes unwired (player pause, incident teardown) its
// pool's context is cancelled — consumers stop and the queue stops being
// drained, so backlog accumulates. When wired, a pool is created/restarted.
//
// Declared MQ resources are intentionally left in place on pause so a paused
// queue keeps accumulating; that is the desired game behavior
type PoolManager struct {
	broker  *Broker
	reg     *Registry
	counts  map[string]int // queue -> worker count
	handler Handler
	// ackPolicy is the per-wired-queue acknowledgement mode ("manual"|"auto");
	// switching it rebuilds the queue's pool with a new Consume autoAck flag.
	ackPolicy map[string]string

	mu        sync.Mutex
	pools     map[string]*poolEntry
	sink      *eventSink
	suspended bool // lifecycle reset in progress; registry events cannot start pools
}

// eventSink routes crash/redelivery events from the consumer layer onto the
// game's event tape, stamped with the run's current tick/elapsed. The tick
// cursor comes from the simulation driver (null before a run starts).
type eventSink struct {
	log     *events.Log
	tick    func() int64
	elapsed func() time.Duration
}

type poolEntry struct {
	pool   *WorkerPool
	cancel context.CancelFunc
	done   chan struct{}
}

func NewPoolManager(broker *Broker, reg *Registry, counts map[string]int, handler Handler) *PoolManager {
	ac := make(map[string]string, len(counts))
	for q := range counts {
		ac[q] = string(AckManual)
	}
	return &PoolManager{
		broker:    broker,
		reg:       reg,
		counts:    counts,
		handler:   handler,
		ackPolicy: ac,
		pools:     make(map[string]*poolEntry),
	}
}

// SetEventSink points crash/redelivery events at the per-run event log (the
// simulation swaps it in each run so the client tape stays per-run). Nil log
// disables event emission.
func (m *PoolManager) SetEventSink(log *events.Log, tick func() int64, elapsed func() time.Duration) {
	m.mu.Lock()
	if log == nil {
		m.sink = nil
	} else {
		m.sink = &eventSink{log: log, tick: tick, elapsed: elapsed}
	}
	emitter := m.makeEmitter()
	entries := make([]*poolEntry, 0, len(m.pools))
	for _, e := range m.pools {
		entries = append(entries, e)
	}
	m.mu.Unlock()
	for _, e := range entries {
		e.pool.SetEmitter(emitter)
	}
}

// makeEmitter builds a pooled-emitter closure reading the current sink. Caller
// must hold m.mu (or pass a snapshot); the closure itself is safe to call
// concurrently from worker goroutines.
func (m *PoolManager) makeEmitter() func(events.Event) {
	s := m.sink
	var emit func(events.Event)
	if s == nil || s.log == nil {
		emit = nil
	} else {
		emit = func(e events.Event) {
			if s.tick != nil {
				e.Tick = s.tick()
			}
			if s.elapsed != nil {
				e.Time = s.elapsed()
			}
			s.log.Append(e)
		}
	}
	return emit
}

// Run reconciles pools against the registry until ctx is done.
func (m *PoolManager) Run(ctx context.Context) {
	changes := m.reg.Subscribe()
	m.reconcile()
	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return
		case <-changes:
			m.reconcile()
		}
	}
}

// reconcile brings running pools in line with the wired service set.
func (m *PoolManager) reconcile() {
	desired := m.reg.WiredQueues()
	want := make(map[string]bool, len(desired))
	for _, q := range desired {
		want[q] = true
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.suspended {
		return
	}

	// Start any desired queue that has no pool yet. desired already comes from
	// WiredQueues, so unwired/stub queues are skipped here by construction; the
	// Scale guard is the second (tested) line of defence.
	for _, q := range desired {
		if _, ok := m.pools[q]; ok {
			continue
		}
		m.startPoolLocked(q, m.counts[q])
	}

	// Stop pools whose queue is no longer wired.
	for q, entry := range m.pools {
		if want[q] {
			continue
		}
		entry.cancel()
		delete(m.pools, q)
		log.Printf("pool manager stopped %q", q)
		<-entry.done
	}
}

// startPoolLocked creates and launches a pool for a wired queue. Caller must
// hold m.mu.
func (m *PoolManager) startPoolLocked(q string, n int) {
	if n <= 0 {
		n = 1
	}
	poolCtx, cancel := context.WithCancel(context.Background())
	ack := m.ackPolicy[q]
	if ack == "" {
		ack = string(AckManual)
	}
	pool := NewWorkerPool(m.broker, m.reg.WiredTopology(), q, n, m.handler, ack == string(AckAuto))
	pool.SetEmitter(m.makeEmitter())
	done := make(chan struct{})
	m.pools[q] = &poolEntry{pool: pool, cancel: cancel, done: done}
	go func() {
		defer close(done)
		pool.Run(poolCtx)
	}()
	log.Printf("pool manager started %q (%d workers, ack=%s)", q, n, ack)
}

// Reconcile exposes a synchronous reconcile for callers that make many changes
// at once and want a settled state before proceeding.
func (m *PoolManager) Reconcile() {
	m.reconcile()
}

// Stop stops every pool and leaves automatic reconciliation suspended.
func (m *PoolManager) Stop() {
	m.StopAndWait()
}

// StopAndWait cancels every consumer pool and waits for its worker goroutines
// to exit. Start must be called after the next run is fully configured.
func (m *PoolManager) StopAndWait() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suspended = true
	for q, entry := range m.pools {
		entry.cancel()
		log.Printf("pool manager stopping %q", q)
	}
	for q, entry := range m.pools {
		<-entry.done
		delete(m.pools, q)
	}
}

// Configure replaces per-run counts and ack policies without starting pools.
func (m *PoolManager) Configure(counts map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts = make(map[string]int, len(counts))
	m.ackPolicy = make(map[string]string, len(counts))
	for queue, workers := range counts {
		m.counts[queue] = workers
		m.ackPolicy[queue] = string(AckManual)
	}
}

// Start resumes registry reconciliation and synchronously starts pools for the
// currently wired topology.
func (m *PoolManager) Start() {
	m.mu.Lock()
	m.suspended = false
	m.mu.Unlock()
	m.reconcile()
}

func (m *PoolManager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suspended = true
	for _, entry := range m.pools {
		entry.cancel()
	}
	for q, entry := range m.pools {
		<-entry.done
		delete(m.pools, q)
	}
}

// PoolCount returns the number of currently-running pools (for tests/inspection).
func (m *PoolManager) PoolCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pools)
}

// Scale restarts the pool for the given queue with a new worker count.
// Returns an error if the queue is unknown (never declares one) or is owned by
// an unwired service — stub services must not gain workers.
func (m *PoolManager) Scale(queue string, workers int) error {
	if !m.reg.HasWiredQueue(queue) {
		return fmt.Errorf("unknown or unwired queue: %s", queue)
	}
	if workers < 1 {
		workers = 1
	}
	m.mu.Lock()
	// Update the count for reconcile.
	m.counts[queue] = workers
	// Stop the existing pool if running.
	m.restartPoolLocked(queue)
	m.mu.Unlock()
	return nil
}

// restartPoolLocked tears down and re-creates one queue's pool. Caller must
// hold m.mu. Used by Scale, SetAckPolicy (a new ack mode needs a fresh consumer
// channel) and the restart-worker hold.
func (m *PoolManager) restartPoolLocked(queue string) {
	entry, ok := m.pools[queue]
	if !ok {
		return
	}
	entry.cancel()
	delete(m.pools, queue)
	<-entry.done
	m.startPoolLocked(queue, m.counts[queue])
}

// Reset stops every pool and restarts the full set at the given worker counts
// (not the last scale_workers value). Used by the run controller between
// incidents so a player's scaling doesn't leak into the next run. Waits for
// every pool goroutine to exit before returning, so no worker from the old
// run can be mid-handler while the runtime resets underneath it.
func (m *PoolManager) Reset(counts map[string]int) {
	m.StopAndWait()
	m.Configure(counts)
	m.Start()
	log.Printf("pool manager reset (%d pools at defaults)", len(m.counts))
}

// Workers returns the configured worker count for a queue (0 if unknown).
func (m *PoolManager) Workers(queue string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[queue]
}

// AckPolicy returns the queue's acknowledgement mode ("manual"|"auto").
func (m *PoolManager) AckPolicy(queue string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.ackPolicy[queue]
	if p == "" {
		p = string(AckManual)
	}
	return p
}

// SetAckPolicy toggles a queue between manual and auto acknowledgement and
// rebuilds its consumer with the new mode. Rejected for unknown/unwired queues
// (stub services must not gain consumers). Switching modes restarts the pool,
// which honestly re-queues any in-flight manual messages (consumer replace).
func (m *PoolManager) SetAckPolicy(queue, policy string) error {
	if !m.reg.HasWiredQueue(queue) {
		return fmt.Errorf("unknown or unwired queue: %s", queue)
	}
	if policy != string(AckManual) && policy != string(AckAuto) {
		return fmt.Errorf("policy must be %q or %q", AckManual, AckAuto)
	}
	m.mu.Lock()
	m.ackPolicy[queue] = policy
	m.restartPoolLocked(queue)
	m.mu.Unlock()
	log.Printf("pool manager set ack policy %q → %s", queue, policy)
	return nil
}

// KillWorker crashes the given worker on a queue and breaks its session
// (channel close → manual-ack batch requeues → redelivery; auto-ack losses are
// credited via DrainCrashedAuto). Used by the Incident 2 crash storm and the
// restart_worker action. The worker_crashed event is emitted here, once, at
// the moment of the kill.
func (m *PoolManager) KillWorker(queue string, workerID int) error {
	m.mu.Lock()
	entry, ok := m.pools[queue]
	sink := m.sink
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no consumer pool for %s", queue)
	}
	msg := entry.pool.ProcessingMessage(workerID)
	if err := entry.pool.Kill(workerID); err != nil {
		return err
	}
	m.emitEvent(sink, events.Event{
		Type: "worker_crashed", Subject: queue,
		Data: "worker=" + itoa(workerID) + " msg=" + msg,
	})
	log.Printf("worker %d on %q crashed (msg=%s)", workerID, queue, msg)
	return nil
}

// emitEvent stamps (tick/elapsed) and appends an event onto the run's tape. The
// sink snapshot is taken under lock by the caller; appends are mutex-safe.
func (m *PoolManager) emitEvent(sink *eventSink, e events.Event) {
	if sink == nil || sink.log == nil {
		return
	}
	if sink.tick != nil {
		e.Tick = sink.tick()
	}
	if sink.elapsed != nil {
		e.Time = sink.elapsed()
	}
	sink.log.Append(e)
}

// RestartWorker deliberately crashes one worker and holds its slot ~30s before
// respawn (Incident 2's restart_worker action). Same mechanism as a DB-spike
// crash — the only difference is the player chose it and it costs throughput.
func (m *PoolManager) RestartWorker(queue string, workerID int) error {
	m.mu.Lock()
	entry, ok := m.pools[queue]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no consumer pool for %s", queue)
	}
	if err := m.KillWorker(queue, workerID); err != nil {
		return err
	}
	entry.pool.Restart(restartHold)
	return nil
}

// RestartRandomWorker crashes a random live worker and holds its slot ~30s
// (restart_worker without a pinned worker id).
func (m *PoolManager) RestartRandomWorker(queue string) error {
	m.mu.Lock()
	entry, ok := m.pools[queue]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no consumer pool for %s", queue)
	}
	report := entry.pool.Report()
	if len(report) == 0 {
		return fmt.Errorf("no live workers on %q", queue)
	}
	w := report[rand.Intn(len(report))]
	if err := m.KillWorker(queue, w.ID); err != nil {
		return err
	}
	entry.pool.Restart(restartHold)
	return nil
}

// CrashRandomWorker crashes a random live worker on a queue (the DB-spike storm
// driver calls this per tick with a small probability). Errors if the pool has
// no live workers at the instant of the call.
func (m *PoolManager) CrashRandomWorker(queue string) error {
	m.mu.Lock()
	entry, ok := m.pools[queue]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no consumer pool for %s", queue)
	}
	report := entry.pool.Report()
	if len(report) == 0 {
		return fmt.Errorf("no live workers on %q", queue)
	}
	w := report[rand.Intn(len(report))]
	if err := m.KillWorker(queue, w.ID); err != nil {
		return err
	}
	// A DB-spike crash is not instantly healed: the dead slot stays cold for
	// restartHold too, so a spike of crashes reads as genuinely "falling
	// workers" rather than a sequence of costless blips.
	entry.pool.Restart(restartHold)
	return nil
}

// PoolReport returns each live worker + current message for a queue's pool.
func (m *PoolManager) PoolReport(queue string) []WorkerReport {
	m.mu.Lock()
	entry, ok := m.pools[queue]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return entry.pool.Report()
}

// DrainCrashedAuto returns and clears the total auto-ack crash loss across all
// pools (message count, not per queue). The simulation turns it into failure
// metrics so the success rate reflects the loss.
func (m *PoolManager) DrainCrashedAuto() int64 {
	m.mu.Lock()
	pools := make([]*WorkerPool, 0, len(m.pools))
	for _, e := range m.pools {
		pools = append(pools, e.pool)
	}
	m.mu.Unlock()
	var total int64
	for _, p := range pools {
		total += p.DrainCrashedAuto()
	}
	return total
}
