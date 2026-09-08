package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"
)

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

	mu    sync.Mutex
	pools map[string]*poolEntry
}

type poolEntry struct {
	pool   *WorkerPool
	cancel context.CancelFunc
	done   chan struct{}
}

func NewPoolManager(broker *Broker, reg *Registry, counts map[string]int, handler Handler) *PoolManager {
	return &PoolManager{
		broker:  broker,
		reg:     reg,
		counts:  counts,
		handler: handler,
		pools:   make(map[string]*poolEntry),
	}
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

	// Start any desired queue that has no pool yet. desired already comes from
	// WiredQueues, so unwired/stub queues are skipped here by construction; the
	// Scale guard is the second (tested) line of defence.
	for _, q := range desired {
		if _, ok := m.pools[q]; ok {
			continue
		}
		n := m.counts[q]
		if n <= 0 {
			n = 1
		}
		poolCtx, cancel := context.WithCancel(context.Background())
		pool := NewWorkerPool(m.broker, m.reg.WiredTopology(), q, n, m.handler)
		done := make(chan struct{})
		m.pools[q] = &poolEntry{pool: pool, cancel: cancel, done: done}
		go func() {
			defer close(done)
			pool.Run(poolCtx)
		}()
		log.Printf("pool manager started %q (%d workers)", q, n)
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

// Reconcile exposes a synchronous reconcile for callers that make many changes
// at once and want a settled state before proceeding.
func (m *PoolManager) Reconcile() {
	m.reconcile()
}

// Stop stops every pool.
func (m *PoolManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for q, entry := range m.pools {
		entry.cancel()
		log.Printf("pool manager stopping %q", q)
	}
	for q, entry := range m.pools {
		<-entry.done
		delete(m.pools, q)
	}
}

func (m *PoolManager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	if entry, ok := m.pools[queue]; ok {
		entry.cancel()
		delete(m.pools, queue)
		<-entry.done
	}
	m.mu.Unlock()
	// Reconcile will start a new pool with the updated count.
	m.reconcile()
	return nil
}

// Reset stops every pool and restarts the full set at the given worker counts
// (not the last scale_workers value). Used by the run controller between
// incidents so a player's scaling doesn't leak into the next run. Waits for
// every pool goroutine to exit before returning, so no worker from the old
// run can be mid-handler while the runtime resets underneath it.
func (m *PoolManager) Reset(counts map[string]int) {
	m.Stop()
	m.mu.Lock()
	m.counts = make(map[string]int, len(counts))
	for k, v := range counts {
		m.counts[k] = v
	}
	m.mu.Unlock()
	m.reconcile()
	log.Printf("pool manager reset (%d pools at defaults)", len(m.counts))
}

// Workers returns the configured worker count for a queue (0 if unknown).
func (m *PoolManager) Workers(queue string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[queue]
}
