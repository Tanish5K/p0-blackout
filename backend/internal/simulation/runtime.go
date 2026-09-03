package simulation

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// serviceName identifies the three simulated backend services. Each owns a DB,
// and exactly one worker queue pumps work to it.
const (
	serviceOrders    = "orders"
	serviceAnalytics = "analytics"
	servicePayments  = "payments"
)

// Runtime is the bridge between the simulated backend services (their DBs and
// per-queue load) and the real broker. The simulation driver feeds it
// published counts; the consumer workers feed it acks, failures and latency
// samples; the metrics derive their picture of service health from it.
type queueState struct {
	service string
	tracker *LoadTracker
	sampler *LatencySampler
	acked   atomic.Int64 // total messages finished successfully
	failed  atomic.Int64 // total messages failed/nacked
}

// Runtime holds everything the workers and the sim need to know about the
// current run.
type Runtime struct {
	mu     sync.RWMutex
	dbs    map[string]*DB         // service name -> DB
	queues map[string]*queueState // queue name -> state
	order  []string               // stable queue iteration order
}

// NewRuntime wires the runtime to the matching topology: orders.work and
// analytics.events both fan out of order.created; payments.work only sees
// payment.auth (see §5.1).
//
// DB tuning below is a DEV-FAST pass for iteration speed: a smallish ramp
// crosses each service's ceiling around ~35-45s so a test run visibly breaks
// within a minute. This is NOT the final Incident 1 balance — see TODO.md.
// Intent to preserve: a true incident runs a 5-minute ramp / 8-minute survive
// window, with the heuristic ceilings well above the calm 200/s baseline so
// strain builds only near the peak. Original (slower-onset) values, kept for
// reference:
//
//	orders.work:    NewDB(6,  400, 4)  // was ~360/s ceiling -> knee ~5s
//	analytics.events: NewDB(10, 120, 3) // saturated from t=0 (full fan-out)
//	payments.work:  NewDB(14, 80,  3)  // ~470/s ceiling -> knee ~6s
func NewRuntime() *Runtime {
	rt := &Runtime{
		dbs: map[string]*DB{
			serviceOrders:    NewDB(1.2, 3000, 6), // ~1300/s ceiling -> knee ~40s
			serviceAnalytics: NewDB(0.8, 4000, 4), // ~1700/s ceiling -> knee ~43s
			servicePayments:  NewDB(1.8, 1500, 3), // ~620/s ceiling -> knee ~47s
		},
		queues: make(map[string]*queueState),
	}
	rt.addQueue("orders.work", serviceOrders)
	rt.addQueue("analytics.events", serviceAnalytics)
	rt.addQueue("payments.work", servicePayments)
	rt.order = []string{"orders.work", "analytics.events", "payments.work"}
	return rt
}

func (rt *Runtime) addQueue(name, service string) {
	rt.queues[name] = &queueState{
		service: service,
		tracker: NewLoadTracker(),
		sampler: NewLatencySampler(),
	}
}

// DB returns the DB for a service (nil if unknown).
func (rt *Runtime) DB(service string) *DB {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.dbs[service]
}

// DBForQueue returns the DB backing the given queue (nil if unknown).
func (rt *Runtime) DBForQueue(queue string) *DB {
	rt.mu.RLock()
	svc, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if !ok {
		return nil
	}
	return rt.dbs[svc.service]
}

// Tracker returns the load tracker for a queue (nil if unknown).
func (rt *Runtime) Tracker(queue string) *LoadTracker {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if q, ok := rt.queues[queue]; ok {
		return q.tracker
	}
	return nil
}

// Sampler returns the latency sampler for a queue (nil if unknown).
func (rt *Runtime) Sampler(queue string) *LatencySampler {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if q, ok := rt.queues[queue]; ok {
		return q.sampler
	}
	return nil
}

// QueueNames returns the queue names, in stable order.
func (rt *Runtime) QueueNames() []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := make([]string, len(rt.order))
	copy(out, rt.order)
	return out
}

// AddPublished records n publishes handed to the broker for a queue.
func (rt *Runtime) AddPublished(queue string, n int) {
	rt.mu.RLock()
	t, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if ok {
		t.tracker.AddPublished(int64(n))
	}
}

// AddAcked records n successful consumer completions for a queue.
func (rt *Runtime) AddAcked(queue string, n int) {
	rt.mu.RLock()
	t, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if ok {
		t.tracker.AddAcked(int64(n))
		t.acked.Add(int64(n))
	}
}

// AddFailed records n consumer failures for a queue.
func (rt *Runtime) AddFailed(queue string, n int) {
	rt.mu.RLock()
	t, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if ok {
		t.failed.Add(int64(n))
	}
}

// RecordLatency records one observed processing time for a queue.
func (rt *Runtime) RecordLatency(queue string, d time.Duration) {
	rt.mu.RLock()
	t, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if ok {
		t.sampler.Record(d)
	}
}

// Reconcile overwrites the queue's pending estimate with authoritative depth
// from the management snapshot.
func (rt *Runtime) Reconcile(queue string, depth int64) {
	rt.mu.RLock()
	t, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if ok {
		t.tracker.Reconcile(depth)
	}
}

// Pending returns the estimated depth of a queue (0 if unknown).
func (rt *Runtime) Pending(queue string) int64 {
	rt.mu.RLock()
	t, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if ok {
		return t.tracker.Pending()
	}
	return 0
}

// SuccessRate returns acked/(acked+failed) for a queue (1 if no work yet).
func (rt *Runtime) SuccessRate(queue string) float64 {
	rt.mu.RLock()
	t, ok := rt.queues[queue]
	rt.mu.RUnlock()
	if !ok {
		return 1
	}
	total := t.acked.Load() + t.failed.Load()
	if total == 0 {
		return 1
	}
	return float64(t.acked.Load()) / float64(total)
}

// QueueSnapshot is a read-only view used by the metrics pass.
type QueueSnapshot struct {
	Name       string
	Service    string
	Pending    int64
	Acked      int64
	Failed     int64
	P50        time.Duration
	P99        time.Duration
	Success    float64
	Capacity   float64
	Multiplier float64
}

// Snapshots returns one snapshot per queue, sorted by queue name for a stable
// display order.
func (rt *Runtime) Snapshots() []QueueSnapshot {
	names := make([]string, len(rt.order))
	rt.mu.RLock()
	copy(names, rt.order)
	rt.mu.RUnlock()

	sort.Strings(names)
	out := make([]QueueSnapshot, 0, len(names))
	for _, name := range names {
		rt.mu.RLock()
		t, ok := rt.queues[name]
		rt.mu.RUnlock()
		if !ok {
			continue
		}
		db := rt.dbs[t.service]
		svc := QueueSnapshot{
			Name:    name,
			Service: t.service,
			Pending: t.tracker.Pending(),
			Acked:   t.acked.Load(),
			Failed:  t.failed.Load(),
			P50:     t.sampler.Percentile(50),
			P99:     t.sampler.Percentile(99),
			Success: 1,
		}
		if db != nil {
			svc.Capacity = db.Capacity()
			svc.Multiplier = db.Multiplier()
		}
		if total := t.acked.Load() + t.failed.Load(); total > 0 {
			svc.Success = float64(t.acked.Load()) / float64(total)
		}
		out = append(out, svc)
	}
	return out
}
