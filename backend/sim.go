package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"blackout/internal/rabbitmq"
	"blackout/internal/simulation"
	"blackout/pkg/events"
)

const (
	mgmtVhost       = "/"
	mgmtPollEvery   = time.Second // ~every 10 ticks; matches the 10Hz broadcast cadence
	maxEvents       = 5000
	maxPublishRetry = 5
)

// cachedQueue is one queue's latest real telemetry plus the rolling rates the
// poller derives by diffing cumulative counters between polls.
type cachedQueue struct {
	stats   rabbitmq.QueueStats
	rateIn  float64
	rateOut float64
}

// queueCache is written by the poller goroutine and read (non-blocking) by the
// 100ms tick loop.
type queueCache struct {
	mu   sync.Mutex
	data map[string]cachedQueue
}

func newQueueCache() *queueCache {
	return &queueCache{data: make(map[string]cachedQueue)}
}

// runSimulation drives the real simulation: on each 100ms tick it decides what
// to publish, publishes it for real through the confirmed Publisher, and turns
// the (real, polled) queue depth back into metrics.
func runSimulation(ctx context.Context, pub *rabbitmq.Publisher, mgmt *rabbitmq.Mgmt, rt *simulation.Runtime) {
	const seed = 42
	profile := simulation.StampedeProfile{
		BaseRate: 200,
		PeakRate: 10000,
		Ramp:     5 * time.Minute,
		Hold:     30 * time.Second,
	}
	state := simulation.NewGame(seed, profile)
	state.Sim = rt

	cache := newQueueCache()
	queueNames := snapshotQueueNames(state)

	go pollQueues(ctx, mgmt, cache, queueNames)

	t := time.NewTicker(simulation.TickInterval)
	defer t.Stop()

	log.Printf("simulation started (seed=%d, tick=%v)", seed, simulation.TickInterval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("simulation stopped after %d ticks (%s)", state.Tick, state.Elapsed)
			return
		case <-t.C:
			evs := simulation.Tick(state, simulation.TickInterval)
			for _, e := range evs {
				state.Log.Append(e)
			}
			bridgePublish(ctx, pub, state)
			applyCache(cache, state)
			state.Log.Trim(maxEvents)
			if state.Tick%10 == 0 {
				logRail(state)
			}
		}
	}
}

// bridgePublish turns this tick's publish plan (decided deterministically by
// Tick) into real messages on RabbitMQ. Order events fan out to orders.work and
// analytics.events; a small share becomes payment authorisations. Every
// successfully published batch also feeds the load trackers so workers feel
// the new load immediately — the management snapshot only reconciles drift.
func bridgePublish(ctx context.Context, pub *rabbitmq.Publisher, s *simulation.GameState) {
	if s.Traffic.OrderMessagesThisTick > 0 {
		n := s.Traffic.OrderMessagesThisTick
		bodies := makeBodies(n, "order.created", s.Tick)
		if err := publishWithRetry(ctx, pub, "order.events", "order.created", bodies); err != nil {
			appendDropped(s, "order.events", n, err)
		} else {
			s.Sim.AddPublished("orders.work", int(n))
			s.Sim.AddPublished("analytics.events", int(n))
		}
	}
	if s.Traffic.PayMessagesThisTick > 0 {
		n := s.Traffic.PayMessagesThisTick
		bodies := makeBodies(n, "payment.auth", s.Tick)
		if err := publishWithRetry(ctx, pub, "payment.events", "payment.auth", bodies); err != nil {
			appendDropped(s, "payment.events", n, err)
		} else {
			s.Sim.AddPublished("payments.work", int(n))
		}
	}
}

// publishWithRetry retries a batch with short backoff, then gives up so the
// caller can record the drop. This keeps the published sequence recoverable
// from the event log rather than silently losing traffic under connection churn.
func publishWithRetry(ctx context.Context, pub *rabbitmq.Publisher, exchange, key string, bodies [][]byte) error {
	backoff := 50 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := pub.PublishBatch(ctx, exchange, key, bodies)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt >= maxPublishRetry {
			return err
		}
		time.Sleep(backoff)
		backoff *= 2
	}
}

// makeBodies builds the message payloads for one batch. Each carries a short
// id so the broker UI and any later causal tracing can tell them apart.
func makeBodies(n int64, kind string, tick int64) [][]byte {
	bodies := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		body := []byte(fmt.Appendf(nil, `{"id":"%s-%d-%d","kind":"%s"}`, kind, tick, i, kind))
		bodies = append(bodies, body)
	}
	return bodies
}

func appendDropped(s *simulation.GameState, subject string, n int64, err error) {
	s.Log.Append(events.Event{
		Tick: s.Tick, Time: s.Elapsed, Type: "fail", Subject: subject,
		Value: float64(n), Data: "dropped: " + err.Error(),
	})
	log.Printf("bridge: dropped %d messages on %s (%v)", n, subject, err)
}

// applyCache writes the latest polled telemetry into the state's queues and
// pools, reading the poller's cache without blocking. Missing entries leave the
// last-known values untouched (no zeroing). It also reconciles the load
// trackers against the authoritative real depth (client-prediction /
// server-snapshot pattern).
func applyCache(c *queueCache, s *simulation.GameState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range s.Queues {
		cq, ok := c.data[s.Queues[i].Name]
		if !ok {
			continue
		}
		s.Queues[i].Depth = cq.stats.Messages
		s.Queues[i].InFlight = cq.stats.MessagesReady
		s.Queues[i].Unacked = cq.stats.MessagesUnacked
		s.Queues[i].RateIn = cq.rateIn
		s.Queues[i].RateOut = cq.rateOut
		s.Sim.Reconcile(s.Queues[i].Name, cq.stats.Messages)
	}
	for i := range s.Pools {
		if cq, ok := c.data[s.Pools[i].Queue]; ok {
			s.Pools[i].Workers = cq.stats.Consumers
		}
	}
}

// pollQueues is a separate goroutine that samples the management API on a slow
// cadence and writes into the cache. The first successful poll for a queue seeds
// the rate baseline (rates = 0); later polls diff the cumulative counters.
func pollQueues(ctx context.Context, mgmt *rabbitmq.Mgmt, cache *queueCache, names []string) {
	t := time.NewTicker(mgmtPollEvery)
	defer t.Stop()
	pollOnce := func() {
		for _, name := range names {
			stats, err := mgmt.QueueStats(mgmtVhost, name)
			if err != nil {
				// Degrade: keep last-known, don't zero, don't crash. Log once.
				log.Printf("bridge: management poll %q failed (%v); holding last-known", name, err)
				continue
			}
			cache.update(name, stats, mgmtPollEvery)
		}
	}
	pollOnce()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pollOnce()
		}
	}
}

func (c *queueCache) update(name string, stats rabbitmq.QueueStats, interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rateIn, rateOut := 0.0, 0.0
	prev, ok := c.data[name]
	if ok {
		secs := interval.Seconds()
		if secs > 0 {
			if stats.PublishCount >= prev.stats.PublishCount {
				rateIn = float64(stats.PublishCount-prev.stats.PublishCount) / secs
			}
			if stats.AckCount >= prev.stats.AckCount {
				rateOut = float64(stats.AckCount-prev.stats.AckCount) / secs
			}
		}
	}
	c.data[name] = cachedQueue{
		stats:   stats,
		rateIn:  rateIn,
		rateOut: rateOut,
	}
}

func snapshotQueueNames(s *simulation.GameState) []string {
	out := make([]string, 0, len(s.Queues))
	for _, q := range s.Queues {
		out = append(out, q.Name)
	}
	return out
}

// logRail prints a one-line moving snapshot of the simulation.
func logRail(s *simulation.GameState) {
	log.Printf(
		"[tick %6d t=%s] rate=%.0f/s inWin=%d  queues: %s  latency p50=%s p99=%s success=%.1f%% health=%.0f events=%d",
		s.Tick,
		s.Elapsed.Round(simulation.TickInterval),
		s.Traffic.RatePerSec,
		windowTotal(s.Traffic.WinIn),
		queueRail(s.Queues),
		s.Metrics.LatencyP50.Round(time.Microsecond),
		s.Metrics.LatencyP99.Round(time.Microsecond),
		s.Metrics.SuccessRate*100,
		s.Metrics.SystemHealth,
		s.Log.Len(),
	)
}

func queueRail(qs []simulation.QueueState) string {
	out := ""
	for i, q := range qs {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s=%dd(in%.0f/out%.0f)", q.Name, q.Depth, q.RateIn, q.RateOut)
	}
	return out
}

func windowTotal(v []int64) int64 {
	var t int64
	for _, x := range v {
		t += x
	}
	return t
}
