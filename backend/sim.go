package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"blackout/internal/api"
	"blackout/internal/rabbitmq"
	"blackout/internal/simulation"
	"blackout/pkg/events"
)

const (
	mgmtVhost       = "/"
	mgmtPollEvery   = time.Second // ~every 10 ticks; matches the 10Hz broadcast cadence
	maxEvents       = 30000       // full 8-min run ≈ 19k events: keeps the whole timeline buildable
	maxPublishRetry = 5
)

// cachedQueue is one queue's latest real telemetry from the management API.
// Depths are authoritative; NO rates are derived from mgmt counters here — the
// message_stats counters reset on RabbitMQ's own retention schedule, so diffing
// them against a fixed poll interval produced bogus rail rates. Rates come from
// the runtime's own trackers instead (see applyRates).
type cachedQueue struct {
	stats rabbitmq.QueueStats
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
// the (real, polled) queue depth back into metrics. It also runs the objective
// evaluator; once a fail triggers or the ramp completes, the generator stops
// and the final state is left in place (no os.Exit, so the HTTP/WS server and
// broker connection stay alive for inspection). Returns the terminal Outcome
// (nil if the run was cancelled externally).
func runSimulation(ctx context.Context, pub *rabbitmq.Publisher, mgmt *rabbitmq.Mgmt, rt *simulation.Runtime, hub *api.Hub, pm *rabbitmq.PoolManager, reg *rabbitmq.Registry, spec *runSpec) *simulation.Outcome {
	survive := simulation.IncidentSurviveDuration
	state := simulation.NewGameOpts(spec.seed, spec.profile, spec.options)
	state.Sim = rt

	cache := newQueueCache()
	queueNames := snapshotQueueNames(state)

	go pollQueues(ctx, mgmt, cache, queueNames)

	generatorOn := true
	var peakWorkers int

	// Phase 5: snapshot carrier. Events are broadcast as a cursor over the
	// shared log (tick traffic AND player action events), not as the tick's own
	// emit list, so the live event tape shows player interventions immediately.
	var prevSnap *api.Snapshot
	var lastLogSeq uint64

	// One-shot health floor warnings so the console flags the crossing the tick
	// it happens, instead of the player having to spot it in a 10-tick rail.
	warn70, warn50 := false, false

// Action log events are stamped with the CURRENT tick. They share the log delta
// with that tick's traffic and arrive after it (same broadcast), so the client
// dedup (append only events with tick > last-seen tick), which keys off the
// previous message's LAST event, keeps the whole sequence.
adapter := &registryAdapter{reg: reg, state: state}
actionHandler := api.NewActionHandler(pm, adapter, adapter, state.Log, func() int64 { return state.Tick })
hub.SetActionHandler(actionHandler)

	// Incident 2's scripted DB event: the spike is active while elapsed is
	// inside any milestone window; entering/leaving is a log+event transition
	// so the tape can tell the story, and while active the driver rolls the
	// per-tick crash die.
	var spikeActive bool
	spikeInWindow := func() (bool, int) {
		if spec.spike == nil {
			return false, 0
		}
		for i, m := range spec.spike.Milestones {
			start, end := m, m+spec.spike.Duration
			if state.Elapsed >= start && state.Elapsed < end {
				return true, i + 1
			}
		}
		return false, 0
	}

	t := time.NewTicker(simulation.TickInterval)
	defer t.Stop()

	log.Printf("simulation started: incident %d \"%s\" (run=%d, seed=%d, ramp=%s, hold=%s, survive=8:00, fastDB=%s, identity=%v, cascade=%.2fx)",
		spec.options.IncidentNumber, spec.options.IncidentName, spec.runID,
		spec.seed, spec.profile.Ramp, spec.profile.Hold, os.Getenv("BLACKOUT_FAST_DB"),
		spec.options.IncludeIdentity, spec.options.DBFailureMult)

	for {
		select {
		case <-ctx.Done():
			log.Printf("simulation stopped after %d ticks (%s)", state.Tick, state.Elapsed)
			return nil
		case <-t.C:
			evs := simulation.Tick(state, simulation.TickInterval)
			for _, e := range evs {
				state.Log.Append(e)
			}
			if generatorOn {
				bridgePublish(ctx, pub, state)
			}
			applyCache(cache, state)
			syncPoolCounts(state, pm)
			adapter.syncWired(state)
			state.Log.Trim(maxEvents)

			// Incident 2 spike driver: set/reset the DB latency multiplier on
			// the transition ticks, roll the crash die while active, and credit
			// this tick's auto-mode crash loss into the failure counters so the
			// success rate honestly reflects the "falling workers" churn.
			if spec.spike != nil {
				on, wave := spikeInWindow()
				if on && !spikeActive {
					spikeActive = true
					_ = rt.SetDBSpike(spec.spike.Queue, spec.spike.LatMult)
					state.Log.Append(events.Event{
						Tick: state.Tick, Time: state.Elapsed, Type: "fail",
						Subject: spec.spike.Queue,
						Data:    fmt.Sprintf("db latency spike ×%.0f (wave %d/3)", spec.spike.LatMult, wave),
					})
					log.Printf("INCIDENT 2: DB latency spike ×%.0f on %s at %s (wave %d/3)",
						spec.spike.LatMult, spec.spike.Queue, spec.options.IncidentName, wave)
				} else if !on && spikeActive {
					spikeActive = false
					_ = rt.SetDBSpike(spec.spike.Queue, 1)
					log.Printf("INCIDENT 2: DB spike cleared on %s at %s", spec.spike.Queue, state.Elapsed.Round(simulation.TickInterval))
				}
				if spikeActive {
					if state.Rng().Float64() < spec.spike.CrashProbPerTick {
						if err := pm.CrashRandomWorker(spec.spike.Queue); err != nil {
							// No live worker this instant (pool mid-hold): the
							// storm is already thinning it; skip quietly.
							_ = err
						}
					}
				}
				if lost := pm.DrainCrashedAuto(); lost > 0 {
					rt.AddFailed(spec.spike.Queue, int(lost))
				}
			}

			// Peak pool workers (the campaign-cascade input): sum CONFIGURED
			// pool sizes, which syncPoolCounts refreshes from the pool manager
			// (scale_workers / run start / crash-hold reductions).
			if total := totalWorkers(state); total > peakWorkers {
				peakWorkers = total
			}

			// One-shot health floor warnings, fired the tick they cross. The
			// worst service is named — a bare percentage hides which node the
			// rail is silently reporting on.
			if generatorOn {
				if !warn70 && state.Metrics.SystemHealth < 70 {
					warn70 = true
					log.Printf("WARN: system health dropped below 70%% at t=%s (health=%.0f%%, worst=%s)",
						state.Elapsed.Round(simulation.TickInterval), state.Metrics.SystemHealth, worstServiceAt(state))
				}
				if !warn50 && state.Metrics.SystemHealth < 50 {
					warn50 = true
					log.Printf("WARN: system health dropped below 50%% at t=%s (health=%.0f%%, worst=%s)",
						state.Elapsed.Round(simulation.TickInterval), state.Metrics.SystemHealth, worstServiceAt(state))
				}
			}

			// Rail rates come from the runtime's own windowed trackers (exact
			// windowing), not from diffing mgmt counters that reset on RabbitMQ's
			// retention schedule.
			if generatorOn && state.Tick%10 == 0 {
				applyRates(state, rt)
				logRail(state)
			}

			// Objective evaluation: a fail trips the debounced floor; otherwise
			// the run completes the moment the 8:00 survive window elapses.
			var out *simulation.Outcome
			if generatorOn {
				out = state.Objectives.Tick(state)
				if out == nil && state.Elapsed >= survive {
					out = state.Objectives.Complete(state)
				}
				if out != nil {
					out.PeakWorkers = peakWorkers
					state.Outcome = out
					state.Timeline = buildTimeline(state, out)
					generatorOn = false
				}
			}

			// Broadcast every tick (10Hz). Events are the whole log delta since
			// the last broadcast; the client appends and dedups by tick. Every
			// message is tagged with the run it belongs to so the client can
			// wipe its merged state the instant a retry starts (a fresh run
			// also means prevSnap starts nil here → a full first frame).
			snap := api.SnapshotFromState(state, !generatorOn)
			snap.RunID = spec.runID
			snap.RunStatus = "running"
			snap.CampaignTotal = spec.campaignTotal
			if out != nil {
				snap.RunStatus = "ended"
			}
			snap.Events = state.Log.After(lastLogSeq)
			lastLogSeq = state.Log.Seq()
			delta := api.Delta(prevSnap, &snap)
			if data, err := api.MarshalSnapshot(delta); err == nil {
				hub.Broadcast(data)
			}
			prevSnap = &snap

			if out != nil {
				// Terminal freeze: the final state (outcome + timeline) was just
				// broadcast; stop the tick loop. The HTTP/WS server and broker
				// connection stay alive for inspection.
				logOutcome(state, out)
				return out
			}
		}
	}
}

// totalWorkers sums the configured pool sizes shown in the state.
func totalWorkers(s *simulation.GameState) int {
	var n int
	for _, p := range s.Pools {
		n += p.Workers
	}
	return n
}

// seedFromEnv resolves the deterministic demo seed (BLACKOUT_SEED). Default 42
// gives the same incident every cold start; 0 is rejected so Chaos-Mode seeds
// (fed in later) can rely on non-zero RNG sources.
func seedFromEnv() int64 {
	var seed int64 = 42
	if v := os.Getenv("BLACKOUT_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n != 0 {
			seed = n
		} else {
			log.Printf("BLACKOUT_SEED %q ignored (want a non-zero integer)", v)
		}
	}
	return seed
}

// stampedeProfileFromEnv builds Incident 1's traffic shape from env. Ramp
// defaults to 5 minutes; BLACKOUT_RAMP overrides only the ramp portion for
// faster dev loops. The survive window stays fixed at the real 8:00, so
// Hold = Survive − Ramp: a dev ramp of 120s still exercises the full 8-minute
// survive objective, just with a compressed build-up.
func stampedeProfileFromEnv() simulation.StampedeProfile {
	ramp := 5 * time.Minute
	if v := os.Getenv("BLACKOUT_RAMP"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ramp = d
		} else {
			log.Printf("BLACKOUT_RAMP %q ignored (want a positive duration)", v)
		}
	}
	hold := simulation.IncidentSurviveDuration - ramp
	if hold < 0 {
		hold = 0
	}
	return simulation.StampedeProfile{
		BaseRate: 200,
		PeakRate: 10000,
		Ramp:     ramp,
		Hold:     hold,
	}
}

// applyRates writes each queue's windowed in/out rate from the runtime's own
// published-vs-acked+failed trackers (exact 1s window, no mgmt retention skew).
func applyRates(s *simulation.GameState, rt *simulation.Runtime) {
	rates := rt.SnapshotRates(simulation.TickInterval * 10)
	for i := range s.Queues {
		if r, ok := rates[s.Queues[i].Name]; ok {
			s.Queues[i].RateIn = r.In
			s.Queues[i].RateOut = r.Out
		}
	}
}

// bridgePublish turns this tick's publish plan (decided deterministically by
// Tick) into real messages on RabbitMQ. Order events fan out to orders.work and
// analytics.events; a small share becomes payment authorisations. Incident 2
// adds the synchronous identity hop (identity.events → identity.worker). Every
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
	if s.Traffic.IdentityMessagesThisTick > 0 {
		n := s.Traffic.IdentityMessagesThisTick
		bodies := makeBodies(n, "identity.check", s.Tick)
		if err := publishWithRetry(ctx, pub, "identity.events", "identity.requests", bodies); err != nil {
			appendDropped(s, "identity.events", n, err)
		} else {
			s.Sim.AddPublished("identity.worker", int(n))
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
// id so the broker UI and any later causal tracing can tell them apart, plus a
// sent-at millisecond timestamp the consumer path reads to measure the real
// publish→ack round trip (the gateway's sync-mode latency).
func makeBodies(n int64, kind string, tick int64) [][]byte {
	bodies := make([][]byte, 0, n)
	now := time.Now().UnixMilli()
	for i := int64(0); i < n; i++ {
		body := []byte(fmt.Appendf(nil, `{"id":"%s-%d-%d","kind":"%s","ts":%d}`, kind, tick, i, kind, now))
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

// applyCache writes the latest polled telemetry into the state's queues,
// reading the poller's cache without blocking. Missing entries leave the
// last-known values untouched (no zeroing). It also reconciles the load
// trackers against the authoritative real depth (client-prediction /
// server-snapshot pattern). Pool worker counts are NOT touched here — see
// syncPoolCounts.
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
		s.Sim.Reconcile(s.Queues[i].Name, cq.stats.Messages)
	}
}

// syncPoolCounts refreshes each pool's displayed worker count from the pool
// manager's configured counts (what scale_workers / run start set). The old
// source — RabbitMQ's management API "consumers" field — counts basic.consume
// subscriptions, and each WorkerPool opens exactly ONE consume then dispatches
// to N goroutines, so it always read 1 no matter how many workers were added.
// Intent is the truth here; broker telemetry only reports queue depth. It also
// mirrors the live ack mode and per-worker slot view the Inspector needs.
func syncPoolCounts(s *simulation.GameState, pm *rabbitmq.PoolManager) {
	for i := range s.Pools {
		q := s.Pools[i].Queue
		s.Pools[i].Workers = pm.Workers(q)
		s.Pools[i].AckPolicy = pm.AckPolicy(q)
		s.Pools[i].WorkerDetail = toWorkerDetail(pm.PoolReport(q))
	}
}

// toWorkerDetail adapts the pool manager's live per-worker report into the
// simulation's WorkerDetail shape (same fields, decoupled packages).
func toWorkerDetail(rep []rabbitmq.WorkerReport) []simulation.WorkerDetail {
	out := make([]simulation.WorkerDetail, 0, len(rep))
	for _, w := range rep {
		out = append(out, simulation.WorkerDetail{ID: w.ID, Msg: w.Msg})
	}
	return out
}

// pollQueues is a separate goroutine that samples the management API on a slow
// cadence and writes into the cache. Depth/consumers are authoritative; rates
// stay out (see applyRates) because mgmt counters reset on RabbitMQ's own
// retention schedule.
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
			cache.update(name, stats)
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

func (c *queueCache) update(name string, stats rabbitmq.QueueStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[name] = cachedQueue{stats: stats}
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

// logOutcome prints the terminal summary on either a fail or a successful
// completion: final metrics, survive results, and the last ~20 event-log
// entries. The generator has already stopped at this point; the frozen final
// state remains live in the server for inspection.
func logOutcome(s *simulation.GameState, out *simulation.Outcome) {
	if out.Failed {
		log.Printf("=== RUN ENDED: FAILURE — %s ===", out.FailReason)
	} else {
		log.Printf("=== RUN ENDED: SURVIVED — the 8:00 surge was ridden out ===")
	}
	if out.Failed {
		log.Printf("=== SCENARIO FAILED === reason: %s", out.FailReason)
	} else {
		log.Printf("=== SCENARIO COMPLETED (ramp survived) ===")
	}
	log.Printf("ended at t=%s (%d ticks)", out.EndedAt.Round(simulation.TickInterval), out.FinalTicks)
	log.Printf("final metrics: p50=%s p99=%s success=%.2f%% health=%.0f",
		out.FinalMetrics.LatencyP50.Round(time.Microsecond),
		out.FinalMetrics.LatencyP99.Round(time.Microsecond),
		out.FinalMetrics.SuccessRate*100,
		out.FinalMetrics.SystemHealth)
	for _, sv := range out.Survive {
		status := "met"
		if !sv.Met {
			status = "violated"
		}
		log.Printf("  objective %-24s %-8s (best streak %d/%d ticks) %s", sv.ID, status, sv.Streak, sv.EndTick, sv.Desc)
	}
	log.Printf("last %d events:", minInt(20, s.Log.Len()))
	for _, e := range s.Log.Tail(20) {
		log.Printf("  tick=%d t=%s type=%s subject=%s value=%.1f %s",
			e.Tick, e.Time.Round(simulation.TickInterval), e.Type, e.Subject, e.Value, e.Data)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// registryAdapter wraps *rabbitmq.Registry to satisfy the api.ServiceReg
// interface without importing the rabbitmq package into the api package. It
// also exposes the simulation's own GameState services so the sync/async flag
// (which lives in the simulation, not in RabbitMQ) can be toggled.
type registryAdapter struct {
	reg   *rabbitmq.Registry
	state *simulation.GameState
}

func (a *registryAdapter) Get(id string) *api.ServiceState {
	svc := a.reg.Get(id)
	if svc == nil {
		return nil
	}
	out := &api.ServiceState{
		ID:    svc.ID,
		Wired: svc.Wired,
	}
	for i := range a.state.Services {
		if a.state.Services[i].ID == id {
			out.Synchronous = a.state.Services[i].Synchronous
			break
		}
	}
	return out
}

func (a *registryAdapter) SetWired(id string, wired bool) bool {
	return a.reg.SetWired(id, wired)
}

func (a *registryAdapter) SetSynchronous(id string, sync bool) bool {
	for i := range a.state.Services {
		if a.state.Services[i].ID == id {
			if a.state.Services[i].Synchronous == sync {
				return false
			}
			a.state.Services[i].Synchronous = sync
			return true
		}
	}
	return false
}

// BudgetGate: emergency_db_failover drains the incident's failover budget and
// hands the live queue to the simulation's failover override (latency drops ~5x
// on a degraded replica with its own inconsistency + residual error). The
// budget is GameState.Budget: spending here is visible in the snapshot, and an
// exhausted budget rejects further failovers.
func (a *registryAdapter) BudgetLeft() int {
	return a.state.Budget
}

func (a *registryAdapter) SpendBudget() bool {
	if a.state.Budget <= 0 {
		return false
	}
	a.state.Budget--
	return true
}

func (a *registryAdapter) Failover(queue string) error {
	return a.state.Sim.Failover(queue, simulation.FailoverLease)
}

// syncWired refreshes each simulation service's Wired flag from the live
// registry, so snapshots (and the frontend pause/resume controls) reflect
// player pauses/resumes that the bridge applies broker-side.
func (a *registryAdapter) syncWired(s *simulation.GameState) {
	for i := range s.Services {
		svc := a.reg.Get(s.Services[i].ID)
		s.Services[i].Wired = svc != nil && svc.Wired
	}
}

// buildTimeline assembles the playable postmortem: a short, human-readable
// sequence of beats derived by filtering and reformatting the event log.
// Deliberately template-based (this is a dump, not narrative generation).
func buildTimeline(s *simulation.GameState, out *simulation.Outcome) []string {
	beats := make([]string, 0, 15)
	beats = append(beats, "Surge begins — traffic ramps from 200 to 10,000 req/s")

	var peak float64
	var peakAt string
	health70, health50 := false, false
	var t70, t50 string
	var worst70, worst50 string
	var lastStates []simulation.ServiceStateReport

	for _, e := range s.Log.After(0) {
		switch e.Type {
		case "request":
			if e.Value > peak {
				peak = e.Value
				peakAt = stamp(e.Time)
			}
		case "action":
			beats = append(beats, fmt.Sprintf("%s at %s", playerActionBeats(e.Subject, e.Data), stamp(e.Time)))
		case "state":
			// The latest per-service snapshot rides the same rail cadence as
			// the metric events, so a crossing below can quote which service
			// was actually worst at that moment.
			lastStates, _ = simulation.ParseServiceState(e.Data)
		case "metric":
			if h, ok := healthIn(e.Data); ok {
				if !health70 && h < 70 {
					health70 = true
					t70 = stamp(e.Time)
					worst70 = worstDetail(lastStates)
				}
				if !health50 && h < 50 {
					health50 = true
					t50 = stamp(e.Time)
					worst50 = worstDetail(lastStates)
				}
			}
		}
	}

	if peak >= 10000 {
		beats = append(beats, fmt.Sprintf("Traffic peaked at ~%.0f req/s (%s)", peak, peakAt))
	} else if peak >= 1000 {
		beats = append(beats, fmt.Sprintf("Traffic reached ~%.0f req/s (%s)", peak, peakAt))
	}
	if health70 {
		beats = append(beats, "System Health first dropped below 70% ("+t70+")"+worst70)
	}
	if health50 {
		beats = append(beats, "System Health first dropped below 50% ("+t50+")"+worst50)
	}

	if out.Failed {
		beats = append(beats, "FAILED — "+out.FailReason)
	} else {
		beats = append(beats, "SURVIVED — the surge held; all objectives met")
	}
	if len(beats) > 15 {
		beats = beats[:15]
	}
	return beats
}

// playerActionBeats reformats an action-log event into a narrative beat.
func playerActionBeats(action, data string) string {
	svc, delta, mode := "", "", ""
	if data != "" {
		var m map[string]any
		if json.Unmarshal([]byte(data), &m) == nil {
			if v, ok := m["service"].(string); ok {
				svc = v
			}
			if v, ok := m["delta"].(float64); ok {
				delta = fmt.Sprintf(" %+.0f", v)
			}
			if v, ok := m["mode"].(string); ok {
				mode = v
			}
		}
	}
	switch action {
	case "scale_workers":
		return "Operator scaled " + svc + " workers" + delta
	case "pause_service":
		return "Operator paused " + svc
	case "resume_service":
		return "Operator resumed " + svc
	case "set_processing_mode":
		return "Operator switched " + svc + " to " + mode + " processing"
	case "toggle_analytics":
		return "Operator toggled analytics"
	default:
		return "Operator ran " + action
	}
}

// worstDetail describes the worst service in a state snapshot the way a
// postmortem beat should: a paused service is called out as pressure
// ("paused, depth 4,200, nothing consuming") rather than staying invisible
// behind a bare percentage; a draining service gets its load/depth readout.
func worstDetail(states []simulation.ServiceStateReport) string {
	if len(states) == 0 {
		return ""
	}
	worst := &states[0]
	for i := range states {
		if states[i].Health < worst.Health {
			worst = &states[i]
		}
	}
	name := displayName(worst.ID)
	if !worst.Wired && worst.RateOut == 0 {
		if worst.RateIn > 0 {
			return fmt.Sprintf(" — %s: paused, depth %s, nothing consuming", name, fmtDepth(worst.Depth))
		}
		return fmt.Sprintf(" — %s: paused, queue idle", name)
	}
	return fmt.Sprintf(" — %s: load %.2f, depth %s", name, worst.Load, fmtDepth(worst.Depth))
}

// worstServiceAt names the lowest-health service from the live state, for the
// one-shot console floor warnings.
func worstServiceAt(s *simulation.GameState) string {
	if len(s.Services) == 0 {
		return "?"
	}
	worst := &s.Services[0]
	for i := range s.Services {
		if s.Services[i].Health < worst.Health {
			worst = &s.Services[i]
		}
	}
	name := displayName(worst.ID)
	if !worst.Wired {
		var depth int64
		for i := range s.Queues {
			if strings.HasPrefix(s.Queues[i].Name, worst.ID) {
				depth = s.Queues[i].Depth
				break
			}
		}
		return fmt.Sprintf("%s: paused, depth %s, not consuming", name, fmtDepth(depth))
	}
	return fmt.Sprintf("%s: load %.2f", name, worst.Load)
}

func displayName(id string) string {
	if n, ok := serviceDisplayNames[id]; ok {
		return n
	}
	return id
}

var serviceDisplayNames = map[string]string{
	"gateway":   "Gateway",
	"orders":    "Orders",
	"payments":  "Payments",
	"analytics": "Analytics",
}

func fmtDepth(d int64) string {
	if d >= 10000 {
		return fmt.Sprintf("%.0fk", float64(d)/1000)
	}
	if d >= 1000 {
		return fmt.Sprintf("%.1fk", float64(d)/1000)
	}
	return fmt.Sprintf("%d", d)
}

// stamp formats an elapsed duration as mm:ss for timeline beats.
func stamp(d time.Duration) string {
	s := int(d.Seconds())
	return fmt.Sprintf("%02d:%02d", s/60, s%60)
}

// healthIn extracts System Health from a rail metric event's Data string of the
// form "health=61.3 p99=412ms". Returns ok=false when absent.
func healthIn(data string) (float64, bool) {
	for _, f := range strings.Fields(data) {
		if v, ok := strings.CutPrefix(f, "health="); ok {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f, true
			}
		}
	}
	return 0, false
}
