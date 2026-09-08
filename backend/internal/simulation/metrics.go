package simulation

import (
	"math"
	"time"
)

// deriveMetrics recomputes service load, health and the operational rail from
// real worker telemetry ("published - acked" load trackers reconciled against
// the management snapshot, real per-message latency samplers, real ack/fail
// counters). No backlog math invents the p50/p99 anymore — those are measured.
//
// Sanity check against the incident objectives: at peak overload System Health
// must sag below 50 while Customer Success stays well above 70% (failures are
// rare, transient noise; the story of an incident is latency + backlog, not
// lost messages).
func deriveMetrics(s *GameState) {
	// --- 1. Service load ------------------------------------------------
	// Gateway is the pure inbound tier: load tracks offered traffic.
	// Orders/Payments/Analytics sit behind queues: load tracks pending work
	// against their DB's capacity, scaled by any incident multiplier (a 10x DB
	// latency spike saturates the service without a single new inbound message).
	reqs := s.Traffic.RatePerSec
	if s.Sim != nil {
		loads := make(map[string]float64, len(s.Services))
		for _, snap := range s.Sim.Snapshots() {
			if snap.Capacity > 0 {
				mult := snap.Multiplier
				if mult < 1 {
					mult = 1
				}
				load := (float64(snap.Pending)/snap.Capacity)*mult + (1-snap.Success)*2
				if load > loads[snap.Service] {
					loads[snap.Service] = load
				}
			}
		}
		for i := range s.Services {
			sv := &s.Services[i]
			switch sv.ID {
			case "gateway":
				sv.Load = clamp01(reqs / 12000)
				// In sync mode the gateway blocks on the order queue, so its
				// load must also track real backend pressure — otherwise the
				// "gateway slow" story hides entirely behind non-gateway load.
				// The threshold is 1s, not 0.5s: with transient messages the
				// calm-state publish→ack p99 stays in the tens of ms, so this
				// term only pinches once queues are genuinely backed up
				// (depth → thousands), never from normal broker round-trips.
				if ordersSync(s) && s.Sim != nil && s.Sim.Gateway != nil {
					gp99 := s.Sim.Gateway.Percentile(99)
					sv.Load = math.Max(sv.Load, clamp01(gp99.Seconds()/1.0))
				}
			default:
				sv.Load = clamp01(loads[sv.ID])
			}
		}
	} else {
		// No runtime attached (pre-bridge unit use): gateway-only baseline.
		for i := range s.Services {
			sv := &s.Services[i]
			if sv.ID == "gateway" {
				sv.Load = clamp01(reqs / 12000)
			} else {
				sv.Load = 0
			}
		}
	}

	// --- 2. Health drift: sustained overload degrades, recovery heals. -----
	// Drain is 0.5/tick (5/s) past a real pin (>0.95) and recovery 0.25/tick
	// (2.5/s) below 0.7. A true overload window lasts minutes, so this gives
	// the player ~10s of reaction time once a service pins instead of the old
	// 1/tick (10/s) which walked 100→50 in five seconds.
	for i := range s.Services {
		sv := &s.Services[i]
		if sv.Load > 0.95 {
			if sv.Health > 0 {
				sv.Health -= 0.5
			}
		} else if sv.Health < 100 && sv.Load < 0.7 {
			sv.Health += 0.25
			if sv.Health > 100 {
				sv.Health = 100
			}
		}
		sv.Status = classify(sv.Health)
	}

	// --- 3. End-to-end (customer-visible) latency. --------------------
	// The gateway is the only tier the customer touches. In sync mode it
	// blocks on the order queue's ack, so p50/p99 come from the real
	// publish→ack round-trip sampler (which includes queue wait). In async
	// mode the gateway returns immediately and the cost of the surge shows up
	// as backlog/depth on the queues instead of latency.
	var p50, p99 time.Duration
	if ordersSync(s) {
		if s.Sim != nil && s.Sim.Gateway != nil {
			p50 = s.Sim.Gateway.Percentile(50)
			p99 = s.Sim.Gateway.Percentile(99)
		}
		if p50 == 0 {
			p50 = 8 * time.Millisecond // calm baseline before any samples
		}
		if p99 == 0 {
			p99 = p50
		}
	} else {
		p50 = 1500 * time.Microsecond // ~1.5ms fast-path
		p99 = 3 * time.Millisecond
	}

	// --- 4. Customer success: real acks / (acks + failures), weighted over
	// the queues carrying the order flow. --------------------------------
	success := 1.0
	if s.Sim != nil {
		var ack, fail int64
		for _, snap := range s.Sim.Snapshots() {
			ack += snap.Acked
			fail += snap.Failed
		}
		if ack+fail > 0 {
			success = float64(ack) / float64(ack+fail)
		}
	}

	// --- 5. System health: worst service, layered with only mild depth pressure.
	// The old (depthMax-200)/20 penalty eroded health straight to 0 from sheer
	// backlog; System Health should hover in the 50-100 band during the
	// "endangered but not yet failed" window so "System Health > 50%" is a
	// meaningful line to defend. Depth now costs at most ~10 points, capped.
	worst := 100.0
	for _, sv := range s.Services {
		if sv.Health < worst {
			worst = sv.Health
		}
	}
	var depthMax int64
	for _, q := range s.Queues {
		if q.Depth > depthMax {
			depthMax = q.Depth
		}
	}
	health := worst
	if depthMax > 2000 {
		penalty := (float64(depthMax) - 2000) / 500.0
		if penalty > 10 {
			penalty = 10
		}
		health -= penalty
	}
	if health < 0 {
		health = 0
	}

	s.Metrics.LatencyP50 = p50
	s.Metrics.LatencyP99 = p99
	s.Metrics.SuccessRate = success
	s.Metrics.SystemHealth = health
}

// ordersSync reports whether the orders service runs its synchronous path.
func ordersSync(s *GameState) bool {
	for i := range s.Services {
		if s.Services[i].ID == "orders" {
			return s.Services[i].Synchronous
		}
	}
	return false
}
