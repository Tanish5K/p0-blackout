package simulation

import "time"

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

	// --- 2. Health drift: sustained overload degrades, recovery heals. ----
	for i := range s.Services {
		sv := &s.Services[i]
		if sv.Load > 0.9 {
			if sv.Health > 0 {
				sv.Health -= 1
			}
		} else if sv.Health < 100 && sv.Load < 0.7 {
			sv.Health += 0.5
			if sv.Health > 100 {
				sv.Health = 100
			}
		}
		sv.Status = classify(sv.Health)
	}

	// --- 3. Real latency percentiles from the worker samplers. ----------
	// End-to-end latency is the worst of the service links (an order waits on
	// its payment authorisation), so take the max p50/p99 across queues.
	var p50, p99 time.Duration
	if s.Sim != nil {
		for _, snap := range s.Sim.Snapshots() {
			if snap.P50 > p50 {
				p50 = snap.P50
			}
			if snap.P99 > p99 {
				p99 = snap.P99
			}
		}
	}
	if p50 == 0 {
		p50 = 8 * time.Millisecond // calm baseline before any work is measured
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
