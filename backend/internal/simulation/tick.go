package simulation

import (
	"time"

	"blackout/pkg/events"
)

// Tick advances the simulation by dt and returns the events produced. Given the
// same GameState and dt it always produces the same result — the function is
// deterministic with respect to the state's own seeded RNG. it decides how
// much traffic to emit this tick and what to publish
func Tick(s *GameState, dt time.Duration) []events.Event {
	s.Tick++
	s.Elapsed += dt

	evs := make([]events.Event, 0, 8)
	r := s.Rng()

	// --- 1. Traffic generation (pure decision) -----------------------------
	target := s.resetTrafficRate()
	expected := target * dt.Seconds()
	// Poisson-ish variance: ±30% jitter.
	jitter := 1.0 + (r.Float64()-0.5)*0.6
	s.Traffic.Accum += expected * jitter
	reqs := int64(s.Traffic.Accum)
	s.Traffic.Accum -= float64(reqs)
	s.Traffic.RequestsThisTick = reqs
	s.Traffic.WinIn = pushWindow(s.Traffic.WinIn, reqs)

	// --- 2. Publish plan: what the driver should emit for real this tick. ---
	payFrac := 0.35 // share of orders that require a payment authorisation
	payIn := int64(float64(reqs) * payFrac)
	s.Traffic.OrderMessagesThisTick = reqs
	s.Traffic.PayMessagesThisTick = payIn

	if reqs > 0 {
		evs = append(evs, events.Event{
			Tick: s.Tick, Time: s.Elapsed, Type: "request", Subject: "gateway",
			Value: float64(reqs), Data: "inbound",
		})
		evs = append(evs, events.Event{
			Tick: s.Tick, Time: s.Elapsed, Type: "route", Subject: "order.events",
			Value: float64(reqs), Data: "orders.work analytics.events",
		})
	}
	if payIn > 0 {
		evs = append(evs, events.Event{
			Tick: s.Tick, Time: s.Elapsed, Type: "route", Subject: "payment.events",
			Value: float64(payIn), Data: "payments.work",
		})
	}

	// --- 3. Service load: proportional to the demand each subsystem routes. --
	svcLoad := map[string]float64{
		"gateway":   clamp01(float64(reqs) / 12000),
		"orders":    clamp01(float64(reqs) / 8000),
		"payments":  clamp01(float64(payIn) / 3000),
		"analytics": clamp01(float64(reqs) / 15000),
	}
	for i := range s.Services {
		sv := &s.Services[i]
		sv.Load = svcLoad[sv.ID]
		// health drifts down with sustained overload
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

	// --- 4. Derived metrics from the QueueState the bridge handed in. --------
	deriveMetrics(s)

	// Emit a metric event every N ticks so the terminal shows a moving rail.
	if s.Tick%10 == 0 {
		evs = append(evs, events.Event{
			Tick: s.Tick, Time: s.Elapsed, Type: "metric", Subject: "rail",
			Value: s.Metrics.LatencyP99.Seconds() * 1000,
			Data:  "",
		})
	}

	return evs
}

// resetTrafficRate recomputes the current generation rate from the profile and
// stores it on the state, returning it.
func (s *GameState) resetTrafficRate() float64 {
	if s.profile != nil {
		s.Traffic.RatePerSec = s.profile.RateAt(s.Elapsed)
	}
	return s.Traffic.RatePerSec
}

func classify(health float64) ServiceStatus {
	switch {
	case health <= 0:
		return StatusFailed
	case health < 30:
		return StatusStalled
	case health < 70:
		return StatusDegraded
	default:
		return StatusHealthy
	}
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
