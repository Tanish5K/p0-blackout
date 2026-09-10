package simulation

import (
	"encoding/json"
	"fmt"
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

	// --- 3. Derived metrics from the real QueueState + worker telemetry. ------
	deriveMetrics(s)

	// Emit a metric event every N ticks so the terminal shows a moving rail.
	// Data carries the health reading so the postmortem timeline can reconstruct
	// when System Health crossed its floors.
	//
	// Emitted BEFORE the metric event so a timeline walk that fires a threshold
	// crossing on a metric event has already seen the same tick's services.
	if s.Tick%10 == 0 {
		evs = append(evs, stateEvent(s))
		evs = append(evs, events.Event{
			Tick: s.Tick, Time: s.Elapsed, Type: "metric", Subject: "rail",
			Value: s.Metrics.LatencyP99.Seconds() * 1000,
			Data:  fmt.Sprintf("health=%.1f p99=%.0fms", s.Metrics.SystemHealth, float64(s.Metrics.LatencyP99.Milliseconds())),
		})
	}

	return evs
}

// ServiceStateReport is the per-service view broadcast in a "state" event.
// The timeline uses it to say which service was worst at a health crossing and
// why ("paused, depth 4,200, nothing consuming") instead of a bare percentage.
type ServiceStateReport struct {
	ID      string  `json:"id"`
	Load    float64 `json:"load"`
	Health  float64 `json:"health"`
	Depth   int64   `json:"depth"`
	Wired   bool    `json:"wired"`
	RateIn  float64 `json:"rateIn"`
	RateOut float64 `json:"rateOut"`
}

// ParseServiceState decodes a "state" event's JSON Data into its per-service
// reports. Returns ok=false for anything that isn't a state event.
func ParseServiceState(data string) ([]ServiceStateReport, bool) {
	var m struct {
		Services []ServiceStateReport `json:"services"`
	}
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return nil, false
	}
	return m.Services, true
}

// stateEvent snapshots every service's live state into a JSON "state" event.
func stateEvent(s *GameState) events.Event {
	reports := make([]ServiceStateReport, 0, len(s.Services))
	for i := range s.Services {
		sv := &s.Services[i]
		report := ServiceStateReport{
			ID:     sv.ID,
			Load:   sv.Load,
			Health: sv.Health,
			Wired:  sv.Wired,
		}
		for j := range s.Queues {
			if serviceForQueue(s.Queues[j].Name) != sv.ID {
				continue
			}
			report.Depth = s.Queues[j].Depth
			report.RateIn = s.Queues[j].RateIn
			report.RateOut = s.Queues[j].RateOut
			break
		}
		reports = append(reports, report)
	}
	data, _ := json.Marshal(struct {
		Services []ServiceStateReport `json:"services"`
	}{Services: reports})
	return events.Event{
		Tick: s.Tick, Time: s.Elapsed, Type: "state", Subject: "system",
		Value: s.Metrics.SystemHealth,
		Data:  string(data),
	}
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
