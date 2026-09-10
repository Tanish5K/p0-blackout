package simulation

import (
	"testing"
	"time"
)

// testState builds a fresh GameState with a runtime attached and its services
// wired on (the default), ready for deriveMetrics to run against.
func testState(t *testing.T) *GameState {
	t.Helper()
	s := NewGame(42, StampedeProfile{
		BaseRate: 200,
		PeakRate: 10000,
		Ramp:     5 * time.Minute,
		Hold:     3 * time.Minute,
	})
	s.Sim = NewRuntime()
	return s
}

// serviceByID finds a live service state by id (not the stub snapshots — those
// live only in the api layer).
func serviceByID(t *testing.T, s *GameState, id string) *ServiceState {
	t.Helper()
	for i := range s.Services {
		if s.Services[i].ID == id {
			return &s.Services[i]
		}
	}
	t.Fatalf("service %q not found", id)
	return nil
}

// TestPausedSilentIsStale is the pause-everything repro: a service whose queue
// has inbound traffic but no consumers must read Stalled, and with no
// completions anywhere the gateway latency (sync) and cumulative success must
// both be flagged stale — and the p50==0 fallback must NOT fabricate 8ms.
func TestPausedSilentIsStale(t *testing.T) {
	s := testState(t)
	orders := serviceByID(t, s, "orders")
	orders.Wired = false
	// Inbound traffic, nobody consuming.
	s.Queues[0].RateIn = 500
	s.Queues[0].RateOut = 0

	deriveMetrics(s)

	if !orders.Stalled {
		t.Fatal("paused non-draining orders queue: expected Stalled=true")
	}
	if serviceByID(t, s, "analytics").Stalled || serviceByID(t, s, "payments").Stalled {
		t.Fatal("wired services must not read Stalled")
	}
	if !s.Metrics.LatencyStale {
		t.Fatal("empty gateway sampler in sync mode: expected LatencyStale=true")
	}
	if !s.Metrics.SuccessStale {
		t.Fatal("no completions anywhere: expected SuccessStale=true")
	}
	if s.Metrics.LatencyP50 != 0 {
		t.Fatalf("stale latency must surface as 0 (not the 8ms fallback), got %v", s.Metrics.LatencyP50)
	}
	// Nothing has ever completed in this run, so the ages read "never" (0).
	if s.Metrics.LatencyAgeMs != 0 || s.Metrics.SuccessAgeMs != 0 {
		t.Fatalf("expected zero 'never completed' ages, got latency=%v success=%v",
			s.Metrics.LatencyAgeMs, s.Metrics.SuccessAgeMs)
	}
}

// TestActiveRunNotStale is the calm healthy run: completions flowing, gateway
// samples present, queues draining — nothing flagged, real latency measured.
func TestActiveRunNotStale(t *testing.T) {
	s := testState(t)
	s.Sim.AddAcked("orders.work", 900)
	s.Sim.AddFailed("orders.work", 100)
	s.Sim.AddAcked("analytics.events", 400)
	s.Sim.RecordLatency("orders.work", 5*time.Millisecond)
	s.Sim.Gateway.Record(5 * time.Millisecond)
	for i := range s.Queues {
		s.Queues[i].RateIn = 500
		s.Queues[i].RateOut = 500
	}

	deriveMetrics(s)

	if s.Metrics.LatencyStale {
		t.Fatal("gateway sampler has samples: expected LatencyStale=false")
	}
	if s.Metrics.SuccessStale {
		t.Fatal("completions flowing: expected SuccessStale=false")
	}
	if serviceByID(t, s, "orders").Stalled {
		t.Fatal("wired draining queue must not read Stalled")
	}
	if s.Metrics.LatencyP50 <= 0 {
		t.Fatalf("expected a real measured p50, got %v", s.Metrics.LatencyP50)
	}
	if s.Metrics.SuccessRate >= 1 {
		t.Fatalf("mix of acks and failures must pull success below 1, got %v", s.Metrics.SuccessRate)
	}
}

// TestAsyncNeverLatencyStale: async mode models a fast-path latency constant,
// so it must never flag latency stale even with a silent gateway sampler.
func TestAsyncNeverLatencyStale(t *testing.T) {
	s := testState(t)
	serviceByID(t, s, "orders").Synchronous = false

	deriveMetrics(s)

	if s.Metrics.LatencyStale {
		t.Fatal("async mode latency is modeled, not measured: expected LatencyStale=false")
	}
	if s.Metrics.LatencyP50 != 1500*time.Microsecond || s.Metrics.LatencyP99 != 3*time.Millisecond {
		t.Fatalf("async fast-path constants expected, got p50=%v p99=%v",
			s.Metrics.LatencyP50, s.Metrics.LatencyP99)
	}
}

// TestMixedOnlyOrdersSilent: orders paused (gateway latency goes stale) while
// analytics still completes — success must stay fresh but latency must be flagged.
func TestMixedOnlyOrdersSilent(t *testing.T) {
	s := testState(t)
	serviceByID(t, s, "orders").Wired = false
	s.Queues[0].RateIn = 500
	s.Queues[0].RateOut = 0
	s.Sim.AddAcked("analytics.events", 300)

	deriveMetrics(s)

	if !s.Metrics.LatencyStale {
		t.Fatal("empty gateway sampler: expected LatencyStale=true even with analytics active")
	}
	if s.Metrics.SuccessStale {
		t.Fatal("analytics still completing: expected SuccessStale=false")
	}
	if !serviceByID(t, s, "orders").Stalled {
		t.Fatal("orders paused with inbound traffic: expected Stalled=true")
	}
}

// TestStateEventRoundtrip covers the B1 wire contract: the per-service "state"
// event's JSON must round-trip through ParseServiceState so the timeline can
// quote which service was worst at a health crossing and why.
func TestStateEventRoundtrip(t *testing.T) {
	s := testState(t)
	serviceByID(t, s, "orders").Wired = false
	s.Queues[0].Depth = 4200
	s.Queues[0].RateIn = 500
	s.Queues[0].RateOut = 0

	ev := stateEvent(s)
	if ev.Time != s.Elapsed || ev.Value != s.Metrics.SystemHealth {
		t.Fatalf("state event envelope mismatch: %+v", ev)
	}
	states, ok := ParseServiceState(ev.Data)
	if !ok || len(states) != len(s.Services) {
		t.Fatalf("expected %d parsed services, got %d (ok=%v)", len(s.Services), len(states), ok)
	}
	var orders *ServiceStateReport
	for i := range states {
		r := &states[i]
		if r.ID == "orders" {
			orders = r
		}
		if r.ID == "gateway" && r.Depth != 0 {
			t.Fatalf("gateway has no queue, expected depth 0: %+v", r)
		}
	}
	if orders == nil || orders.Wired || orders.Depth != 4200 || orders.RateIn != 500 || orders.RateOut != 0 {
		t.Fatalf("orders report mismatch: %+v", orders)
	}
	if _, ok := ParseServiceState("not json"); ok {
		t.Fatal("garbage data must fail ParseServiceState")
	}
}