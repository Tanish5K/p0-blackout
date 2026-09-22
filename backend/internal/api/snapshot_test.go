package api

import (
	"testing"
	"time"

	"blackout/internal/simulation"
)

func testState() *simulation.GameState {
	return simulation.NewGame(42, simulation.StampedeProfile{
		BaseRate: 200,
		PeakRate: 10000,
		Ramp:     5 * time.Minute,
		Hold:     3 * time.Minute,
	})
}

func TestSnapshotSevenServices(t *testing.T) {
	state := testState()
	for i := 0; i < 10; i++ {
		simulation.Tick(state, simulation.TickInterval)
	}
	snap := SnapshotFromState(state, false)

	if len(snap.Services) != 7 {
		t.Fatalf("expected 7 services on the map, got %d", len(snap.Services))
	}

	// The 3 stub services must read as idle, flat at health 100 / load 0, and
	// never show a processing-mode flag.
	for _, id := range []string{"identity", "notifications", "audit"} {
		var s *ServiceSnapshot
		for i := range snap.Services {
			if snap.Services[i].ID == id {
				s = &snap.Services[i]
				break
			}
		}
		if s == nil {
			t.Fatalf("missing stub service %q", id)
		}
		if s.Health != 100 || s.Load != 0 || s.Status != "idle" || s.Synchronous {
			t.Errorf("stub %q: want Health=100 Load=0 Status=idle Synchronous=false, got %+v", id, s)
		}
	}
}

func TestSnapshotOrdersSyncDefault(t *testing.T) {
	state := testState()
	simulation.Tick(state, simulation.TickInterval)
	snap := SnapshotFromState(state, false)

	var orders *ServiceSnapshot
	for i := range snap.Services {
		if snap.Services[i].ID == "orders" {
			orders = &snap.Services[i]
			break
		}
	}
	if orders == nil {
		t.Fatal("missing orders service")
	}
	if !orders.Synchronous {
		t.Fatal("orders must report Synchronous=true by default (Incident 1 ^)")
	}
}

func TestSnapshotThreeObjectives(t *testing.T) {
	state := testState()
	for i := 0; i < 5; i++ {
		simulation.Tick(state, simulation.TickInterval)
	}
	snap := SnapshotFromState(state, false)

	if len(snap.Objectives) != 3 {
		t.Fatalf("expected 3 live objectives, got %d", len(snap.Objectives))
	}
	want := map[string]string{
		"system-health-floor":    "fail",
		"customer-success-floor": "fail",
		"survive":                "survive",
	}
	for _, o := range snap.Objectives {
		role, ok := want[o.ID]
		if !ok {
			t.Errorf("unexpected objective %q", o.ID)
			continue
		}
		if o.Role != role {
			t.Errorf("objective %q: want role %s, got %s", o.ID, role, o.Role)
		}
		if o.Target <= 0 {
			t.Errorf("objective %q: target must be positive, got %v", o.ID, o.Target)
		}
	}
}

func TestSnapshotOutcomePresentWhenEnded(t *testing.T) {
	state := testState()
	for i := 0; i < 5; i++ {
		simulation.Tick(state, simulation.TickInterval)
	}
	state.Outcome = &simulation.Outcome{
		Failed:       true,
		FailReason:   "system-health-floor failed after 3 consecutive ticks",
		EndedAt:      3 * time.Second,
		FinalMetrics: state.Metrics,
		FinalTicks:   state.Tick,
	}
	state.Timeline = []string{"beat one", "beat two"}

	snap := SnapshotFromState(state, true)
	if snap.Phase != "ended" {
		t.Fatalf("want phase ended, got %q", snap.Phase)
	}
	if snap.Outcome == nil {
		t.Fatal("expected outcome in ended snapshot")
	}
	if !snap.Outcome.Failed || snap.Outcome.Reason == "" {
		t.Errorf("outcome mismatch: %+v", snap.Outcome)
	}
	if len(snap.Outcome.Timeline) != 2 {
		t.Errorf("want 2 timeline beats, got %d: %v", len(snap.Outcome.Timeline), snap.Outcome.Timeline)
	}
	if snap.Outcome.Health != state.Metrics.SystemHealth {
		t.Errorf("outcome health %v != final health %v", snap.Outcome.Health, state.Metrics.SystemHealth)
	}
}

func TestSnapshotIncludesAckPolicy(t *testing.T) {
	state := simulation.NewGame(42, simulation.ConstantProfile{Rate: 1})
	state.Pools = []simulation.PoolState{{Queue: "orders.work", Workers: 3, AckPolicy: "auto"}}
	snapshot := SnapshotFromState(state, false)
	if got := snapshot.Pools[0].AckPolicy; got != "auto" {
		t.Fatalf("ackPolicy = %q, want auto", got)
	}
}

func TestDeltaUsesRunIdentityNotTick(t *testing.T) {
	prev := &Snapshot{RunID: 7, Tick: 10, Services: []ServiceSnapshot{{ID: "orders", Health: 100}}}
	cur := &Snapshot{RunID: 7, Tick: 11, Services: []ServiceSnapshot{{ID: "orders", Health: 100}}}
	delta := Delta(prev, cur)
	if delta.Services != nil {
		t.Fatalf("unchanged services should be omitted across ticks: %#v", delta.Services)
	}

	cur.RunID = 8
	if got := Delta(prev, cur); got != cur {
		t.Fatal("a new run must receive the complete snapshot")
	}
}

func TestPoolDeltaIncludesAckPolicyChange(t *testing.T) {
	prev := &Snapshot{RunID: 1, Pools: []PoolSnapshot{{Queue: "orders.work", Workers: 2, AckPolicy: "manual"}}}
	cur := &Snapshot{RunID: 1, Pools: []PoolSnapshot{{Queue: "orders.work", Workers: 2, AckPolicy: "auto"}}}
	if got := Delta(prev, cur).Pools; len(got) != 1 || got[0].AckPolicy != "auto" {
		t.Fatalf("ack policy change missing from delta: %#v", got)
	}
}
