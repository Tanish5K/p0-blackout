package simulation

import (
	"testing"
	"time"
)

// --- Incident 2 scaffolding: identity as a wired leg vs. a stub ------------

func TestNewGameDefaultHasNoIdentity(t *testing.T) {
	s := NewGame(42, StampedeProfile{BaseRate: 200, PeakRate: 10000, Ramp: 5 * time.Minute})
	for _, sv := range s.Services {
		if sv.ID == "identity" {
			t.Fatal("identity must not be scaffolded into Incident 1")
		}
	}
	for _, q := range s.Queues {
		if q.Name == "identity.worker" {
			t.Fatal("identity.worker must not exist in Incident 1")
		}
	}
	if s.IncludeIdentity {
		t.Fatal("IncludeIdentity must default false")
	}
}

func TestNewGameOptsIdentityScaffolding(t *testing.T) {
	s := NewGameOpts(42, StampedeProfile{BaseRate: 200, PeakRate: 10000, Ramp: 5 * time.Minute},
		GameOptions{IncidentNumber: 2, IncidentName: "Falling Workers", IncludeIdentity: true, Budget: 2, BudgetMax: 2})

	var sawIdentity bool
	for _, sv := range s.Services {
		if sv.ID == "identity" {
			sawIdentity = true
			if !sv.Wired || !sv.Synchronous {
				t.Fatalf("identity service must start wired + sync, got %+v", sv)
			}
		}
	}
	if !sawIdentity {
		t.Fatal("identity service missing from Incident 2 state")
	}
	sawQueue := false
	for _, q := range s.Queues {
		if q.Name == "identity.worker" {
			sawQueue = true
		}
	}
	if !sawQueue {
		t.Fatal("identity.worker queue missing")
	}
	sawPool := false
	for _, p := range s.Pools {
		if p.Queue == "identity.worker" {
			sawPool = true
			if p.Workers < 1 {
				t.Fatalf("identity pool must have workers, got %d", p.Workers)
			}
		}
	}
	if !sawPool {
		t.Fatal("identity.worker pool missing")
	}
	if s.Budget != 2 || s.BudgetMax != 2 {
		t.Fatalf("budget not seeded: %d/%d", s.Budget, s.BudgetMax)
	}
	if s.IncidentNumber != 2 || s.IncidentName != "Falling Workers" {
		t.Fatalf("incident identity wrong: %d %q", s.IncidentNumber, s.IncidentName)
	}
}

// --- Objective independence: a per-critical-service floor trips alone -------

func TestCriticalServiceFloorTripsIndependently(t *testing.T) {
	s := NewGameOpts(42, StampedeProfile{}, GameOptions{
		IncludeIdentity: true,
		ObjectiveItems:  []Objective{CriticalServiceFloor("identity", "Identity")},
	})
	o := s.Objectives
	var out *Outcome
	for i := 0; i < 3; i++ {
		for j := range s.Services {
			if s.Services[j].ID == "identity" {
				s.Services[j].Health = 49
			}
		}
		// Orders never degrades: the identity floor must trip on its own.
		for j := range s.Services {
			if s.Services[j].ID == "orders" {
				s.Services[j].Health = 90
			}
		}
		out = o.Tick(s)
	}
	if out == nil || !out.Failed {
		t.Fatalf("identity health floor must fail independently, got %+v", out)
	}
}

// --- DB failover: speed shift live only during the lease, residual after -----

func TestDBFailoverShapesLatencyAndFailure(t *testing.T) {
	d := NewDB(10, 1000, 5)
	baseLat := d.LatencyMS(3, 50)
	if baseLat <= 0 {
		t.Fatalf("bad baseline latency %f", baseLat)
	}
	baseFail := d.FailureChance(50)

	lease := 60 * time.Millisecond
	d.BeginFailover(lease, 0.2, 0.05, 0.01)

	// During the lease: latency ~5x faster, elevated failure (replica inconsistency).
	failoverLat := d.LatencyMS(3, 50)
	if failoverLat >= baseLat*0.5 {
		t.Fatalf("failover must cut latency ~5x, got %.2f vs base %.2f", failoverLat, baseLat)
	}
	failoverFail := d.FailureChance(50)
	// The replica inconsistency (0.05) hits the maxFail*mult ceiling (0.01), so
	// the surface reading is ~1% — still a 5x jump over the 0.2% baseline, which
	// IS the visible "imperfect fix" the rail should show.
	if failoverFail <= baseFail {
		t.Fatalf("failover must raise failure via replica inconsistency, got %f vs %f", failoverFail, baseFail)
	}
	if failoverFail > 0.011 {
		t.Fatalf("failover failure must stay bounded by the ceiling, got %f", failoverFail)
	}

	// After the lease but before 2x: residual error still present, latency back.
	time.Sleep(lease + 5*time.Millisecond)
	postLat := d.LatencyMS(3, 50)
	if postLat < baseLat*0.5 {
		t.Fatalf("latency must return to baseline after lease, got %.2f", postLat)
	}
	residFail := d.FailureChance(50)
	if residFail <= baseFail {
		t.Fatalf("residual error must linger after lease, got %f vs base %f", residFail, baseFail)
	}

	// Full 2x lease: everything back to baseline.
	time.Sleep(lease + 5*time.Millisecond)
	homeLat := d.LatencyMS(3, 50)
	if homeLat < baseLat*0.5 {
		t.Fatalf("latency must settle to baseline after residual window, got %.2f", homeLat)
	}
	homeFail := d.FailureChance(50)
	if homeFail != 0 && homeFail-0.001 > baseFail {
		t.Fatalf("failure must settle to baseline after residual window, got %f vs %f", homeFail, baseFail)
	}
}

// --- DB spike: 10x latency without touching the failure chance --------------

func TestDBSpikeIsLatencyOnly(t *testing.T) {
	d := NewDB(10, 1000, 5)
	beforeLat := d.LatencyMS(3, 50)
	beforeFail := d.FailureChance(50)

	d.SetMultiplier(10)

	spikeLat := d.LatencyMS(3, 50)
	if spikeLat < beforeLat*9 {
		t.Fatalf("spike must raise latency ~10x, got %.2f vs base %.2f", spikeLat, beforeLat)
	}
	if f := d.FailureChance(50); f != beforeFail {
		t.Fatalf("latency spike must not alter failure chance, got %f vs %f", f, beforeFail)
	}

	d.SetMultiplier(1)
	if back := d.LatencyMS(3, 50); back != beforeLat {
		t.Fatalf("multiplier reset must restore baseline latency, got %f", back)
	}
}

// --- Cascade: DB failure multiplier scales failure chance, capped ------------

func TestCascadeFailureMult(t *testing.T) {
	d := NewDB(0.6, 6000, 6) // Incident 2 orders preset
	baseFail := d.FailureChance(100)
	if baseFail <= 0 || baseFail >= 0.01 {
		t.Fatalf("baseline failure out of band, got %f", baseFail)
	}

	d.SetFailureMult(1.5) // surviving Incident 1 with hot scaling
	up := d.FailureChance(100)
	if up <= baseFail {
		t.Fatalf("cascade multiplier must raise failure chance, got %f vs %f", up, baseFail)
	}

	d.SetFailureMult(50) // absurd cascade, still bounded by maxFail*mult
	cap_ := d.FailureChance(100)
	if cap_ > d.maxFail*50 {
		t.Fatalf("failure chance must respect the ceiling, got %f", cap_)
	}
}

// --- Failover round-trip through the Runtime facade --------------------------

func TestRuntimeFailover(t *testing.T) {
	rt := NewRuntime()
	rt.Reset(RuntimeOpts{IncludeIdentity: true})

	if err := rt.SetDBSpike("identity.worker", 10); err != nil {
		t.Fatalf("set spike: %v", err)
	}
	db := rt.DBForQueue("identity.worker")
	if got := db.Multiplier(); got != 10 {
		t.Fatalf("spike not applied, multiplier %f", got)
	}

	if err := rt.Failover("identity.worker", time.Minute); err != nil {
		t.Fatalf("failover: %v", err)
	}
	if db.FailoverUntil().IsZero() {
		t.Fatal("failover lease not set")
	}

	if err := rt.Failover("nope.work", time.Minute); err == nil {
		t.Fatal("failover for unknown queue must error")
	}
}
