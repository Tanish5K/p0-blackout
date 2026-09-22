package main

import (
	"time"

	"blackout/internal/simulation"
)

// Incident is one scenario in the campaign (§0 roadmap, §7 timeline). It is
// the incident-as-data seam: everything that differs between incidents except
// the traffic profile lives here, and the run controller / simulation driver
// consume the fields rather than special-casing incident numbers.
type Incident struct {
	Number int
	Name   string
	Desc   string
	// Budget / BudgetMax is the emergency-budget bank Incident 2's
	// emergency_db_failover spends (defaults: 2/run).
	Budget    int
	BudgetMax int
	// IncludeIdentity scaffolds the Identity service/queue/pool into the
	// simulation, registry, and broker topology (Incident 2's new leg).
	IncludeIdentity bool
	// Objectives overrides the incident objective set. Empty = the aggregate
	// floors + survive (Incident 1 canonical set).
	Objectives []simulation.Objective
	// Spike is Incident 2's scripted DB event. nil = no spikes.
	Spike *DBSpikeSpec
}

// DBSpikeSpec schedules periodic DB latency spikes on one queue's service plus
// a per-tick worker-crash probability while a spike is live ("DB latency spikes
// 10x, workers crash mid-processing"). The crash probability is held steady and
// the cascade multiplier multiplies the DB failure chance independently, so a
// "hot" prior Incident 1 makes the same spike bite harder.
type DBSpikeSpec struct {
	Queue           string
	LatMult         float64 // e.g. 10 → 10x baseline latency during the spike
	Duration        time.Duration
	CrashProbPerTick float64 // per 100ms-tick
	Milestones      []time.Duration // offsets into the run where spikes fire
}

// campaign is the full scenario list, in canonical order. Incident 1 is the
// phase-5 stampede; Incident 2 rides the follow-on falling-workers failure.
var campaign = []Incident{
	{
		Number:    1,
		Name:      "The Great Stampede",
		Desc:      "A traffic surge floods the order path. Ride out the 8:00 stampede — never let System Health collapse and keep Customer Success above 70%.",
		Budget:    2,
		BudgetMax: 2,
	},
	{
		Number:          2,
		Name:            "Falling Workers",
		Desc:            "The stampede's dirty secrets clear: Identity is now synchronous, and the shared DB spikes hit hard mid-surge. Hold Orders AND Identity health above 50% — or spend your 2 emergency DB failovers.",
		IncludeIdentity: true,
		Budget:          2,
		BudgetMax:       2,
		Objectives:      incident2Objectives(),
		Spike: &DBSpikeSpec{
			Queue:             "identity.worker",
			LatMult:           10,
			Duration:          10 * time.Second,
			CrashProbPerTick:  0.03,
			Milestones:        []time.Duration{2 * time.Minute, 4 * time.Minute, 6 * time.Minute},
		},
	},
}

// incident2Objectives layers per-critical-service stability floors (Orders,
// Identity) on top of the shared aggregate floors + survive clock. A single
// degraded critical node now trips its OWN fail floor — jumping the aggregate
// System Health line is no longer enough to win Incident 2.
func incident2Objectives() []simulation.Objective {
	return append(simulation.BaseObjectiveItems(),
		simulation.CriticalServiceFloor("orders", "Orders"),
		simulation.CriticalServiceFloor("identity", "Identity"),
	)
}

// runSpec is the resolved, immutable plan for one run: everything the driver
// and controller need, assembled from the current incident + env envelope.
type runSpec struct {
	runID         int64
	seed          int64
	profile       simulation.StampedeProfile
	options       simulation.GameOptions
	counts        map[string]int
	spike         *DBSpikeSpec
	campaignTotal int
}

// buildSpec resolves the current campaign position into a runnable spec.
func (c *RunController) buildSpec(runID int64) *runSpec {
	inc := c.currentIncident()
	counts := defaultWorkerCounts()
	if inc.IncludeIdentity {
		counts["identity.worker"] = 4
	}
	return &runSpec{
		runID:   runID,
		seed:    seedFromEnv(),
		profile: stampedeProfileFromEnv(),
		options: simulation.GameOptions{
			Budget:          inc.Budget,
			BudgetMax:       inc.BudgetMax,
			IncidentNumber:  inc.Number,
			IncidentName:    inc.Name,
			IncidentDesc:    inc.Desc,
			IncludeIdentity: inc.IncludeIdentity,
			DBFailureMult:   c.dbFailureMult(),
			CascadeStep:     c.cascadeStep(),
			ObjectiveItems:  inc.Objectives,
		},
		counts:        counts,
		spike:         inc.Spike,
		campaignTotal: len(campaign),
	}
}