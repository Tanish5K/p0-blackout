package simulation

import (
	"fmt"
	"time"
)

// This file seeds the objectives / postmortem system (§13). A scenario is
// scored against a small slice of Objectives, each with a check over GameState
// and a role: fail-trigger vs survive-trigger. Objectives are evaluated once
// per tick by the driver; a high-water threshold must stay violated for several
// consecutive ticks before a fail fires, so a one-tick sampler artifact cannot
// end a run.

// ObjectiveRole says whether meeting the goal's condition is a loss (fail) or
// the thing we're trying to hold across the survive window (survive).
type ObjectiveRole int

const (
	RoleSurvive ObjectiveRole = iota
	RoleFail
)

// Objective is one scored goal for a scenario.
type Objective struct {
	ID    string
	Label string
	Desc  string
	Role  ObjectiveRole
	Check func(*GameState) bool // true = met (for survive) / violated (for fail)
	// Metric returns the live value shown in the objectives strip (health %,
	// success %, survive progress in seconds).
	Metric        func(*GameState) float64
	Target        float64
	DebounceTicks int // consecutive ticks the condition must hold before it takes effect
}

// SurviveResult reports whether a survive objective held across the whole run.
type SurviveResult struct {
	ID      string
	Desc    string
	Met     bool
	Streak  int // longest consecutive-tick streak where it held (survive) or was violated (fail)
	EndTick int64
}

// ObjectiveStatus is the live, per-objective view broadcast in every snapshot
// so the rail can show pass/fail state while the incident is running.
type ObjectiveStatus struct {
	ID      string  `json:"id"`
	Label   string  `json:"label"`
	Role    string  `json:"role"`    // "fail" | "survive"
	Current float64 `json:"current"` // live value (Metric)
	Target  float64 `json:"target"`
	Met     bool    `json:"met"`
}

// Outcome is the terminal decision of a scenario: the run ended because a fail
// objective tripped (Failed=true) or because the ramp completed without one.
// FailReason is set in the fail case. The whole Outcome is a JSON-friendly
// snapshot a later frontend/postmortem phase can render.
type Outcome struct {
	Failed       bool
	FailReason   string
	EndedAt      time.Duration
	FinalMetrics Metrics
	FinalTicks   int64
	Survive      []SurviveResult
	// PeakWorkers is the highest sum of configured pool workers reached during
	// the run — the campaign-cascade input (aggressive Incident 1 scaling is
	// paid for in Incident 2). Set by the driver before the outcome is settled.
	PeakWorkers int
}

// Objectives evaluates a set of objectives tick by tick and settles on a
// terminal outcome once one fires (fail) or the driver reports the ramp is
// complete (survive evaluation with no fail).
type Objectives struct {
	items     []Objective
	failIdx   map[string]int // objective ID -> items index (fail role)
	streaks   map[string]int // objective ID -> current consecutive-tick count
	maxStreak map[string]int // objective ID -> longest observed streak
	ended     bool
}

// NewObjectives builds the Incident 1 objective set (PLAN §7: "must meet ALL").
// System Health > 50 and Customer Success > 70 are fail floors; the incident is
// survived by riding the full 8:00 window ("Survive 8:00").
func NewObjectives() *Objectives {
	return NewObjectivesWith(baseObjectiveItems())
}

// NewObjectivesWith builds an evaluator from an explicit objective set. It is
// the incident-as-data seam: scenario authors assemble their own item list
// (aggregate floors + per-critical-service stability floors + the survive
// clock) and hand it straight to the evaluator.
func NewObjectivesWith(items []Objective) *Objectives {
	o := &Objectives{
		failIdx:   make(map[string]int),
		streaks:   make(map[string]int),
		maxStreak: make(map[string]int),
	}
	o.items = append(o.items, items...)
	for i, it := range o.items {
		if it.Role == RoleFail {
			o.failIdx[it.ID] = i
		}
	}
	return o
}

// BaseObjectiveItems exposes the canonical aggregate floors + survive set for
// scenario authors outside the package (incident specs compose their own lists
// with it, e.g. Incident 2 layers per-service floors on top).
func BaseObjectiveItems() []Objective {
	return baseObjectiveItems()
}

// baseObjectiveItems is Incident 1's canonical objective set, reused by every
// incident (incident 2 layers per-critical-service floors on top).
func baseObjectiveItems() []Objective {
	return []Objective{
		{
			ID:            "system-health-floor",
			Label:         "System Health",
			Desc:          "System Health must stay above 50%",
			Role:          RoleFail,
			DebounceTicks: 3,
			Metric:        func(s *GameState) float64 { return s.Metrics.SystemHealth },
			Target:        50,
			Check: func(s *GameState) bool {
				return s.Metrics.SystemHealth <= 50
			},
		},
		{
			ID:            "customer-success-floor",
			Label:         "Customer Success",
			Desc:          "Customer Success must stay above 70%",
			Role:          RoleFail,
			DebounceTicks: 3,
			Metric:        func(s *GameState) float64 { return s.Metrics.SuccessRate * 100 },
			Target:        70,
			Check: func(s *GameState) bool {
				return s.Metrics.SuccessRate*100 <= 70
			},
		},
		{
			ID:            "survive",
			Label:         "Survive 8:00",
			Desc:          "Ride out the full surge window",
			Role:          RoleSurvive,
			DebounceTicks: 1,
			Metric:        func(s *GameState) float64 { return s.Elapsed.Seconds() },
			Target:        IncidentSurviveDuration.Seconds(),
			// Held while the survive window has not expired. Inclusive so the
			// completing tick (elapsed == 480s exactly) still counts as held.
			Check: func(s *GameState) bool {
				return s.Elapsed <= IncidentSurviveDuration
			},
		},
	}
}

// CriticalServiceFloor is a per-critical-service uptime objective, structurally
// separate from the aggregate System Health floor: a single degraded critical
// node trips ITS OWN fail floor even when aggregate health stays above its
// line (e.g. Identity-only degradation in Incident 2).
func CriticalServiceFloor(id, label string) Objective {
	return Objective{
		ID:            id + "-health-floor",
		Label:         label,
		Desc:          label + " Health must stay above 50%",
		Role:          RoleFail,
		DebounceTicks: 3,
		Metric: func(s *GameState) float64 {
			for i := range s.Services {
				if s.Services[i].ID == id {
					return s.Services[i].Health
				}
			}
			return 100
		},
		Target: 50,
		Check: func(s *GameState) bool {
			for i := range s.Services {
				if s.Services[i].ID == id {
					return s.Services[i].Health <= 50
				}
			}
			return false
		},
	}
}

// Status returns the live objective strip for the current snapshot.
func (o *Objectives) Status(s *GameState) []ObjectiveStatus {
	out := make([]ObjectiveStatus, 0, len(o.items))
	for i := range o.items {
		it := &o.items[i]
		cur := it.Metric(s)
		role := "fail"
		if it.Role == RoleSurvive {
			role = "survive"
		}
		// Live "met": a fail floor reads met when NOT currently violated; the
		// survive clock reads met once the full window has been ridden.
		met := !it.Check(s)
		if it.Role == RoleSurvive {
			met = cur >= it.Target
		}
		out = append(out, ObjectiveStatus{
			ID: it.ID, Label: it.Label, Role: role,
			Current: cur, Target: it.Target, Met: met,
		})
	}
	return out
}

// Tick updates streak state for every objective and returns a fail outcome if
// any fail objective tripped its debounce, otherwise nil.
func (o *Objectives) Tick(s *GameState) *Outcome {
	if o.ended {
		return nil
	}
	var trigger string
	var triggerTicks int
	for i := range o.items {
		it := &o.items[i]
		cond := it.Check(s)
		if it.Role == RoleFail {
			if cond {
				o.streaks[it.ID]++
				if o.streaks[it.ID] > o.maxStreak[it.ID] {
					o.maxStreak[it.ID] = o.streaks[it.ID]
				}
				if o.streaks[it.ID] >= it.DebounceTicks {
					trigger = it.ID
					triggerTicks = o.streaks[it.ID]
				}
			} else {
				o.streaks[it.ID] = 0
			}
		} else {
			// survive objectives: track the longest streak where the condition held true
			if cond {
				o.streaks[it.ID]++
				if o.streaks[it.ID] > o.maxStreak[it.ID] {
					o.maxStreak[it.ID] = o.streaks[it.ID]
				}
			} else {
				o.streaks[it.ID] = 0
			}
		}
		if trigger != "" {
			break
		}
	}

	if trigger != "" {
		o.ended = true
		return &Outcome{
			Failed:       true,
			FailReason:   fmt.Sprintf("%s failed after %d consecutive ticks", trigger, triggerTicks),
			EndedAt:      s.Elapsed,
			FinalMetrics: s.Metrics,
			FinalTicks:   s.Tick,
		}
	}
	return nil
}

// Complete finalizes the outcome when the ramp finishes with no fail: reports
// each survive objective against its longest-held streak (a survive objective
// "met" = it held for the whole run, approximated by a streak covering every
// tick so far).
func (o *Objectives) Complete(s *GameState) *Outcome {
	o.ended = true
	out := &Outcome{
		Failed:       false,
		EndedAt:      s.Elapsed,
		FinalMetrics: s.Metrics,
		FinalTicks:   s.Tick,
	}
	for _, it := range o.items {
		if it.Role == RoleFail {
			continue
		}
		out.Survive = append(out.Survive, SurviveResult{
			ID:      it.ID,
			Desc:    it.Desc,
			Met:     o.maxStreak[it.ID] >= int(s.Tick),
			Streak:  o.maxStreak[it.ID],
			EndTick: s.Tick,
		})
	}
	return out
}
