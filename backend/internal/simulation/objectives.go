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
	ID            string
	Desc          string
	Role          ObjectiveRole
	Check         func(*GameState) bool // true = met (for survive) / violated (for fail)
	DebounceTicks int                   // consecutive ticks the condition must hold before it takes effect
}

// SurviveResult reports whether a survive objective held across the whole run.
type SurviveResult struct {
	ID      string
	Desc    string
	Met     bool
	Streak  int // longest consecutive-tick streak where it held (survive) or was violated (fail)
	EndTick int64
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

// NewObjectives builds the Incident 1 objective set.
func NewObjectives() *Objectives {
	o := &Objectives{
		failIdx:   make(map[string]int),
		streaks:   make(map[string]int),
		maxStreak: make(map[string]int),
	}
	o.items = []Objective{
		// Fail: system health held at/below the floor long enough.
		{
			ID:            "system-health-floor",
			Desc:          "System Health must not stay at or below 50 for 3 consecutive ticks",
			Role:          RoleFail,
			DebounceTicks: 3,
			Check: func(s *GameState) bool {
				return s.Metrics.SystemHealth <= 50
			},
		},
	}
	for i, it := range o.items {
		if it.Role == RoleFail {
			o.failIdx[it.ID] = i
		}
	}
	return o
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
