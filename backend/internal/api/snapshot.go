package api

import (
	"encoding/json"
	"fmt"

	"blackout/internal/simulation"
	"blackout/pkg/events"
)

// Snapshot is the server-to-client state broadcast matching §4.2.
type Snapshot struct {
	Type  string `json:"type"` // always "snapshot"
	Tick  int64  `json:"tick"`
	Clock string `json:"clock"` // "02:17:34"
	Phase string `json:"phase"` // "running" | "ended"
	// RunID and RunStatus identify which run a snapshot belongs to. RunID
	// increments every start/retry; clients treat a change as a fresh run and
	// wipe their merged state (events in particular). RunStatus is
	// "idle" before the first Start and during the run-ends window, so the
	// frontend can gate the game behind a start screen.
	RunID     int64             `json:"runId"`
	RunStatus string            `json:"runStatus"` // "idle" | "running" | "ended"
	Services  []ServiceSnapshot `json:"services"`
	Queues    []QueueSnapshot   `json:"queues"`
	Pools     []PoolSnapshot    `json:"pools"`
	Metrics   MetricsSnapshot   `json:"metrics"`
	// Incident identifies the live scenario and its emergency budget. The
	// budget is Incident 2's failover allowance (spending it shows here); it
	// is always included so the client can render "2/2" without merge tricks.
	IncidentNumber int    `json:"incidentNumber"`
	IncidentName   string `json:"incidentName"`
	IncidentDesc   string `json:"incidentDesc"`
	Budget         int    `json:"budget"`
	BudgetMax      int    `json:"budgetMax"`
	// CampaignTotal is how many incidents the campaign holds (the frontend maps
	// incidentNumber → "retry" when failed, "continue to Incident N+1" after a
	// survive, "campaign complete" on the last).
	CampaignTotal int `json:"campaignTotal"`
	// Objectives is the live objective strip (fail floors + survive clock).
	Objectives []ObjectiveSnapshot `json:"objectives"`
	// Outcome appears once the run ends: terminal verdict for the postmortem.
	Outcome *OutcomeSnapshot `json:"outcome,omitempty"`
	// Events carries the whole log delta since the last broadcast (tick traffic
	// AND player action events, keyed by sequence). The client appends them to
	// its event tape and dedups by tick.
	Events []events.Event `json:"events"`
}

type ServiceSnapshot struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Load   float64 `json:"load"`
	Health float64 `json:"health"`
	Status string  `json:"status"`
	// Wired mirrors the registry's live-MQ flag: paused services read false so
	// the frontend can show pause/resume control state.
	Wired bool `json:"wired"`
	// Stalled flags a paused service whose queue is filling with nobody
	// consuming — the failure signature the rail's latency/success can't see.
	Stalled bool `json:"stalled"`
	// Synchronous reflects the orders path's processing mode; only wired
	// services toggle it, stubs always report false.
	Synchronous bool `json:"synchronous"`
}

type ObjectiveSnapshot struct {
	ID      string  `json:"id"`
	Label   string  `json:"label"`
	Role    string  `json:"role"` // "fail" | "survive"
	Current float64 `json:"current"`
	Target  float64 `json:"target"`
	Met     bool    `json:"met"`
}

type OutcomeSnapshot struct {
	Failed    bool    `json:"failed"`
	Reason    string  `json:"reason,omitempty"`
	EndedAtMs int64   `json:"endedAtMs"`
	Health    float64 `json:"health"`
	Success   float64 `json:"success"`
	P50Ms     float64 `json:"p50Ms"`
	P99Ms     float64 `json:"p99Ms"`
	// LatencyStale/SuccessStale flag terminal metrics computed with no fresh
	// completions in the sample window (e.g. pause-everything): the postmortem
	// renders those as grayed "—" instead of reporting 8ms / 100% as current.
	LatencyStale bool     `json:"latencyStale"`
	SuccessStale bool     `json:"successStale"`
	LatencyAgeMs float64  `json:"latencyAgeMs"`
	SuccessAgeMs float64  `json:"successAgeMs"`
	Timeline     []string `json:"timeline"`
}

type QueueSnapshot struct {
	ID       string  `json:"id"`
	Depth    int64   `json:"depth"`
	InFlight int64   `json:"inFlight"`
	Unacked  int64   `json:"unacked"`
	RateIn   float64 `json:"rateIn"`
	RateOut  float64 `json:"rateOut"`
}

type PoolSnapshot struct {
	Queue   string `json:"queue"`
	Workers int    `json:"workers"`
	// AckPolicy is the pool's current acknowledgement mode ("manual"|"auto").
	AckPolicy string `json:"ackPolicy"`
	// Live lists the worker slot ids currently running (one fewer than Workers
	// while a crashed slot holds). Static unless something crashes.
	Live []int `json:"live"`
}

type MetricsSnapshot struct {
	SystemHealth float64   `json:"systemHealth"`
	SuccessRate  float64   `json:"successRate"`
	LatencyMs    LatencyMs `json:"latencyMs"`
	// Stale flags + ages tell the frontend which rail numbers to gray out as
	// "no fresh samples" instead of showing frozen/fabricated readings. See
	// simulation.Metrics.
	LatencyStale bool    `json:"latencyStale"`
	SuccessStale bool    `json:"successStale"`
	LatencyAgeMs float64 `json:"latencyAgeMs"`
	SuccessAgeMs float64 `json:"successAgeMs"`
}

type LatencyMs struct {
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
}

// serviceNames maps service IDs to display names.
var serviceNames = map[string]string{
	"gateway":       "Gateway",
	"orders":        "Orders",
	"payments":      "Payments",
	"analytics":     "Analytics",
	"identity":      "Identity",
	"notifications": "Notifications",
	"audit":         "Audit",
}

// SnapshotFromState builds a Snapshot from the current GameState.
// ended should be true once the scenario outcome has fired.
func SnapshotFromState(s *simulation.GameState, ended bool) Snapshot {
	snap := Snapshot{
		Type:  "snapshot",
		Tick:  s.Tick,
		Clock: formatElapsed(s.Elapsed),
	}
	if ended {
		snap.Phase = "ended"
	} else {
		snap.Phase = "running"
	}

	snap.Services = make([]ServiceSnapshot, len(s.Services))
	for i, sv := range s.Services {
		snap.Services[i] = ServiceSnapshot{
			ID:          sv.ID,
			Name:        serviceNames[sv.ID],
			Load:        sv.Load,
			Health:      sv.Health,
			Status:      string(sv.Status),
			Wired:       sv.Wired,
			Stalled:     sv.Stalled,
			Synchronous: sv.Synchronous,
		}
	}

	// §5.2 stub nodes: Notifications and Audit are visible on the map from run
	// one but have no wired topology until their incident. Identity stops being
	// a stub the moment its incident wires it (then it lives in s.Services).
	snap.Services = append(snap.Services, stubServiceSnapshots(s.Services)...)

	snap.Queues = make([]QueueSnapshot, len(s.Queues))
	for i, q := range s.Queues {
		snap.Queues[i] = QueueSnapshot{
			ID:       q.Name,
			Depth:    q.Depth,
			InFlight: q.InFlight,
			Unacked:  q.Unacked,
			RateIn:   q.RateIn,
			RateOut:  q.RateOut,
		}
	}

	snap.Pools = make([]PoolSnapshot, len(s.Pools))
	for i, p := range s.Pools {
		snap.Pools[i] = PoolSnapshot{
			Queue:     p.Queue,
			Workers:   p.Workers,
			AckPolicy: p.AckPolicy,
			Live:      workerIDs(p.WorkerDetail),
		}
	}

	snap.IncidentNumber = s.IncidentNumber
	snap.IncidentName = s.IncidentName
	snap.IncidentDesc = s.IncidentDesc
	snap.Budget = s.Budget
	snap.BudgetMax = s.BudgetMax

	if s.Objectives != nil {
		snap.Objectives = make([]ObjectiveSnapshot, 0, 3)
		for _, st := range s.Objectives.Status(s) {
			snap.Objectives = append(snap.Objectives, ObjectiveSnapshot{
				ID:      st.ID,
				Label:   st.Label,
				Role:    st.Role,
				Current: st.Current,
				Target:  st.Target,
				Met:     st.Met,
			})
		}
	}

	if s.Outcome != nil {
		m := s.Outcome.FinalMetrics
		snap.Outcome = &OutcomeSnapshot{
			Failed:       s.Outcome.Failed,
			Reason:       s.Outcome.FailReason,
			EndedAtMs:    s.Outcome.EndedAt.Milliseconds(),
			Health:       m.SystemHealth,
			Success:      m.SuccessRate,
			P50Ms:        float64(m.LatencyP50.Microseconds()) / 1000.0,
			P99Ms:        float64(m.LatencyP99.Microseconds()) / 1000.0,
			LatencyStale: m.LatencyStale,
			SuccessStale: m.SuccessStale,
			LatencyAgeMs: m.LatencyAgeMs,
			SuccessAgeMs: m.SuccessAgeMs,
			Timeline:     s.Timeline,
		}
	}

	snap.Metrics = MetricsSnapshot{
		SystemHealth: s.Metrics.SystemHealth,
		SuccessRate:  s.Metrics.SuccessRate,
		LatencyStale: s.Metrics.LatencyStale,
		SuccessStale: s.Metrics.SuccessStale,
		LatencyAgeMs: s.Metrics.LatencyAgeMs,
		SuccessAgeMs: s.Metrics.SuccessAgeMs,
		LatencyMs: LatencyMs{
			P50: float64(s.Metrics.LatencyP50.Microseconds()) / 1000.0,
			P99: float64(s.Metrics.LatencyP99.Microseconds()) / 1000.0,
		},
	}

	return snap
}

// Delta returns a copy of cur with unchanged fields stripped.
// If prev is nil, returns cur unchanged (full snapshot).
func Delta(prev, cur *Snapshot) *Snapshot {
	if prev == nil || prev.RunID != cur.RunID {
		return cur
	}

	out := *cur // shallow copy

	// Services: only include if load/health/status changed
	if servicesEqual(prev.Services, cur.Services) {
		out.Services = nil
	}

	// Queues: only include if any numeric field changed
	if queuesEqual(prev.Queues, cur.Queues) {
		out.Queues = nil
	}

	// Pools: only include if worker counts changed
	if poolsEqual(prev.Pools, cur.Pools) {
		out.Pools = nil
	}

	// Metrics: only include if values changed
	if prev.Metrics.SystemHealth == cur.Metrics.SystemHealth &&
		prev.Metrics.SuccessRate == cur.Metrics.SuccessRate &&
		prev.Metrics.LatencyMs.P50 == cur.Metrics.LatencyMs.P50 &&
		prev.Metrics.LatencyMs.P99 == cur.Metrics.LatencyMs.P99 &&
		prev.Metrics.LatencyStale == cur.Metrics.LatencyStale &&
		prev.Metrics.SuccessStale == cur.Metrics.SuccessStale &&
		prev.Metrics.LatencyAgeMs == cur.Metrics.LatencyAgeMs &&
		prev.Metrics.SuccessAgeMs == cur.Metrics.SuccessAgeMs {
		out.Metrics = MetricsSnapshot{}
	}

	return &out
}

func servicesEqual(a, b []ServiceSnapshot) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Load != b[i].Load || a[i].Health != b[i].Health ||
			a[i].Status != b[i].Status || a[i].Wired != b[i].Wired ||
			a[i].Stalled != b[i].Stalled ||
			a[i].Synchronous != b[i].Synchronous {
			return false
		}
	}
	return true
}

func queuesEqual(a, b []QueueSnapshot) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Depth != b[i].Depth || a[i].InFlight != b[i].InFlight ||
			a[i].Unacked != b[i].Unacked || a[i].RateIn != b[i].RateIn ||
			a[i].RateOut != b[i].RateOut {
			return false
		}
	}
	return true
}

func poolsEqual(a, b []PoolSnapshot) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Workers != b[i].Workers ||
			a[i].AckPolicy != b[i].AckPolicy ||
			!intsEqual(a[i].Live, b[i].Live) {
			return false
		}
	}
	return true
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// MarshalSnapshot serialises a Snapshot to JSON bytes.
func MarshalSnapshot(snap *Snapshot) ([]byte, error) {
	return json.Marshal(snap)
}

func formatElapsed(d interface{ Seconds() float64 }) string {
	sec := int(d.Seconds())
	h := sec / 3600
	m := (sec % 3600) / 60
	s := sec % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// stubServiceSnapshots returns the idle §5.2 stub services that sit on the
// map from run one but have no wired topology. Identity is skipped once its
// incident wires it (its real ServiceSnapshot comes from s.Services).
var stubServiceIDs = []string{"identity", "notifications", "audit"}

func stubServiceSnapshots(wired []simulation.ServiceState) []ServiceSnapshot {
	live := map[string]bool{}
	for _, sv := range wired {
		live[sv.ID] = true
	}
	out := make([]ServiceSnapshot, 0, len(stubServiceIDs))
	for _, id := range stubServiceIDs {
		if live[id] {
			continue
		}
		out = append(out, ServiceSnapshot{
			ID:     id,
			Name:   serviceNames[id],
			Health: 100,
			Load:   0,
			Status: "idle",
		})
	}
	return out
}

// workerIDs extracts the ordered list of live worker slot ids from a pool's
// detail rows (a crashed slot drops out; the list stays static otherwise).
func workerIDs(detail []simulation.WorkerDetail) []int {
	out := make([]int, 0, len(detail))
	for _, w := range detail {
		out = append(out, w.ID)
	}
	return out
}
