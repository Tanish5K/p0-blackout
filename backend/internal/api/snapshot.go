package api

import (
	"encoding/json"
	"fmt"

	"blackout/internal/simulation"
	"blackout/pkg/events"
)

// Snapshot is the server-to-client state broadcast matching §4.2.
type Snapshot struct {
	Type     string            `json:"type"` // always "snapshot"
	Tick     int64             `json:"tick"`
	Clock    string            `json:"clock"` // "02:17:34"
	Phase    string            `json:"phase"` // "running" | "ended"
	Services []ServiceSnapshot `json:"services"`
	Queues   []QueueSnapshot   `json:"queues"`
	Pools    []PoolSnapshot    `json:"pools"`
	Metrics  MetricsSnapshot   `json:"metrics"`
	// Events carries only the events emitted during this tick (not the log
	// tail). The client appends them to its event tape; overlapping windows
	// would require client dedup, so this stays per-tick.
	Events []events.Event `json:"events"`
}

type ServiceSnapshot struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Load   float64 `json:"load"`
	Health float64 `json:"health"`
	Status string  `json:"status"`
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
}

type MetricsSnapshot struct {
	SystemHealth float64   `json:"systemHealth"`
	SuccessRate  float64   `json:"successRate"`
	LatencyMs    LatencyMs `json:"latencyMs"`
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
			ID:     sv.ID,
			Name:   serviceNames[sv.ID],
			Load:   sv.Load,
			Health: sv.Health,
			Status: string(sv.Status),
		}
	}

	// §5.2 stub nodes: Identity, Notifications and Audit are visible on the
	// map from run one but have no wired topology until their incident. They
	// report a static idle state so the frontend renders them without needing
	// a client-side registry.
	snap.Services = append(snap.Services, stubServiceSnapshots()...)

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
			Queue:   p.Queue,
			Workers: p.Workers,
		}
	}

	snap.Metrics = MetricsSnapshot{
		SystemHealth: s.Metrics.SystemHealth,
		SuccessRate:  s.Metrics.SuccessRate,
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
	if prev == nil || prev.Tick != cur.Tick {
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
		prev.Metrics.LatencyMs.P99 == cur.Metrics.LatencyMs.P99 {
		out.Metrics = MetricsSnapshot{}
	}

	return &out
}

func servicesEqual(a, b []ServiceSnapshot) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Load != b[i].Load || a[i].Health != b[i].Health || a[i].Status != b[i].Status {
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
		if a[i].Workers != b[i].Workers {
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

// stubServiceSnapshots returns the 3 idle §5.2 stub services that sit on the
// map from run one but have no wired topology.
var stubServiceIDs = []string{"identity", "notifications", "audit"}

func stubServiceSnapshots() []ServiceSnapshot {
	out := make([]ServiceSnapshot, len(stubServiceIDs))
	for i, id := range stubServiceIDs {
		out[i] = ServiceSnapshot{
			ID:     id,
			Name:   serviceNames[id],
			Status: "idle",
		}
	}
	return out
}
