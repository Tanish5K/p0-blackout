package simulation

import (
	"math/rand"
	"time"

	"blackout/pkg/events"
)

// GameState fully describes one moment of the simulation. Seeded RNG is stored
// on the state so that feeding a snapshot back in — same RNG, same elapsed
// time — reproduces identical output. This is for the replay/Chaos-Mode.
type GameState struct {
	Seed    int64         // 0 means "unseeded"; NewGame assigns one
	Tick    int64         // monotonic tick counter
	Elapsed time.Duration // time since run start (tick * TickInterval)

	rng     *rand.Rand
	profile TrafficProfile

	// Sim is the runtime bridge to the real broker-side workers: their load
	// trackers, latency samplers and simulated DBs. It is attached by the
	// driver (main/sim) after NewGame; metrics derive service health from it.
	Sim *Runtime

	Traffic  TrafficState
	Services []ServiceState
	Queues   []QueueState
	Pools    []PoolState
	Metrics  Metrics

	// Objectives owns the scenario objective set (state.driver uses
	// state.Objectives.Tick/Complete). Kept on the state so snapshot builders
	// can surface live objective status without a second code path.
	Objectives *Objectives

	// Outcome and Timeline are set by the driver once a fail tripped or the
	// survive window completed; the terminal snapshot serialises them.
	Outcome  *Outcome
	Timeline []string

	Log *events.Log
}

// TickInterval is the fixed simulation timestep.
const TickInterval = 100 * time.Millisecond

// IncidentSurviveDuration is the phase-5 survive window: the whole incident
// (ramp + hold) must be ridden for this long, counted from t=0. The driver
// derives Hold = IncidentSurviveDuration − Ramp so a dev-speed ramp override
// still exercises the true 8:00 objective. Per PLAN.md §7.*: "Survive 8:00".
const IncidentSurviveDuration = 8 * time.Minute

// TrafficState is the current inbound request load.
type TrafficState struct {
	RatePerSec float64 // current generated rate (incoming target)
	Accum      float64 // fractional requests carried between ticks

	// RequestsThisTick is how many inbound requests the tick decided to emit.
	RequestsThisTick int64

	// OrderMessagesThisTick / PayMessagesThisTick are this tick's publish plan —
	// a driver-internal handoff so the bridge knows what to PublishBatch. They
	// are excluded from JSON snapshots: they are not frontend display data.
	OrderMessagesThisTick int64 `json:"-"`
	PayMessagesThisTick   int64 `json:"-"`

	WinIn  []int64 // rolling window of request counts per tick (rate-in)
	WinOut []int64 // rolling window of processed counts per tick (rate-out)
}

// ServiceStatus is the coarse health label for a node.
type ServiceStatus string

const (
	StatusHealthy  ServiceStatus = "healthy"
	StatusDegraded ServiceStatus = "degraded"
	StatusStalled  ServiceStatus = "stalled"
	StatusFailed   ServiceStatus = "failed"
)

// ServiceState is one of the Lumen subsystems in the ops room.
type ServiceState struct {
	ID     string        `json:"id"`
	Load   float64       `json:"load"`   // 0..1 utilisation
	Health float64       `json:"health"` // 0..100
	Status ServiceStatus `json:"status"`

	// Wired mirrors the registry's live-MQ flag (the driver refreshes it each
	// tick after the bridge applies player pauses/resumes). It reaches the
	// frontend so pause/resume controls can show their current state.
	Wired bool `json:"wired"`

	// Synchronous marks the orders path's processing mode (Incident 1's
	// sync/async toggle). Sync = the gateway blocks on the order queue's ack;
	// async = fire-and-forget. Only the orders service toggles today; the flag
	// lives generically on ServiceState so later incidents can reuse it.
	Synchronous bool `json:"synchronous"`
}

// QueueState is one durable work/event queue.
type QueueState struct {
	Name     string  `json:"id"`
	Depth    int64   `json:"depth"`
	InFlight int64   `json:"inFlight"`
	Unacked  int64   `json:"unacked"`
	RateIn   float64 `json:"rateIn"`  // rolling window, per second
	RateOut  float64 `json:"rateOut"` // rolling window, per second
}

// PoolState is a consumer pool attached to a queue. Workers (consumer count) is
// fed from real RabbitMQ telemetry once the sim is bridged; the fake CapPerTick
// drain was removed because real consumers own how fast queues empty.
type PoolState struct {
	Queue   string `json:"queue"`
	Workers int    `json:"workers"`
}

// Metrics are the derived operational numbers surfaced to the ops rail.
type Metrics struct {
	LatencyP50   time.Duration `json:"-"`
	LatencyP99   time.Duration `json:"-"`
	SuccessRate  float64       `json:"successRate"`
	SystemHealth float64       `json:"systemHealth"`
}

// NewGame builds a fresh, seeded GameState for a profile.
func NewGame(seed int64, profile TrafficProfile) *GameState {
	if seed == 0 {
		seed = 1
	}
	win := make([]int64, 0, WindowSize)
	s := &GameState{
		Seed:    seed,
		Tick:    0,
		rng:     rand.New(rand.NewSource(seed)),
		profile: profile,
		Traffic: TrafficState{RatePerSec: profile.RateAt(0), WinIn: win, WinOut: win},
		Log:     events.NewLog(),
	}
	s.resetServices(profile)
	s.Objectives = NewObjectives()
	return s
}

// Rng returns the state's seeded RNG (nil-safe accessor).
func (s *GameState) Rng() *rand.Rand { return s.rng }

// resetServices initializes the Lumen services, queues, and worker pools that
// the simulation models. Idempotent on construction.
func (s *GameState) resetServices(profile TrafficProfile) {
	s.Services = []ServiceState{
		{ID: "gateway", Load: 0, Health: 100, Status: StatusHealthy, Wired: true},
		{ID: "orders", Load: 0, Health: 100, Status: StatusHealthy, Wired: true, Synchronous: true},
		{ID: "payments", Load: 0, Health: 100, Status: StatusHealthy, Wired: true},
		{ID: "analytics", Load: 0, Health: 100, Status: StatusHealthy, Wired: true},
	}
	s.Queues = []QueueState{
		{Name: "orders.work"},
		{Name: "analytics.events"},
		{Name: "payments.work"},
	}
	// Default pool sizes mirror the driver's defaultWorkerCounts() so the
	// pre-poll snapshot matches reality until the management poll overwrites
	// Workers with the real consumer count. PLAYTEST PASS 2: comfortable
	// starting pressure — strain builds in the final minutes, not second one.
	s.Pools = []PoolState{
		{Queue: "orders.work", Workers: 6},
		{Queue: "analytics.events", Workers: 6},
		{Queue: "payments.work", Workers: 4},
	}
	s.Metrics = Metrics{
		LatencyP50:   0,
		LatencyP99:   0,
		SuccessRate:  1.0,
		SystemHealth: 100,
	}
}
