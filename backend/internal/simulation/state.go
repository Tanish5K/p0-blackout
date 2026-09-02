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

	Log *events.Log
}

// TickInterval is the fixed simulation timestep.
const TickInterval = 100 * time.Millisecond

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
	ID     string
	Load   float64 // 0..1 utilisation
	Health float64 // 0..100
	Status ServiceStatus
}

// QueueState is one durable work/event queue.
type QueueState struct {
	Name     string
	Depth    int64
	InFlight int64
	Unacked  int64
	RateIn   float64 // rolling window, per second
	RateOut  float64 // rolling window, per second
}

// PoolState is a consumer pool attached to a queue. Workers (consumer count) is
// fed from real RabbitMQ telemetry once the sim is bridged; the fake CapPerTick
// drain was removed because real consumers own how fast queues empty.
type PoolState struct {
	Queue     string
	Workers   int
	Processed int64
	Failures  int64
}

// Metrics are the derived operational numbers surfaced to the ops rail.
type Metrics struct {
	LatencyP50   time.Duration
	LatencyP99   time.Duration
	SuccessRate  float64
	SystemHealth float64
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
	return s
}

// Rng returns the state's seeded RNG (nil-safe accessor).
func (s *GameState) Rng() *rand.Rand { return s.rng }

// resetServices initializes the Lumen services, queues, and worker pools that
// the simulation models. Idempotent on construction.
func (s *GameState) resetServices(profile TrafficProfile) {
	s.Services = []ServiceState{
		{ID: "gateway", Load: 0, Health: 100, Status: StatusHealthy},
		{ID: "orders", Load: 0, Health: 100, Status: StatusHealthy},
		{ID: "payments", Load: 0, Health: 100, Status: StatusHealthy},
		{ID: "analytics", Load: 0, Health: 100, Status: StatusHealthy},
	}
	s.Queues = []QueueState{
		{Name: "orders.work"},
		{Name: "analytics.events"},
		{Name: "payments.work"},
	}
	s.Pools = []PoolState{
		{Queue: "orders.work", Workers: 3},
		{Queue: "analytics.events", Workers: 2},
		{Queue: "payments.work", Workers: 2},
	}
	s.Metrics = Metrics{
		LatencyP50:   0,
		LatencyP99:   0,
		SuccessRate:  1.0,
		SystemHealth: 100,
	}
}
