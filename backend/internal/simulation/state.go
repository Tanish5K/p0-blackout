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

	// Budget is the incident's emergency-ability budget (Incident 2's
	// failover-fund). Spent by player actions; a live snapshot field so the
	// frontend can show how many escalations remain. BudgetMax is the starting
	// budget (spent value is Budget/BudgetMax).
	Budget   int
	BudgetMax int

	// Incident fields tag every state with the campaign position so
	// snapshots and the idle/briefing frames can identify which incident the
	// screen is preparing. Supply via GameOptions on construction.
	IncidentNumber int
	IncidentName   string
	IncidentDesc   string

	// IncludeIdentity scaffolds the Identity service/queue/pool into the state.
	// Incident 2 flips this on; Incident 1 models the four wired services only.
	IncludeIdentity bool

	// dbFailureMult is the campaign cascade floor (start 1.0): aggressive
	// Incident 1 scaling raises it, and Incident 2's DB failure chance is
	// seeded from it (see DB.SetFailureMult).
	dbFailureMult float64
	cascadeStep   float64 // how much the last run bumped the multiplier (log/verification)

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
	// IdentityMessagesThisTick is Incident 2's synchronous identity share of the
	// inbound flow (0 when Identity is not part of the incident).
	IdentityMessagesThisTick int64 `json:"-"`

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

	// Stalled flags a service whose queue is receiving messages but has no
	// consumers draining them (paused pool, backlog climbing). It is the
	// "messages entering, nobody consuming" failure signature the rail's
	// latency/success numbers cannot see — the map node uses it to show the
	// pause as pressure, not as a healthy green.
	Stalled bool `json:"stalled"`

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

// PoolState is a consumer pool attached to a queue. Workers is the CONFIGURED
// pool size (set by scale_workers / run start and sourced from the pool
// manager), not RabbitMQ's mgmt "consumers" count — one WorkerPool consumes on
// a single channel, so RabbitMQ always reports 1 regardless of how many
// goroutines actually drain the queue. The fake CapPerTick drain was removed
// because real consumers own how fast queues empty.
type PoolState struct {
	Queue   string `json:"queue"`
	Workers int    `json:"workers"`
	// AckPolicy mirrors the pool manager's per-queue acknowledgement mode
	// ("manual" | "auto"). The driver copies it in so the frontend's pool rows
	// can show/toggle it without a second source of truth.
	AckPolicy string `json:"ackPolicy"`
	// WorkerDetail is the live per-worker view (currently-held message id)
	// pulled from the pool manager, used by the Inspector's worker rows.
	WorkerDetail []WorkerDetail `json:"workerDetail"`
}

// WorkerDetail is one consumer's live view: its worker id and the message it
// is currently processing ("" = idle).
type WorkerDetail struct {
	ID  int    `json:"id"`
	Msg string `json:"msg"`
}

// Metrics are the derived operational numbers surfaced to the ops rail.
type Metrics struct {
	LatencyP50   time.Duration `json:"-"`
	LatencyP99   time.Duration `json:"-"`
	SuccessRate  float64       `json:"successRate"`
	SystemHealth float64       `json:"systemHealth"`

	// LatencyStale/SuccessStale mark metrics with no fresh completions inside
	// the sample window. A paused-everything run leaves the gateway latency
	// sampler empty and freezes the cumulative success ratio; both would read
	// as healthy ("8ms / 100%") next to a real failure. The flag lets the UI
	// render "—" instead of a confidently-wrong number. The numerics stay
	// cumulative/windowed as before — the stale flags only affect DISPLAY.
	LatencyStale  bool `json:"-"`
	SuccessStale  bool `json:"-"`

	// LatencyAgeMs/SuccessAgeMs are how long ago the underlying completions
	// last moved (0 = never in this run). The UI shows "last updated Xs ago"
	// alongside a stale flag so a blanked number reads as intent, not a bug.
	LatencyAgeMs  float64 `json:"-"`
	SuccessAgeMs  float64 `json:"-"`
}

// GameOptions carries the per-incident choices that shape a GameState beyond
// the base profile: whether Identity is scaffolded in (Incident 2), the
// emergency budget, the campaign DB-failure cascade multiplier, and the
// incident identity for snapshots/briefings.
type GameOptions struct {
	Budget          int
	BudgetMax       int
	IncidentNumber  int
	IncidentName    string
	IncidentDesc    string
	IncludeIdentity bool
	DBFailureMult   float64
	CascadeStep     float64
	// ObjectiveItems overrides the incident objective set (defaults to the
	// Incident 1 aggregate floors + survive). Incident 2 adds per-critical
	// service stability floors here.
	ObjectiveItems []Objective
}

// NewGame builds a fresh, seeded GameState for a profile (Incident 1 defaults).
func NewGame(seed int64, profile TrafficProfile) *GameState {
	return NewGameOpts(seed, profile, GameOptions{
		Budget: 2, BudgetMax: 2, IncidentNumber: 1,
		IncidentName: "The Great Stampede",
		IncidentDesc: "Ride out the stampede — a traffic surge floods the order path for 8:00.",
	})
}

// NewGameOpts builds a fresh, seeded GameState for the given options.
func NewGameOpts(seed int64, profile TrafficProfile, opts GameOptions) *GameState {
	if seed == 0 {
		seed = 1
	}
	if opts.DBFailureMult <= 0 {
		opts.DBFailureMult = 1
	}
	if opts.BudgetMax <= 0 {
		opts.BudgetMax = opts.Budget
	}
	win := make([]int64, 0, WindowSize)
	s := &GameState{
		Seed:            seed,
		Tick:            0,
		rng:             rand.New(rand.NewSource(seed)),
		profile:         profile,
		Traffic:         TrafficState{RatePerSec: profile.RateAt(0), WinIn: win, WinOut: win},
		Log:             events.NewLog(),
		Budget:          opts.Budget,
		BudgetMax:       opts.BudgetMax,
		IncidentNumber:  opts.IncidentNumber,
		IncidentName:    opts.IncidentName,
		IncidentDesc:    opts.IncidentDesc,
		IncludeIdentity: opts.IncludeIdentity,
		dbFailureMult:   opts.DBFailureMult,
		cascadeStep:     opts.CascadeStep,
	}
	s.resetServices(profile, opts)
	if len(opts.ObjectiveItems) > 0 {
		s.Objectives = NewObjectivesWith(opts.ObjectiveItems)
	} else {
		s.Objectives = NewObjectives()
	}
	return s
}

// Rng returns the state's seeded RNG (nil-safe accessor).
func (s *GameState) Rng() *rand.Rand { return s.rng }

// resetServices initializes the Lumen services, queues, and worker pools that
// the simulation models. Idempotent on construction.
func (s *GameState) resetServices(profile TrafficProfile, opts GameOptions) {
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
	if opts.IncludeIdentity {
		s.Services = append(s.Services,
			ServiceState{ID: "identity", Load: 0, Health: 100, Status: StatusHealthy, Wired: true, Synchronous: true})
		s.Queues = append(s.Queues, QueueState{Name: "identity.worker"})
	}
	// Default pool sizes mirror the driver's defaultWorkerCounts() so the first
	// frames match reality before syncPoolCounts stamps them with the pool
	// manager's authoritative counts. PLAYTEST PASS 2: comfortable starting
	// pressure — strain builds in the final minutes, not second one.
	s.Pools = []PoolState{
		{Queue: "orders.work", Workers: 6},
		{Queue: "analytics.events", Workers: 6},
		{Queue: "payments.work", Workers: 4},
	}
	if opts.IncludeIdentity {
		s.Pools = append(s.Pools, PoolState{Queue: "identity.worker", Workers: 4})
	}
	s.Metrics = Metrics{
		LatencyP50:   0,
		LatencyP99:   0,
		SuccessRate:  1.0,
		SystemHealth: 100,
	}
}
