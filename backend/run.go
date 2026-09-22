package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"blackout/internal/api"
	"blackout/internal/rabbitmq"
	"blackout/internal/simulation"
)

// RunStatus is the lifecycle state of the current incident run, broadcast to
// the game screen so the frontend can gate play behind a start screen and let
// the player retry without touching the backend.
type RunStatus string

const (
	RunIdle    RunStatus = "idle"    // awaiting the first start (or a retry prompt)
	RunRunning RunStatus = "running" // tick loop generating traffic
	RunEnded   RunStatus = "ended"   // terminal outcome broadcast; retry allowed
)

// defaultWorkerCounts returns the starting consumer count per queue for every
// incident (Incident 1 PLAYTEST PASS 2): comfortable pressure at launch so
// strain builds in the final minutes rather than from second one. Incident 2
// drops Identity on top of this set in buildSpec.
func defaultWorkerCounts() map[string]int {
	return map[string]int{
		"orders.work":      6,
		"analytics.events": 6,
		"payments.work":    4,
	}
}

// RunController owns the run lifecycle. It is the single place that starts,
// cancels, and retries a simulation run, and it guarantees old-run teardown
// COMPLETES before new-run startup: cancellation is async, so between cancelling
// a run and wiring up fresh state it waits for the run goroutine to return and
// for every old worker pool to exit. Otherwise a stray in-flight consumer
// callback from the cancelled run could touch a half-rebuilt runtime.
type RunController struct {
	// base ctx: cancelled on shutdown; every run forks a child of it.
	ctx    context.Context
	broker *rabbitmq.Broker
	pub    *rabbitmq.Publisher
	mgmt   *rabbitmq.Mgmt
	rt     *simulation.Runtime
	hub    *api.Hub
	pm     *rabbitmq.PoolManager
	reg    *rabbitmq.Registry

	mu        sync.Mutex
	runId     int64
	status    RunStatus
	cancelRun context.CancelFunc
	doneRun   chan struct{}

	// Campaign position: which incident the NEXT begin() starts. Advances on a
	// survived run; a failed run keeps the player on the same incident.
	incidentIdx int
	// dbMult is the campaign DB-failure cascade multiplier (start 1.0) seeded
	// into every DB before Incident 2: aggressive Incident 1 scaling raises it,
	// and Incident 2's DB failure chance is then seeded from it (see
	// simulation.DB.SetFailureMult). lastCascadeStep is the last bump (log).
	dbMult          float64
	lastCascadeStep float64
}

// NewRunController wires the controller to every bridge/messaging dependency.
func NewRunController(
	ctx context.Context,
	broker *rabbitmq.Broker,
	pub *rabbitmq.Publisher,
	mgmt *rabbitmq.Mgmt,
	rt *simulation.Runtime,
	hub *api.Hub,
	pm *rabbitmq.PoolManager,
	reg *rabbitmq.Registry,
) *RunController {
	return &RunController{
		ctx:    ctx,
		broker: broker,
		pub:    pub,
		mgmt:   mgmt,
		rt:     rt,
		hub:    hub,
		pm:     pm,
		reg:    reg,
		status: RunIdle,
		dbMult: 1.0,
	}
}

// currentIncident returns the campaign incident the NEXT begin() will start.
func (c *RunController) currentIncident() *Incident {
	c.mu.Lock()
	idx := c.incidentIdx
	c.mu.Unlock()
	if idx < 0 || idx >= len(campaign) {
		idx = 0
	}
	return &campaign[idx]
}

func (c *RunController) dbFailureMult() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dbMult
}

func (c *RunController) cascadeStep() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastCascadeStep
}

func (c *RunController) campLength() int {
	return len(campaign)
}

// settleLine applies the campaign-cascade rule after a run's outcome lands:
// surviving Incident 1 with peak total workers >= the cascade threshold raises
// the multiplier Incident 2's DBs are seeded from (capped), modelling "your
// scaling is what shook the shared DB loose". Expected value below the
// threshold is left untouched, so restrained play is NOT punished. Called only
// on a survived incident-1 run (the campaign advance point).
func (c *RunController) settleCascade(peak int) {
	const cascadeThreshold = 14 // peak total configured workers
	if peak < cascadeThreshold {
		return
	}
	c.mu.Lock()
	next := c.dbMult * 1.5
	if next > 2.0 {
		next = 2.0
	}
	c.lastCascadeStep = next - c.dbMult
	c.dbMult = next
	mult := c.dbMult
	c.mu.Unlock()
	log.Printf("campaign: incident 1 survived with peak %d workers >= %d → incident 2 DB cascade multiplier %.2fx (last step +%.2fx)",
		peak, cascadeThreshold, mult, c.lastCascadeStep)
}

// Boot broadcasts the idle state so a freshly-connected game screen has a
// complete snapshot (runId 0, runStatus "idle") before any run starts.
func (c *RunController) Boot() {
	c.broadcastRunFrame(0, RunIdle)
	log.Printf("simulation idle — campaign %d/%d awaiting \"start\" over /ws (set BLACKOUT_AUTOSTART=1 for boot-and-run)",
		c.currentIncident().Number, len(campaign))
}

// frozenState builds a pristine, never-ticked GameState for the idle / first
// frame broadcasts (defaults: health 100, load 0, objectives seeded). It is
// rebuilt per call so successive broadcasts never share mutation. The incident
// identity (number/name/budget) mirrors whatever campaign position is current.
func (c *RunController) frozenState() *simulation.GameState {
	inc := c.currentIncident()
	return simulation.NewGameOpts(seedFromEnv(), stampedeProfileFromEnv(), simulation.GameOptions{
		Budget:          inc.Budget,
		BudgetMax:       inc.BudgetMax,
		IncidentNumber:  inc.Number,
		IncidentName:    inc.Name,
		IncidentDesc:    inc.Desc,
		IncludeIdentity: inc.IncludeIdentity,
		DBFailureMult:   c.dbFailureMult(),
		ObjectiveItems:  inc.Objectives,
	})
}

// Start begins a run. Like Retry it errors if a run is already running (a
// fresh start mid-run would stomp the player's live state).
func (c *RunController) Start() error {
	return c.begin()
}

// Retry is an alias for Start used after a run has ended.
func (c *RunController) Retry() error {
	return c.begin()
}

// begin is the ordered restart sequence described on the type. It rejects a
// second concurrent run but happily restarts from "ended" or "idle".
func (c *RunController) begin() error {
	c.mu.Lock()
	if c.status == RunRunning {
		c.mu.Unlock()
		return errors.New("a run is already in progress")
	}
	// Reserve the next run ID and detach any prior run's handles.
	runID := c.runId + 1
	c.runId = runID
	c.status = RunRunning // provisional; confirmed below as the run launches
	cancel, done := c.cancelRun, c.doneRun
	c.cancelRun, c.doneRun = nil, nil
	c.mu.Unlock()

	spec := c.buildSpec(runID)

	// --- 1. Old-run teardown, fully COMPLETED before anything new starts. ---
	// Cancellation is async: a stray tick or consumer callback from the old
	// run must not race the new run's resets, so we cancel and then WAIT for
	// the goroutine to return before touching any shared state.
	if cancel != nil {
		cancel()
		select {
		case <-done:
			log.Printf("run controller: previous run stopped cleanly")
		case <-time.After(5 * time.Second):
			log.Printf("run controller: WARNING — previous run did not stop within 5s; proceeding anyway")
		}
	}

	// Domino order so no old worker can write into fresh state:
	//   pools first (Reset waits per-pool for handlers to finish),
	//   then runtime + registry resets,
	//   then wire this incident's new service (Incident 2: identity),
	//   then queue purge (covers the newly-wired queue too),
	//   then settle the pools at the reset worker set.
	c.pm.Reset(spec.counts)
	c.rt.Reset(simulation.RuntimeOpts{
		IncludeIdentity: spec.options.IncludeIdentity,
		FailureMult:     spec.options.DBFailureMult,
	})
	c.reg.ResetWired()
	if spec.options.IncludeIdentity {
		if !c.reg.SetWired("identity", true) {
			log.Printf("run controller: identity wiring no-op (already wired)")
		}
	}
	if err := purgeQueues(c.ctx, c.broker, c.reg.WiredQueues()); err != nil {
		log.Printf("run controller: queue purge failed (%v); continuing", err)
	}
	c.pm.Reconcile()

	// --- 2. Launch the fresh run. ------------------------------------------
	child, cancelChild := context.WithCancel(c.ctx)
	doneCh := make(chan struct{})

	c.mu.Lock()
	c.cancelRun = cancelChild
	c.doneRun = doneCh
	c.mu.Unlock()

	go func() {
		defer close(doneCh)
		out := runSimulation(child, c.pub, c.mgmt, c.rt, c.hub, c.pm, c.reg, spec)
		c.finish(runID, out)
	}()

	// Broadcast an immediate "running" frame (defaults until tick 1 lands
	// ~100ms later) so the UI leaves the start/postmortem overlay at once.
	c.broadcastRunFrame(runID, RunRunning)
	return nil
}

// finish marks the controller idle-of-run when the run launched with the given
// ID returns (either a terminal outcome broadcast by runSimulation or an
// external cancel). A newer run that already claimed the controller is left
// untouched. On a SURVIVED run the campaign advances to the next incident (and
// the cascade rule books Incident 1's scaling cost); a failed run stays put.
func (c *RunController) finish(id int64, out *simulation.Outcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != c.runId {
		return
	}
	c.status = RunEnded
	c.cancelRun = nil
	c.doneRun = nil

	if out == nil {
		return // externally cancelled; no campaign consequence
	}
	if !out.Failed && c.incidentIdx < len(campaign)-1 {
		c.settleCascade(out.PeakWorkers)
		c.incidentIdx++
		next := campaign[c.incidentIdx]
		log.Printf("campaign: incident %d \"%s\" SURVIVED → next up: %d \"%s\"",
			out.FinalTicks, campaign[c.incidentIdx-1].Name, next.Number, next.Name)
	}
}

// broadcastRunFrame sends a full, never-mutated snapshot tagged with the given
// run identity and status.
func (c *RunController) broadcastRunFrame(runID int64, st RunStatus) {
	snap := api.SnapshotFromState(c.frozenState(), false)
	snap.Phase = string(st)
	snap.RunID = runID
	snap.RunStatus = string(st)
	snap.CampaignTotal = c.campLength()
	snap.Events = nil
	if data, err := api.MarshalSnapshot(&snap); err == nil {
		c.hub.BroadcastSnapshot(data, data)
	}
}

// HandleControl dispatches {"type":"control","action":"start"|"retry"} from the
// WebSocket hub. Control is intentionally separate from player actions: a
// control message must work even mid-run teardown, when the per-run action
// handler may be mid-swap.
func (c *RunController) HandleControl(raw []byte) []byte {
	var msg struct {
		Type   string `json:"type"`
		Action string `json:"action"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil || msg.Type != "control" {
		return controlResult(false, "invalid control message")
	}
	switch msg.Action {
	case "start", "retry":
		if err := c.begin(); err != nil {
			return controlResult(false, err.Error())
		}
		return controlResult(true, "")
	default:
		return controlResult(false, "unknown control action: "+msg.Action)
	}
}

func controlResult(ok bool, err string) []byte {
	b, _ := json.Marshal(api.ActionResult{OK: ok, Error: err})
	return b
}

// purgeQueues drains leftover messages so a re-run starts clean. Guarded by
// BLACKOUT_KEEP_QUEUES=1 for anyone resuming a prior run.
func purgeQueues(ctx context.Context, broker *rabbitmq.Broker, queues []string) error {
	if os.Getenv("BLACKOUT_KEEP_QUEUES") == "1" {
		return nil
	}
	ch, err := broker.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	var failed []string
	for _, q := range queues {
		if _, err := ch.QueuePurge(q, false); err != nil {
			failed = append(failed, fmt.Sprintf("%s (%v)", q, err))
			continue
		}
		log.Printf("purged %q", q)
	}
	if len(failed) > 0 {
		return fmt.Errorf("purge failed for: %s", strings.Join(failed, ", "))
	}
	_ = ctx
	return nil
}
