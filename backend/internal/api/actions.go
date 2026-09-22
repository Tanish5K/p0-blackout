package api

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"blackout/pkg/events"
)

// IncomingMessage is the client-to-server action envelope (§4.2).
type IncomingMessage struct {
	Type    string          `json:"type"` // must be "action"
	Action  string          `json:"action"`
	Payload json.RawMessage `json:"payload"`
}

// ActionResult is the server-to-client response for a player action.
type ActionResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// PoolScaler abstracts the PoolManager methods the action handlers need.
type PoolScaler interface {
	Scale(queue string, workers int) error
	Workers(queue string) int
	// SetAckPolicy / AckPolicy expose Incident 2's manual-vs-auto ack control.
	SetAckPolicy(queue, policy string) error
	AckPolicy(queue string) string
	// RestartWorker / RestartRandomWorker crash one consumer and the pool
	// manager credits the deliberate kill on the current session.
	RestartWorker(queue string, workerID int) error
	RestartRandomWorker(queue string) error
}

// BudgetGate abstracts the incident's emergency-ability budget (Incident 2):
// the player has a small number of emergency_db_failover uses per run, and the
// adapter (backed by GameState.Budget) is the single source of truth for
// spend + exhaustion.
type BudgetGate interface {
	BudgetLeft() int
	SpendBudget() bool
	Failover(queue string) error
}

// ServiceReg abstracts the Registry methods the action handlers need.
type ServiceReg interface {
	Get(id string) *ServiceState
	SetWired(id string, wired bool) bool
	// SetSynchronous toggles the orders path's processing mode (Incident 1's
	// sync/async control). The simulation owns the flag; the adapter maps it.
	SetSynchronous(id string, sync bool) bool
}

// ServiceState is the minimal service view the action handler reads.
type ServiceState struct {
	ID      string
	Wired   bool
	Workers int // for scale_workers delta calculation
	// Synchronous mirrors the simulation's processing-mode flag so the handler
	// can decide the "already toggled" case before calling SetSynchronous.
	Synchronous bool
}

// ActionHandler processes a raw player action message and returns a JSON response.
type ActionHandler func(raw []byte) []byte

// NewActionHandler builds an ActionHandler wired to the given pool manager,
// registry, budget gate, event log, and tick source. The returned function is
// safe to pass directly to hub.SetActionHandler.
func NewActionHandler(
	pm PoolScaler,
	reg ServiceReg,
	budget BudgetGate,
	log *events.Log,
	tick func() int64,
) ActionHandler {
	// Dispatch table: action name → handler function.
	type handlerFn func(IncomingMessage) ActionResult
	handlers := map[string]handlerFn{
		"scale_workers":         handleScaleWorkers(pm),
		"pause_service":         handlePauseService(reg),
		"resume_service":        handleResumeService(reg),
		"toggle_analytics":      handleToggleAnalytics(reg),
		"restart_worker":        handleRestartWorker(pm),
		"set_ack_policy":        handleSetAckPolicy(pm),
		"set_retry_policy":      stubHandler("set_retry_policy"),
		"route_to_dlq":          stubHandler("route_to_dlq"),
		"set_exchange_type":     stubHandler("set_exchange_type"),
		"add_binding":           stubHandler("add_binding"),
		"remove_binding":        stubHandler("remove_binding"),
		"set_priority":          stubHandler("set_priority"),
		"set_processing_mode":   handleSetProcessingMode(reg),
		"use_freeze_frame":      stubHandler("use_freeze_frame"),
		"emergency_db_failover": handleDBFailover(budget),
	}

	var mu sync.Mutex // protects event log append
	return func(raw []byte) []byte {
		var msg IncomingMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			return marshalResult(ActionResult{OK: false, Error: "malformed message"})
		}
		if msg.Type != "action" {
			return marshalResult(ActionResult{OK: false, Error: "unknown message type"})
		}

		fn, ok := handlers[msg.Action]
		if !ok {
			return marshalResult(ActionResult{OK: false, Error: fmt.Sprintf("unknown action: %s", msg.Action)})
		}

		result := fn(msg)

		// Log every valid action to the event log.
		if result.OK {
			mu.Lock()
			log.Append(events.Event{
				Tick:    tick(),
				Time:    time.Duration(tick()) * 100 * time.Millisecond,
				Type:    "action",
				Subject: msg.Action,
				Data:    string(msg.Payload),
			})
			mu.Unlock()
		}

		return marshalResult(result)
	}
}

func marshalResult(r ActionResult) []byte {
	b, _ := json.Marshal(r)
	return b
}

// --- Live action handlers ---

func handleScaleWorkers(pm PoolScaler) func(IncomingMessage) ActionResult {
	type payload struct {
		Service string `json:"service"`
		Delta   int    `json:"delta"`
	}
	return func(msg IncomingMessage) ActionResult {
		var p payload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return ActionResult{OK: false, Error: "invalid payload"}
		}
		if p.Service == "" {
			return ActionResult{OK: false, Error: "service is required"}
		}
		queue := serviceToQueue(p.Service)
		if queue == "" {
			return ActionResult{OK: false, Error: fmt.Sprintf("unknown service: %s", p.Service)}
		}
		current := pm.Workers(queue)
		next := current + p.Delta
		if next < 1 {
			next = 1
		}
		if next > 20 {
			next = 20
		}
		if err := pm.Scale(queue, next); err != nil {
			return ActionResult{OK: false, Error: err.Error()}
		}
		log.Printf("action: scale_workers %s %d→%d workers", p.Service, current, next)
		return ActionResult{OK: true}
	}
}

func handlePauseService(reg ServiceReg) func(IncomingMessage) ActionResult {
	type payload struct {
		Service string `json:"service"`
	}
	return func(msg IncomingMessage) ActionResult {
		var p payload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return ActionResult{OK: false, Error: "invalid payload"}
		}
		if p.Service == "" {
			return ActionResult{OK: false, Error: "service is required"}
		}
		svc := reg.Get(p.Service)
		if svc == nil {
			return ActionResult{OK: false, Error: fmt.Sprintf("unknown service: %s", p.Service)}
		}
		if !svc.Wired {
			return ActionResult{OK: false, Error: fmt.Sprintf("service %s is not wired", p.Service)}
		}
		reg.SetWired(p.Service, false)
		log.Printf("action: pause_service %s", p.Service)
		return ActionResult{OK: true}
	}
}

func handleResumeService(reg ServiceReg) func(IncomingMessage) ActionResult {
	type payload struct {
		Service string `json:"service"`
	}
	return func(msg IncomingMessage) ActionResult {
		var p payload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return ActionResult{OK: false, Error: "invalid payload"}
		}
		if p.Service == "" {
			return ActionResult{OK: false, Error: "service is required"}
		}
		svc := reg.Get(p.Service)
		if svc == nil {
			return ActionResult{OK: false, Error: fmt.Sprintf("unknown service: %s", p.Service)}
		}
		if svc.Wired {
			return ActionResult{OK: false, Error: fmt.Sprintf("service %s is already running", p.Service)}
		}
		reg.SetWired(p.Service, true)
		log.Printf("action: resume_service %s", p.Service)
		return ActionResult{OK: true}
	}
}

func handleToggleAnalytics(reg ServiceReg) func(IncomingMessage) ActionResult {
	return func(msg IncomingMessage) ActionResult {
		svc := reg.Get("analytics")
		if svc == nil {
			return ActionResult{OK: false, Error: "analytics service not found"}
		}
		reg.SetWired("analytics", !svc.Wired)
		state := "paused"
		if !svc.Wired {
			state = "resumed"
		}
		log.Printf("action: toggle_analytics → %s", state)
		return ActionResult{OK: true}
	}
}

// handleSetProcessingMode toggles the orders service between its synchronous
// (Incident 1 ^) and asynchronous (fast-path) processing modes. Orders-only by
// contract: the payload defaults service=orders and the handler rejects any
// other service, keeping the mode toggle a single visible control.
func handleSetProcessingMode(reg ServiceReg) func(IncomingMessage) ActionResult {
	type payload struct {
		Service string `json:"service"`
		Mode    string `json:"mode"`
	}
	return func(msg IncomingMessage) ActionResult {
		var p payload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return ActionResult{OK: false, Error: "invalid payload"}
		}
		if p.Service == "" {
			p.Service = "orders"
		}
		if p.Service != "orders" {
			return ActionResult{OK: false, Error: fmt.Sprintf("processing mode only applies to orders")}
		}
		if p.Mode != "sync" && p.Mode != "async" {
			return ActionResult{OK: false, Error: "mode must be \"sync\" or \"async\""}
		}
		svc := reg.Get(p.Service)
		if svc == nil {
			return ActionResult{OK: false, Error: fmt.Sprintf("unknown service: %s", p.Service)}
		}
		if !svc.Wired {
			return ActionResult{OK: false, Error: fmt.Sprintf("service %s is not wired", p.Service)}
		}
		want := p.Mode == "sync"
		if svc.Synchronous == want {
			return ActionResult{OK: false, Error: fmt.Sprintf("orders already running in %s mode", p.Mode)}
		}
		if !reg.SetSynchronous(p.Service, want) {
			return ActionResult{OK: false, Error: "failed to switch processing mode"}
		}
		log.Printf("action: set_processing_mode orders → %s", p.Mode)
		return ActionResult{OK: true}
	}
}

// --- Stub action handlers ---

func stubHandler(name string) func(IncomingMessage) ActionResult {
	return func(msg IncomingMessage) ActionResult {
		log.Printf("action: %s (stub, no-op)", name)
		return ActionResult{OK: true}
	}
}

// handleRestartWorker deliberately crashes a running worker (Incident 2's
// restart_worker control). It costs the slot ~30s of throughput; a crashed
// in-flight manual-ack message redelivers, in auto mode it is lost (credited).
func handleRestartWorker(pm PoolScaler) func(IncomingMessage) ActionResult {
	type payload struct {
		Service  string `json:"service"`
		WorkerID *int   `json:"workerId"`
	}
	return func(msg IncomingMessage) ActionResult {
		var p payload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return ActionResult{OK: false, Error: "invalid payload"}
		}
		if p.Service == "" {
			return ActionResult{OK: false, Error: "service is required"}
		}
		queue := serviceToQueue(p.Service)
		if queue == "" {
			return ActionResult{OK: false, Error: fmt.Sprintf("unknown service: %s", p.Service)}
		}
		var err error
		if p.WorkerID != nil {
			err = pm.RestartWorker(queue, *p.WorkerID)
		} else {
			err = pm.RestartRandomWorker(queue)
		}
		if err != nil {
			return ActionResult{OK: false, Error: err.Error()}
		}
		log.Printf("action: restart_worker %s", p.Service)
		return ActionResult{OK: true}
	}
}

// handleSetAckPolicy toggles a queue between manual and auto acknowledgement
// (Incident 2's most important control): manual keeps a crash redeliverable,
// auto makes lost messages provable but gone. Switching rebuilds the consumer.
func handleSetAckPolicy(pm PoolScaler) func(IncomingMessage) ActionResult {
	type payload struct {
		Queue  string `json:"queue"`
		Policy string `json:"policy"`
	}
	return func(msg IncomingMessage) ActionResult {
		var p payload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return ActionResult{OK: false, Error: "invalid payload"}
		}
		if p.Queue == "" {
			return ActionResult{OK: false, Error: "queue is required"}
		}
		if err := pm.SetAckPolicy(p.Queue, p.Policy); err != nil {
			return ActionResult{OK: false, Error: err.Error()}
		}
		log.Printf("action: set_ack_policy %s → %s", p.Queue, p.Policy)
		return ActionResult{OK: true}
	}
}

// handleDBFailover shifts a service's DB onto an emergency replica for a fixed
// lease: latency collapses ~5x, the replica surfaces its own inconsistency
// rate for the lease, then a residual error rate lingers after — an imperfect,
// budgeted fix. Each use drains the incident's emergency budget (2/run).
func handleDBFailover(budget BudgetGate) func(IncomingMessage) ActionResult {
	type payload struct {
		Service string `json:"service"`
	}
	return func(msg IncomingMessage) ActionResult {
		var p payload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return ActionResult{OK: false, Error: "invalid payload"}
		}
		if p.Service == "" {
			return ActionResult{OK: false, Error: "service is required"}
		}
		queue := serviceToQueue(p.Service)
		if queue == "" {
			return ActionResult{OK: false, Error: fmt.Sprintf("unknown service: %s", p.Service)}
		}
		if budget.BudgetLeft() <= 0 {
			return ActionResult{OK: false, Error: "no emergency-budget remaining" +
				" (2 DB failovers per incident; the spikes return without it)"}
		}
		if !budget.SpendBudget() {
			return ActionResult{OK: false, Error: "emergency-budget already exhausted"}
		}
		if err := budget.Failover(queue); err != nil {
			return ActionResult{OK: false, Error: err.Error()}
		}
		log.Printf("action: emergency_db_failover %s (budget left %d)", p.Service, budget.BudgetLeft())
		return ActionResult{OK: true}
	}
}

// serviceToQueue maps a service ID to its primary work queue.
func serviceToQueue(service string) string {
	switch service {
	case "orders":
		return "orders.work"
	case "payments":
		return "payments.work"
	case "analytics":
		return "analytics.events"
	case "identity":
		return "identity.worker"
	default:
		return ""
	}
}
