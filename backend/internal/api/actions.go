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
}

// ServiceReg abstracts the Registry methods the action handlers need.
type ServiceReg interface {
	Get(id string) *ServiceState
	SetWired(id string, wired bool) bool
}

// ServiceState is the minimal service view the action handler reads.
type ServiceState struct {
	ID      string
	Wired   bool
	Workers int // for scale_workers delta calculation
}

// ActionHandler processes a raw player action message and returns a JSON response.
type ActionHandler func(raw []byte) []byte

// NewActionHandler builds an ActionHandler wired to the given pool manager,
// registry, event log, and tick source. The returned function is safe to pass
// directly to hub.SetActionHandler.
func NewActionHandler(
	pm PoolScaler,
	reg ServiceReg,
	log *events.Log,
	tick func() int64,
) ActionHandler {
	// Dispatch table: action name → handler function.
	type handlerFn func(IncomingMessage) ActionResult
	handlers := map[string]handlerFn{
		"scale_workers":       handleScaleWorkers(pm),
		"pause_service":       handlePauseService(reg),
		"resume_service":      handleResumeService(reg),
		"toggle_analytics":    handleToggleAnalytics(reg),
		"restart_worker":      stubHandler("restart_worker"),
		"set_ack_policy":      stubHandler("set_ack_policy"),
		"set_retry_policy":    stubHandler("set_retry_policy"),
		"route_to_dlq":        stubHandler("route_to_dlq"),
		"set_exchange_type":   stubHandler("set_exchange_type"),
		"add_binding":         stubHandler("add_binding"),
		"remove_binding":      stubHandler("remove_binding"),
		"set_priority":        stubHandler("set_priority"),
		"set_processing_mode": stubHandler("set_processing_mode"),
		"use_freeze_frame":    stubHandler("use_freeze_frame"),
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

// --- Stub action handlers ---

func stubHandler(name string) func(IncomingMessage) ActionResult {
	return func(msg IncomingMessage) ActionResult {
		log.Printf("action: %s (stub, no-op)", name)
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
	default:
		return ""
	}
}
