package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"blackout/internal/rabbitmq"
	"blackout/pkg/events"
)

// ---------------------------------------------------------------------------
// Fakes for unit tests
// ---------------------------------------------------------------------------

type fakePool struct {
	counts map[string]int
}

func newFakePool() *fakePool {
	return &fakePool{counts: map[string]int{
		"orders.work":      3,
		"analytics.events": 2,
		"payments.work":    2,
	}}
}

func (f *fakePool) Scale(queue string, workers int) error {
	if _, ok := f.counts[queue]; !ok {
		return fmt.Errorf("unknown queue %q", queue)
	}
	f.counts[queue] = workers
	return nil
}

func (f *fakePool) Workers(queue string) int {
	return f.counts[queue]
}

type fakeReg struct {
	services map[string]*ServiceState
}

func newFakeReg() *fakeReg {
	return &fakeReg{services: map[string]*ServiceState{
		"orders":        {ID: "orders", Wired: true, Synchronous: true},
		"payments":      {ID: "payments", Wired: true},
		"analytics":     {ID: "analytics", Wired: true},
		"gateway":       {ID: "gateway", Wired: true},
		"identity":      {ID: "identity", Wired: false},
		"notifications": {ID: "notifications", Wired: false},
		"audit":         {ID: "audit", Wired: false},
	}}
}

func (f *fakeReg) Get(id string) *ServiceState {
	svc, ok := f.services[id]
	if !ok {
		return nil
	}
	return svc
}

func (f *fakeReg) SetWired(id string, wired bool) bool {
	svc, ok := f.services[id]
	if !ok {
		return false
	}
	if svc.Wired == wired {
		return false
	}
	svc.Wired = wired
	return true
}

func (f *fakeReg) SetSynchronous(id string, sync bool) bool {
	svc, ok := f.services[id]
	if !ok {
		return false
	}
	if svc.Synchronous == sync {
		return false
	}
	svc.Synchronous = sync
	return true
}

func newTestHandler(pm PoolScaler, reg ServiceReg) ActionHandler {
	log := events.NewLog()
	tick := int64(42)
	return NewActionHandler(pm, reg, log, func() int64 { return tick })
}

func send(h ActionHandler, msg IncomingMessage) ActionResult {
	raw, _ := json.Marshal(msg)
	var r ActionResult
	json.Unmarshal(h(raw), &r)
	return r
}

// ---------------------------------------------------------------------------
// Unit tests — no deps
// ---------------------------------------------------------------------------

func TestMalformedJSON(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	raw := []byte("{not json")
	var r ActionResult
	json.Unmarshal(h(raw), &r)
	if r.OK || r.Error != "malformed message" {
		t.Fatalf("expected malformed message error, got %+v", r)
	}
}

func TestWrongMessageType(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	r := send(h, IncomingMessage{Type: "snapshot"})
	if r.OK || r.Error != "unknown message type" {
		t.Fatalf("expected unknown message type, got %+v", r)
	}
}

func TestUnknownAction(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	r := send(h, IncomingMessage{Type: "action", Action: "bogus"})
	if r.OK || r.Error != "unknown action: bogus" {
		t.Fatalf("expected unknown action error, got %+v", r)
	}
}

func TestScaleWorkersValid(t *testing.T) {
	pm := newFakePool()
	h := newTestHandler(pm, newFakeReg())

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "scale_workers",
		Payload: json.RawMessage(`{"service":"orders","delta":2}`),
	})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if got := pm.Workers("orders.work"); got != 5 {
		t.Fatalf("expected 5 workers, got %d", got)
	}
}

func TestScaleWorkersNegative(t *testing.T) {
	pm := newFakePool()
	h := newTestHandler(pm, newFakeReg())

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "scale_workers",
		Payload: json.RawMessage(`{"service":"orders","delta":-5}`),
	})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if got := pm.Workers("orders.work"); got != 1 {
		t.Fatalf("expected clamped to 1, got %d", got)
	}
}

func TestScaleWorkersOverMax(t *testing.T) {
	pm := newFakePool()
	h := newTestHandler(pm, newFakeReg())

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "scale_workers",
		Payload: json.RawMessage(`{"service":"orders","delta":30}`),
	})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if got := pm.Workers("orders.work"); got != 20 {
		t.Fatalf("expected clamped to 20, got %d", got)
	}
}

func TestScaleWorkersUnknownService(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "scale_workers",
		Payload: json.RawMessage(`{"service":"gateway","delta":1}`),
	})
	if r.OK || r.Error != "unknown service: gateway" {
		t.Fatalf("expected unknown service error, got %+v", r)
	}
}

func TestScaleWorkersEmptyService(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "scale_workers",
		Payload: json.RawMessage(`{"service":"","delta":1}`),
	})
	if r.OK || r.Error != "service is required" {
		t.Fatalf("expected service required error, got %+v", r)
	}
}

func TestPauseServiceOK(t *testing.T) {
	reg := newFakeReg()
	h := newTestHandler(newFakePool(), reg)

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "pause_service",
		Payload: json.RawMessage(`{"service":"analytics"}`),
	})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if reg.services["analytics"].Wired {
		t.Fatal("expected analytics to be paused")
	}
}

func TestPauseServiceNotWired(t *testing.T) {
	reg := newFakeReg()
	h := newTestHandler(newFakePool(), reg)

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "pause_service",
		Payload: json.RawMessage(`{"service":"identity"}`),
	})
	if r.OK || r.Error != "service identity is not wired" {
		t.Fatalf("expected not wired error, got %+v", r)
	}
}

func TestPauseServiceUnknown(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "pause_service",
		Payload: json.RawMessage(`{"service":"bogus"}`),
	})
	if r.OK || r.Error != "unknown service: bogus" {
		t.Fatalf("expected unknown service error, got %+v", r)
	}
}

func TestResumeServiceOK(t *testing.T) {
	reg := newFakeReg()
	reg.services["identity"].Wired = false
	h := newTestHandler(newFakePool(), reg)

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "resume_service",
		Payload: json.RawMessage(`{"service":"identity"}`),
	})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if !reg.services["identity"].Wired {
		t.Fatal("expected identity to be resumed")
	}
}

func TestResumeServiceAlreadyRunning(t *testing.T) {
	reg := newFakeReg()
	h := newTestHandler(newFakePool(), reg)

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "resume_service",
		Payload: json.RawMessage(`{"service":"orders"}`),
	})
	if r.OK || r.Error != "service orders is already running" {
		t.Fatalf("expected already running error, got %+v", r)
	}
}

func TestToggleAnalytics(t *testing.T) {
	reg := newFakeReg()
	h := newTestHandler(newFakePool(), reg)

	// analytics starts wired → toggle pauses it
	r := send(h, IncomingMessage{Type: "action", Action: "toggle_analytics"})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if reg.services["analytics"].Wired {
		t.Fatal("expected analytics to be paused after first toggle")
	}

	// second toggle resumes it
	r = send(h, IncomingMessage{Type: "action", Action: "toggle_analytics"})
	if !r.OK {
		t.Fatalf("expected OK on second toggle, got %+v", r)
	}
	if !reg.services["analytics"].Wired {
		t.Fatal("expected analytics to be resumed after second toggle")
	}
}

func TestAllStubsReturnOK(t *testing.T) {
	stubs := []string{
		"restart_worker", "set_ack_policy", "set_retry_policy",
		"route_to_dlq", "set_exchange_type", "add_binding",
		"remove_binding", "set_priority",
		"use_freeze_frame",
	}
	h := newTestHandler(newFakePool(), newFakeReg())
	for _, name := range stubs {
		r := send(h, IncomingMessage{Type: "action", Action: name})
		if !r.OK {
			t.Errorf("stub %q: expected OK, got %+v", name, r)
		}
	}
}

func TestSetProcessingModeToggle(t *testing.T) {
	reg := newFakeReg()
	h := newTestHandler(newFakePool(), reg)

	// orders starts synchronous (Incident 1 ^), so first toggle flips async.
	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "set_processing_mode",
		Payload: json.RawMessage(`{"mode":"async"}`),
	})
	if !r.OK {
		t.Fatalf("expected OK switching to async, got %+v", r)
	}
	if reg.services["orders"].Synchronous {
		t.Fatal("expected orders to be async after toggle")
	}

	// Same mode again is a no-op with a clear error.
	r = send(h, IncomingMessage{
		Type:    "action",
		Action:  "set_processing_mode",
		Payload: json.RawMessage(`{"mode":"async"}`),
	})
	if r.OK || r.Error != "orders already running in async mode" {
		t.Fatalf("expected already-async error, got %+v", r)
	}

	// Flip back to sync.
	r = send(h, IncomingMessage{
		Type:    "action",
		Action:  "set_processing_mode",
		Payload: json.RawMessage(`{"service":"orders","mode":"sync"}`),
	})
	if !r.OK {
		t.Fatalf("expected OK switching back to sync, got %+v", r)
	}
	if !reg.services["orders"].Synchronous {
		t.Fatal("expected orders to be sync again")
	}
}

func TestSetProcessingModeInvalidMode(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "set_processing_mode",
		Payload: json.RawMessage(`{"mode":"bogus"}`),
	})
	if r.OK || r.Error != `mode must be "sync" or "async"` {
		t.Fatalf("expected invalid-mode error, got %+v", r)
	}
}

func TestSetProcessingModeRejectsOtherServices(t *testing.T) {
	h := newTestHandler(newFakePool(), newFakeReg())
	for _, svc := range []string{"identity", "notifications", "audit", "payments"} {
		r := send(h, IncomingMessage{
			Type:    "action",
			Action:  "set_processing_mode",
			Payload: json.RawMessage(`{"service":"` + svc + `","mode":"async"}`),
		})
		if r.OK || r.Error != "processing mode only applies to orders" {
			t.Errorf("%s: expected orders-only error, got %+v", svc, r)
		}
	}
}

func TestSetProcessingModeServiceNotWired(t *testing.T) {
	reg := newFakeReg()
	reg.services["orders"].Wired = false
	h := newTestHandler(newFakePool(), reg)

	r := send(h, IncomingMessage{
		Type:    "action",
		Action:  "set_processing_mode",
		Payload: json.RawMessage(`{"mode":"async"}`),
	})
	if r.OK || r.Error != "service orders is not wired" {
		t.Fatalf("expected not-wired error, got %+v", r)
	}
}

func TestStubActionsRejected(t *testing.T) {
	// The three stub services (identity, notifications, audit) must not be
	// actionable by the incident's live controls: scaling, pausing, or toggling
	// processing mode all fail. (resume_service remains open on unwired
	// services by design — it is the same control that re-wires analytics.)
	h := newTestHandler(newFakePool(), newFakeReg())
	for _, svc := range []string{"identity", "notifications", "audit"} {
		for _, a := range []struct {
			action  string
			payload string
		}{
			{"scale_workers", `{"service":"` + svc + `","delta":1}`},
			{"pause_service", `{"service":"` + svc + `"}`},
			{"set_processing_mode", `{"service":"` + svc + `","mode":"async"}`},
		} {
			r := send(h, IncomingMessage{Type: "action", Action: a.action, Payload: json.RawMessage(a.payload)})
			if r.OK {
				t.Errorf("%s on stub %s must be rejected, got OK", a.action, svc)
			}
		}
	}
}

func TestActionLogged(t *testing.T) {
	log := events.NewLog()
	tick := int64(99)
	h := NewActionHandler(newFakePool(), newFakeReg(), log, func() int64 { return tick })

	h(rawmsg("scale_workers", `{"service":"orders","delta":1}`))

	entries := log.Tail(10)
	found := false
	for _, e := range entries {
		if e.Type == "action" && e.Subject == "scale_workers" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected action event in log")
	}
}

func rawmsg(action, payload string) []byte {
	b, _ := json.Marshal(IncomingMessage{
		Type:    "action",
		Action:  action,
		Payload: json.RawMessage(payload),
	})
	return b
}

// ---------------------------------------------------------------------------
// Integration tests — require Docker RabbitMQ at BLACKOUT_AMQP_URL
// ---------------------------------------------------------------------------

const intTestAmqpURL = "amqp://lumen:lumen@localhost:5673/"

func skipIfNoBroker(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	b := rabbitmq.NewBroker(intTestAmqpURL)
	if err := b.Connect(ctx); err != nil {
		t.Skipf("rabbitmq not available at %s: %v", intTestAmqpURL, err)
	}
	b.Close()
}

type intTestDeps struct {
	broker    *rabbitmq.Broker
	pub       *rabbitmq.Publisher
	reg       *rabbitmq.Registry
	pm        *rabbitmq.PoolManager
	handler   ActionHandler
	processed map[string]*atomic.Int64 // per-queue consumed-message counts
	cancel    context.CancelFunc
}

func setupIntTest(t *testing.T) *intTestDeps {
	t.Helper()
	skipIfNoBroker(t)

	ctx, cancel := context.WithCancel(context.Background())
	broker := rabbitmq.NewBroker(intTestAmqpURL)
	if err := broker.Connect(ctx); err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}

	reg := rabbitmq.AllServices()
	top := reg.WiredTopology()
	ch, err := broker.Channel()
	if err != nil {
		cancel()
		broker.Close()
		t.Fatalf("channel: %v", err)
	}
	if err := top.Declare(ctx, ch); err != nil {
		ch.Close()
		cancel()
		broker.Close()
		t.Fatalf("declare: %v", err)
	}
	// Purge queues so tests start clean.
	for _, q := range reg.WiredQueues() {
		_, _ = ch.QueuePurge(q, false)
	}
	ch.Close()

	pub, err := rabbitmq.NewPublisher(broker)
	if err != nil {
		cancel()
		broker.Close()
		t.Fatalf("publisher: %v", err)
	}

	counts := map[string]int{
		"orders.work":      3,
		"analytics.events": 2,
		"payments.work":    2,
	}
	processed := map[string]*atomic.Int64{
		"orders.work":      {},
		"analytics.events": {},
		"payments.work":    {},
	}
	handler := func(_ context.Context, queue string, d rabbitmq.Delivery) error {
		if c := processed[queue]; c != nil {
			c.Add(1)
		}
		// Do NOT ack here: the worker pool acks after the handler returns nil.
		return nil
	}
	pm := rabbitmq.NewPoolManager(broker, reg, counts, handler)
	go pm.Run(ctx)

	adapter := &regAdapter{reg: reg, modes: map[string]bool{"orders": true}}
	actionLog := events.NewLog()
	tick := int64(100)
	actionHandler := NewActionHandler(pm, adapter, actionLog, func() int64 { return tick })

	return &intTestDeps{
		broker:    broker,
		pub:       pub,
		reg:       reg,
		pm:        pm,
		handler:   actionHandler,
		processed: processed,
		cancel:    cancel,
	}
}

func (d *intTestDeps) teardown() {
	d.cancel()
	d.pm.Stop()
	d.pub.Close()
	d.broker.Close()
}

// regAdapter wraps *rabbitmq.Registry to satisfy ServiceReg.
type regAdapter struct {
	reg   *rabbitmq.Registry
	modes map[string]bool // orders processing-mode flag (simulation-backed in prod)
}

func (a *regAdapter) Get(id string) *ServiceState {
	svc := a.reg.Get(id)
	if svc == nil {
		return nil
	}
	return &ServiceState{
		ID:          svc.ID,
		Wired:       svc.Wired,
		Synchronous: a.modes["orders"],
	}
}

func (a *regAdapter) SetWired(id string, wired bool) bool {
	return a.reg.SetWired(id, wired)
}

func (a *regAdapter) SetSynchronous(id string, sync bool) bool {
	if a.modes[id] == sync {
		return false
	}
	a.modes[id] = sync
	return true
}

// publishTo publishes n test messages straight into a named queue via the
// default exchange (routing key == queue name).
func publishTo(t *testing.T, d *intTestDeps, n int, queue string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bodies := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		bodies = append(bodies, []byte(fmt.Sprintf(`{"id":"it-%s-%d","kind":"test"}`, queue, i)))
	}
	if err := d.pub.PublishBatch(ctx, "", queue, bodies); err != nil {
		t.Fatalf("publish to %s: %v", queue, err)
	}
}

// waitProcessed polls a per-queue consumed-message counter until it reaches
// want or the timeout expires. The counter updates on every ack — immediate,
// no async management stats involved.
func waitProcessed(t *testing.T, c *atomic.Int64, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Load() >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d processed messages on queue (got %d)", want, c.Load())
}

func TestInt_ScaleWorkers(t *testing.T) {
	d := setupIntTest(t)
	defer d.teardown()

	// Publish and drain a baseline batch: proves the orders pool is consuming.
	publishTo(t, d, 5, "orders.work")
	waitProcessed(t, d.processed["orders.work"], 5, 10*time.Second)

	// Scale up by +2.
	r := send(d.handler, IncomingMessage{
		Type:    "action",
		Action:  "scale_workers",
		Payload: json.RawMessage(`{"service":"orders","delta":2}`),
	})
	if !r.OK {
		t.Fatalf("scale_workers failed: %+v", r)
	}

	// Verify PoolManager updated its count.
	if got := d.pm.Workers("orders.work"); got != 5 {
		t.Fatalf("expected 5 workers after scale, got %d", got)
	}

	// Publish another batch and wait for it to drain: proves the new pool
	// (5 workers) actually consumes from the real broker.
	publishTo(t, d, 5, "orders.work")
	waitProcessed(t, d.processed["orders.work"], 10, 10*time.Second)
}

func TestInt_PauseResumeService(t *testing.T) {
	d := setupIntTest(t)
	defer d.teardown()

	// Baseline: publish 5 to analytics, wait for all to drain.
	publishTo(t, d, 5, "analytics.events")
	waitProcessed(t, d.processed["analytics.events"], 5, 10*time.Second)

	// Pause analytics.
	r := send(d.handler, IncomingMessage{
		Type:    "action",
		Action:  "pause_service",
		Payload: json.RawMessage(`{"service":"analytics"}`),
	})
	if !r.OK {
		t.Fatalf("pause failed: %+v", r)
	}
	svc := d.reg.Get("analytics")
	if svc == nil || svc.Wired {
		t.Fatal("expected analytics to be unwired after pause")
	}

	// Let the pool stop, then publish while paused. Messages should sit in the
	// queue: the processed counter must stay frozen at 5.
	time.Sleep(500 * time.Millisecond)
	publishTo(t, d, 5, "analytics.events")
	time.Sleep(800 * time.Millisecond)
	if got := d.processed["analytics.events"].Load(); got != 5 {
		t.Fatalf("expected frozen at 5 processed while paused, got %d", got)
	}

	// Resume analytics.
	r = send(d.handler, IncomingMessage{
		Type:    "action",
		Action:  "resume_service",
		Payload: json.RawMessage(`{"service":"analytics"}`),
	})
	if !r.OK {
		t.Fatalf("resume failed: %+v", r)
	}
	svc = d.reg.Get("analytics")
	if svc == nil || !svc.Wired {
		t.Fatal("expected analytics to be wired after resume")
	}

	// The restarted pool must drain the backlog plus the new messages: 5 + 5.
	waitProcessed(t, d.processed["analytics.events"], 10, 10*time.Second)
}

func TestInt_ToggleAnalytics(t *testing.T) {
	d := setupIntTest(t)
	defer d.teardown()

	// Baseline: prove analytics is consuming.
	publishTo(t, d, 5, "analytics.events")
	waitProcessed(t, d.processed["analytics.events"], 5, 10*time.Second)

	// Toggle off → messages freeze.
	r := send(d.handler, IncomingMessage{Type: "action", Action: "toggle_analytics"})
	if !r.OK {
		t.Fatalf("toggle 1 failed: %+v", r)
	}
	svc := d.reg.Get("analytics")
	if svc == nil || svc.Wired {
		t.Fatal("expected analytics paused after first toggle")
	}
	time.Sleep(500 * time.Millisecond)
	publishTo(t, d, 5, "analytics.events")
	time.Sleep(800 * time.Millisecond)
	if got := d.processed["analytics.events"].Load(); got != 5 {
		t.Fatalf("expected frozen at 5 processed while paused, got %d", got)
	}

	// Toggle back on → backlog + new messages drain.
	r = send(d.handler, IncomingMessage{Type: "action", Action: "toggle_analytics"})
	if !r.OK {
		t.Fatalf("toggle 2 failed: %+v", r)
	}
	svc = d.reg.Get("analytics")
	if svc == nil || !svc.Wired {
		t.Fatal("expected analytics resumed after second toggle")
	}
	waitProcessed(t, d.processed["analytics.events"], 10, 10*time.Second)
}

func TestInt_AllActionsAccepted(t *testing.T) {
	d := setupIntTest(t)
	defer d.teardown()

	actions := []struct {
		action  string
		payload string
	}{
		{"scale_workers", `{"service":"orders","delta":1}`},
		{"pause_service", `{"service":"analytics"}`},
		{"resume_service", `{"service":"analytics"}`},
		{"toggle_analytics", `{}`},
		{"restart_worker", `{}`},
		{"set_ack_policy", `{}`},
		{"set_retry_policy", `{}`},
		{"route_to_dlq", `{}`},
		{"set_exchange_type", `{}`},
		{"add_binding", `{}`},
		{"remove_binding", `{}`},
		{"set_priority", `{}`},
		{"set_processing_mode", `{"mode":"async"}`},
		{"set_processing_mode", `{"mode":"sync"}`},
		{"use_freeze_frame", `{}`},
	}

	for _, a := range actions {
		r := send(d.handler, IncomingMessage{
			Type:    "action",
			Action:  a.action,
			Payload: json.RawMessage(a.payload),
		})
		if !r.OK {
			t.Errorf("action %q: expected OK, got %+v", a.action, r)
		}
	}
}
