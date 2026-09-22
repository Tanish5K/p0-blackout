package rabbitmq

import (
	"context"
	"testing"
)

func newDisabledPM() *PoolManager {
	reg := AllServices()
	// counts only matter for pools that actually start; Scale never starts one
	// because every Scale call here targets unknown queues and returns early.
	return NewPoolManager(nil, reg, map[string]int{}, nil)
}

func TestHasWiredQueue(t *testing.T) {
	reg := AllServices()

	if !reg.HasWiredQueue("orders.work") {
		t.Error("orders.work must be wired from incident 1")
	}
	if !reg.HasWiredQueue("analytics.events") {
		t.Error("analytics.events must be wired from incident 1")
	}
	if !reg.HasWiredQueue("payments.work") {
		t.Error("payments.work must be wired from incident 1")
	}

	// Identity/Notifications/Audit own no queues until their incidents, so any
	// queue name pointing at them must come back unwired.
	if reg.HasWiredQueue("identity.work") || reg.HasWiredQueue("notifications.work") || reg.HasWiredQueue("audit.work") {
		t.Error("stub services must not own wired queues")
	}
}

func TestScaleRejectsUnknownQueue(t *testing.T) {
	pm := newDisabledPM()

	// Complete unknowns (never a queue name) are rejected outright.
	for _, q := range []string{"identity.work", "notifications.work", "audit.work", "bogus.queue"} {
		if err := pm.Scale(q, 4); err == nil {
			t.Errorf("Scale(%q, 4) must fail for an unwired/unknown queue", q)
		}
		if got := pm.Workers(q); got != 0 {
			t.Errorf("Scale(%q) must not touch worker counts on rejection (got %d)", q, got)
		}
	}

	// Pausing a service then scaling its queue must also fail: a paused service
	// owns no worker pool.
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg := AllServices()
	if !reg.SetWired("analytics", false) {
		t.Fatal("failed to pause analytics")
	}
	pm2 := NewPoolManager(nil, reg, map[string]int{"analytics.events": 2}, nil)
	if err := pm2.Scale("analytics.events", 6); err == nil {
		t.Error("Scale on a paused service's queue must fail")
	}
}

func TestConfigureDoesNotStartPoolsWhileSuspended(t *testing.T) {
	registry := AllServices()
	manager := NewPoolManager(nil, registry, map[string]int{"orders.work": 2}, nil)
	manager.StopAndWait()
	manager.Configure(map[string]int{"orders.work": 9})
	manager.reconcile()
	if got := manager.PoolCount(); got != 0 {
		t.Fatalf("configured suspended manager started %d pools", got)
	}
	if got := manager.Workers("orders.work"); got != 9 {
		t.Fatalf("workers = %d, want 9", got)
	}
	if got := manager.AckPolicy("orders.work"); got != string(AckManual) {
		t.Fatalf("ack policy = %q, want manual", got)
	}
}

func TestRunShutdownLeavesManagerSuspended(t *testing.T) {
	registry := AllServices()
	manager := NewPoolManager(nil, registry, nil, nil)
	manager.suspended = true // prevent a nil-broker pool from starting in this unit test
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { manager.Run(ctx); close(done) }()
	cancel()
	<-done
	if !manager.suspended {
		t.Fatal("shutdown should leave reconciliation suspended")
	}
}
