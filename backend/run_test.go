package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"blackout/internal/simulation"
)

func TestBeginAbortsBeforeResetWhenPreviousRunTimesOut(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	controller := &RunController{
		status:          RunEnded,
		runId:           4,
		dbMult:          1,
		cancelRun:       cancel,
		doneRun:         make(chan struct{}),
		teardownTimeout: 10 * time.Millisecond,
	}
	err := controller.begin()
	if err == nil || !strings.Contains(err.Error(), "restart aborted") {
		t.Fatalf("begin error = %v, want teardown abort", err)
	}
	if controller.runId != 4 {
		t.Fatalf("run id advanced to %d after failed teardown", controller.runId)
	}
	if controller.status != RunEnded {
		t.Fatalf("status = %q, want restored ended", controller.status)
	}
}

func TestFinishAdvancesWithoutDeadlockAndUsesAddedWorkers(t *testing.T) {
	baseline := totalConfiguredWorkers(defaultWorkerCounts())
	controller := &RunController{runId: 1, status: RunRunning, dbMult: 1}
	done := make(chan struct{})
	go func() {
		controller.finish(1, &simulation.Outcome{PeakWorkers: baseline + 8})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("finish deadlocked while applying the cascade")
	}
	if controller.incidentIdx != 1 {
		t.Fatalf("incident index = %d, want 1", controller.incidentIdx)
	}
	if controller.dbMult != 1.5 {
		t.Fatalf("db multiplier = %.2f, want 1.5", controller.dbMult)
	}
}

func TestCascadeThresholdIsAboveBaseline(t *testing.T) {
	baseline := totalConfiguredWorkers(defaultWorkerCounts())
	controller := &RunController{dbMult: 1}
	controller.settleCascade(baseline + 7)
	if controller.dbMult != 1 {
		t.Fatalf("sub-threshold scale changed multiplier to %.2f", controller.dbMult)
	}
	controller.settleCascade(baseline + 8)
	if controller.dbMult != 1.5 {
		t.Fatalf("threshold scale left multiplier at %.2f", controller.dbMult)
	}
}
