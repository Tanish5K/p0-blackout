package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"blackout/internal/api"
	"blackout/internal/rabbitmq"
	"blackout/internal/simulation"

	"github.com/joho/godotenv"
)

// sentAtMillis extracts the generator's sent-at timestamp ({"ts": millis})
// from a message body, returning ok=false for bodies without one.
func sentAtMillis(body []byte) (time.Time, bool) {
	var m struct {
		Ts int64 `json:"ts"`
	}
	if err := json.Unmarshal(body, &m); err != nil || m.Ts == 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(m.Ts), true
}

func main() {
	_ = godotenv.Load() // load backend/.env if present; no error if missing

	addr := os.Getenv("BLACKOUT_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	origin := os.Getenv("BLACKOUT_ORIGIN")
	if origin == "" {
		origin = "http://localhost:5173"
	}
	amqpURL := os.Getenv("BLACKOUT_AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://lumen:lumen@localhost:5673/"
	}
	mgmtURL := os.Getenv("BLACKOUT_MGMT_URL")
	if mgmtURL == "" {
		mgmtURL = "http://localhost:15673"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := rabbitmq.NewBroker(amqpURL)
	if err := broker.Connect(ctx); err != nil {
		if ctx.Err() != nil {
			log.Println("interrupted before connecting; shutting down")
			return
		}
		log.Fatalf("connect rabbitmq: %v", err)
	}
	defer broker.Close()

	// Only Wired services contribute exchanges/queues/bindings; Identity/Notifications/Audit are
	// stubs until their incident flips Wired on (§5.2).
	reg := rabbitmq.AllServices()
	top := reg.WiredTopology()
	{
		ch, err := broker.Channel()
		if err != nil {
			log.Fatalf("open channel: %v", err)
		}
		if err := top.Declare(ctx, ch); err != nil {
			log.Fatalf("declare topology: %v", err)
		}
		// Purge leftover messages so a re-run starts clean. Messages are
		// published transient and the queues are durable, but a hard-stopped
		// backend can still leave an in-flight backlog. Set BLACKOUT_KEEP_QUEUES=1
		// to skip (e.g. resume a prior run).
		if os.Getenv("BLACKOUT_KEEP_QUEUES") != "1" {
			if err := purgeQueues(ctx, broker, reg.WiredQueues()); err != nil {
				log.Printf("initial purge: %v", err)
			}
		}
		_ = ch.Close()
	}
	log.Printf("wired services: %d of %d (stubs: identity, notifications, audit)",
		len(reg.Wired()), len(reg.Services))

	pub, err := rabbitmq.NewPublisher(broker)
	if err != nil {
		log.Fatalf("create publisher: %v", err)
	}
	defer pub.Close()

	// Runtime carries the detached simulation-only machinery (queue trackers,
	// load samplers, the sync-gateway latency recorder).
	rt := simulation.NewRuntime()

	// gatewayTimed wraps the work handler so the ORDER path's publish→ack round
	// trip lands in Runtime.Gateway: the customer-visible latency the simulation
	// reads in sync mode. The age is computed against the sent-at stamp the
	// generator embeds in each body, so queue wait + processing are both real.
	// Incident 2's synchronous identity share mixes into the same sampler, so
	// the customer-visible latency honestly reflects whichever leg is hurting.
	handler := func(ctx context.Context, queue string, d rabbitmq.Delivery) error {
		if queue == "orders.work" || queue == "identity.worker" {
			if t, ok := sentAtMillis(d.Body); ok {
				rt.Gateway.Record(time.Since(t))
			}
		}
		return simulation.Work(ctx, rt, queue)
	}
	pm := rabbitmq.NewPoolManager(broker, reg, defaultWorkerCounts(), handler)
	go pm.Run(ctx)
	defer pm.Stop()

	hub, router := api.NewRouter(origin)
	go hub.Run(ctx)

	srv := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("blackout backend listening on %s (allowed origin %s)", addr, origin)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Phase 3 bridge: the simulation is the traffic generator. It publishes its
	// decided traffic for real and reads real queue depth back from RabbitMQ's
	// management API. The Hub broadcasts snapshots at 10Hz and dispatches
	// player actions, plus run-lifecycle control messages.
	mgmt := rabbitmq.NewMgmt(mgmtURL, "lumen", "lumen")

	// Incident 1 is now playable from the UI: idle on boot, Started/Retried over
	// the /ws control channel. BLACKOUT_AUTOSTART=1 preserves the old
	// boot-and-run behaviour for scripts and smoke tests.
	controller := NewRunController(ctx, broker, pub, mgmt, rt, hub, pm, reg)
	hub.SetControlHandler(controller.HandleControl)
	controller.Boot()
	if os.Getenv("BLACKOUT_AUTOSTART") == "1" {
		log.Printf("BLACKOUT_AUTOSTART=1 — starting run immediately")
		if err := controller.Start(); err != nil {
			log.Printf("autostart: %v", err)
		}
	}

	<-ctx.Done()
	log.Println("shutting down…")

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("blackout backend stopped")
}
