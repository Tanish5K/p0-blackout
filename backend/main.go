package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"blackout/internal/api"
	"blackout/internal/rabbitmq"
	"blackout/internal/simulation"
)

func main() {
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
		_ = ch.Close()
	}
	log.Printf("wired services: %d of %d (stubs: identity, notifications, audit)",
		len(reg.Wired()), len(reg.Services))

	pub, err := rabbitmq.NewPublisher(broker)
	if err != nil {
		log.Fatalf("create publisher: %v", err)
	}
	defer pub.Close()

	// Worker counts per wireable work queue (Phase 2: real simulated work).
	rt := simulation.NewRuntime()
	workerCount := map[string]int{
		"orders.work":      3,
		"analytics.events": 2,
		"payments.work":    2,
	}
	handler := func(ctx context.Context, queue string, d rabbitmq.Delivery) error {
		return simulation.Work(ctx, rt, queue)
	}
	pm := rabbitmq.NewPoolManager(broker, reg, workerCount, handler)
	go pm.Run(ctx)
	defer pm.Stop()

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewRouter(origin),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("blackout backend listening on %s (allowed origin %s)", addr, origin)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Phase 2 bridge: the simulation is the traffic generator. It publishes its
	// decided traffic for real and reads real queue depth back from RabbitMQ's
	// management API
	mgmt := rabbitmq.NewMgmt(mgmtURL, "lumen", "lumen")
	go runSimulation(ctx, pub, mgmt, rt)

	<-ctx.Done()
	log.Println("shutting down…")

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("blackout backend stopped")
}
