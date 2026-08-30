package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"blackout/internal/api"
	"blackout/internal/rabbitmq"
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

	top := rabbitmq.LumenTopology()
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

	pub, err := rabbitmq.NewPublisher(broker)
	if err != nil {
		log.Fatalf("create publisher: %v", err)
	}
	defer pub.Close()

	// orders.work consumers: manual ack, log every message to stdout.
	orders := rabbitmq.NewWorkerPool(broker, top, "orders.work", 3,
		func(ctx context.Context, d rabbitmq.Delivery) error {
			// Phase 1: no business logic yet — just accept the message.
			return nil
		})
	go orders.Run(ctx)
	defer orders.Stop()

	analytics := rabbitmq.NewWorkerPool(broker, top, "analytics.events", 2,
    func(ctx context.Context, d rabbitmq.Delivery) error {
        return nil // accept, ack
    })
	go analytics.Run(ctx)
	defer analytics.Stop()
	

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

	// Synthetic publisher: prove publish → orders.work + analytics.events.
	go runSyntheticPublisher(ctx, pub)

	<-ctx.Done()
	log.Println("shutting down…")

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("blackout backend stopped")
}

// runSyntheticPublisher emits one order.created event per tick so the routing
// chain (order.events → orders.work + analytics.events) is observable without
// a real traffic generator yet. Replaced by simulation traffic in Phase 2.
func runSyntheticPublisher(ctx context.Context, pub *rabbitmq.Publisher) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			seq++
			body := []byte(fmt.Appendf(nil,
				`{"orderId":"ORD-%05d","event":"order.created","ts":"%s"}`,
				seq, tick.UTC().Format(time.RFC3339)))
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := pub.Publish(pctx, "order.events", "order.created", body)
			cancel()
			if err != nil {
				log.Printf("publish failed: %v", err)
				continue
			}
			log.Printf("published order.created id=ORD-%05d (confirmed)", seq)
		}
	}
}
