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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("blackout backend stopped")
}
