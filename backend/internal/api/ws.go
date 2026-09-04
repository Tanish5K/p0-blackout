package api

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Hub tracks live WebSocket connections and broadcasts game state snapshots.
// It also dispatches incoming player actions to a registered handler.
type Hub struct {
	mu      sync.Mutex
	conns   map[*websocket.Conn]bool
	origin  string
	handler func(raw []byte) []byte // action dispatcher, set by sim.go
}

// NewRouter creates the Hub and returns the HTTP handler for /ws and /healthz.
func NewRouter(origin string) (*Hub, http.Handler) {
	hub := &Hub{
		conns:  make(map[*websocket.Conn]bool),
		origin: origin,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ws", hub.handleWS)
	return hub, mux
}

// SetActionHandler registers the function called for every incoming player
// action. The handler receives the raw JSON message and must return a JSON
// response to send back to the client. Must be called before Run.
func (h *Hub) SetActionHandler(fn func(raw []byte) []byte) {
	h.handler = fn
}

// Broadcast sends data to every connected client. Dead connections are
// silently dropped. This is the primary output path for game state snapshots.
func (h *Hub) Broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn := range h.conns {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			conn.Close()
			delete(h.conns, conn)
		}
	}
}

// Run starts the Hub's background loop: sends pings and garbage-collects dead
// connections. Block until ctx is done.
func (h *Hub) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			h.mu.Lock()
			for conn := range h.conns {
				conn.Close()
			}
			h.conns = make(map[*websocket.Conn]bool)
			h.mu.Unlock()
			return
		case <-ticker.C:
			h.mu.Lock()
			for conn := range h.conns {
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					conn.Close()
					delete(h.conns, conn)
				}
			}
			h.mu.Unlock()
		}
	}
}

// ConnCount returns the number of live connections (for diagnostics).
func (h *Hub) ConnCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

func (h *Hub) handleWS(w http.ResponseWriter, r *http.Request) {
	if !h.originAllowed(r) {
		log.Printf("ws rejected: origin %q not allowed", r.Header.Get("Origin"))
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true }, // checked above
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}

	h.mu.Lock()
	h.conns[conn] = true
	n := len(h.conns)
	h.mu.Unlock()
	log.Printf("ws client connected (total %d)", n)

	defer func() {
		h.mu.Lock()
		delete(h.conns, conn)
		n := len(h.conns)
		h.mu.Unlock()
		conn.Close()
		log.Printf("ws client disconnected (total %d)", n)
	}()

	// Heartbeat to detect dead clients.
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	// Send hello to confirm the socket is live.
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello","msg":"connected"}`)); err != nil {
		return
	}

	// Read loop: dispatch player actions to the registered handler.
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if h.handler == nil {
			continue
		}
		resp := h.handler(msg)
		if resp != nil {
			if err := conn.WriteMessage(websocket.TextMessage, resp); err != nil {
				return
			}
		}
	}
}

func (h *Hub) originAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Non-browser clients (tests, tools) send no Origin.
		return true
	}
	return strings.EqualFold(strings.TrimRight(origin, "/"), strings.TrimRight(h.origin, "/"))
}
