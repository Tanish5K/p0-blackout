package api

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Hub tracks live WebSocket connections. Phase 0 only verifies connectivity;
// game state broadcasting lands in a later phase.
type Hub struct {
	mu     sync.Mutex
	conns  map[*websocket.Conn]bool
	origin string
}

func NewRouter(origin string) http.Handler {
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
	return mux
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

	// Phase 0: just confirm the socket is live, then echo nothing.
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello","msg":"connected"}`)); err != nil {
		return
	}

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		// Player actions arrive here in a later phase.
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
