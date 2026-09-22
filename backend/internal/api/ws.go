package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const clientSendBuffer = 64

// Hub tracks live WebSocket clients. Every connection has exactly one writer
// goroutine; snapshots, action replies, lifecycle replies, and pings all pass
// through that writer.
type Hub struct {
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
	origin  string
	handler func(raw []byte) []byte
	control func(raw []byte) []byte
	latest  []byte // latest complete snapshot, used to hydrate late clients
}

type wsClient struct {
	conn *websocket.Conn
	send chan []byte
	done chan struct{}
	once sync.Once
}

func (c *wsClient) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func NewRouter(origin string) (*Hub, http.Handler) {
	hub := &Hub{clients: make(map[*wsClient]struct{}), origin: origin}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ws", hub.handleWS)
	return hub, mux
}

// SetActionHandler atomically swaps the current per-run action dispatcher.
func (h *Hub) SetActionHandler(fn func(raw []byte) []byte) {
	h.mu.Lock()
	h.handler = fn
	h.mu.Unlock()
}

func (h *Hub) SetControlHandler(fn func(raw []byte) []byte) {
	h.mu.Lock()
	h.control = fn
	h.mu.Unlock()
}

// Broadcast sends data to every connected client. Snapshot callers should use
// BroadcastSnapshot so reconnecting clients receive a complete current frame.
func (h *Hub) Broadcast(data []byte) {
	h.broadcast(data, nil)
}

// BroadcastSnapshot broadcasts data (which may be a delta) and caches full as
// the authoritative reconnect frame. Buffers are copied before return.
func (h *Hub) BroadcastSnapshot(data, full []byte) {
	h.broadcast(data, full)
}

func (h *Hub) broadcast(data, full []byte) {
	h.mu.Lock()
	if full != nil {
		h.latest = append(h.latest[:0], full...)
	}
	clients := make([]*wsClient, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.Unlock()

	for _, client := range clients {
		if !enqueue(client, data) {
			h.remove(client)
		}
	}
}

func enqueue(client *wsClient, data []byte) bool {
	msg := append([]byte(nil), data...)
	select {
	case <-client.done:
		return false
	case client.send <- msg:
		return true
	default:
		// Slow clients cannot stall the simulation or grow memory without bound.
		client.close()
		return false
	}
}

func (h *Hub) remove(client *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		client.close()
	}
	h.mu.Unlock()
}

// Run closes clients on server shutdown. Writer pumps own heartbeat scheduling
// to preserve the single-writer invariant.
func (h *Hub) Run(ctx context.Context) {
	<-ctx.Done()
	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
		delete(h.clients, client)
	}
	h.mu.Unlock()
	for _, client := range clients {
		client.close()
	}
}

func (h *Hub) ConnCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) handleWS(w http.ResponseWriter, r *http.Request) {
	if !h.originAllowed(r) {
		log.Printf("ws rejected: origin %q not allowed", r.Header.Get("Origin"))
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}
	client := &wsClient{conn: conn, send: make(chan []byte, clientSendBuffer), done: make(chan struct{})}

	h.mu.Lock()
	h.clients[client] = struct{}{}
	latest := append([]byte(nil), h.latest...)
	n := len(h.clients)
	h.mu.Unlock()
	log.Printf("ws client connected (total %d)", n)

	go h.writePump(client)
	if !enqueue(client, []byte(`{"type":"hello","msg":"connected"}`)) {
		h.remove(client)
		return
	}
	if len(latest) > 0 && !enqueue(client, latest) {
		h.remove(client)
		return
	}

	defer func() {
		h.remove(client)
		log.Printf("ws client disconnected (total %d)", h.ConnCount())
	}()
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if response := h.dispatch(msg); response != nil && !enqueue(client, response) {
			return
		}
	}
}

func (h *Hub) dispatch(msg []byte) []byte {
	h.mu.RLock()
	control, handler := h.control, h.handler
	h.mu.RUnlock()
	if control != nil {
		var probe struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(msg, &probe) == nil && probe.Type == "control" {
			return control(msg)
		}
	}
	if handler == nil {
		return nil
	}
	return handler(msg)
}

func (h *Hub) writePump(client *wsClient) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	defer h.remove(client)
	for {
		select {
		case <-client.done:
			return
		case msg := <-client.send:
			if err := client.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Hub) originAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	return strings.EqualFold(strings.TrimRight(origin, "/"), strings.TrimRight(h.origin, "/"))
}
