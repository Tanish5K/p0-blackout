package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestLateClientReceivesLatestFullSnapshot(t *testing.T) {
	hub, handler := NewRouter("http://example.test")
	hub.BroadcastSnapshot([]byte(`{"type":"snapshot","tick":2}`), []byte(`{"type":"snapshot","tick":2,"services":[{"id":"orders"}]}`))

	server := httptest.NewServer(handler)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, hello, err := conn.ReadMessage()
	if err != nil || !strings.Contains(string(hello), `"type":"hello"`) {
		t.Fatalf("hello = %q, err = %v", hello, err)
	}
	_, snapshot, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(snapshot); !strings.Contains(got, `"services"`) || !strings.Contains(got, `"tick":2`) {
		t.Fatalf("late-client snapshot was not full: %s", got)
	}
}

func TestHandlerSwapAndBroadcastAreRaceSafe(t *testing.T) {
	hub, _ := NewRouter("")
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			hub.SetActionHandler(func([]byte) []byte { return nil })
			hub.SetControlHandler(func([]byte) []byte { return nil })
		}
	}()
	for i := 0; i < 1000; i++ {
		hub.dispatch([]byte(`{"type":"action"}`))
		hub.Broadcast([]byte(`{"type":"snapshot"}`))
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler swaps did not complete")
	}
}
