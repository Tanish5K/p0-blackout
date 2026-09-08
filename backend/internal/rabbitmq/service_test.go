package rabbitmq

import "testing"

func TestResetWiredRestoresCampaignDefaults(t *testing.T) {
	r := AllServices()

	// Pause a wired service and pin a stub on. Start: gateway, orders,
	// payments, analytics (4 wired). After: gateway, payments, analytics,
	// identity (4 wired, but orders gone and identity on).
	if !r.SetWired("orders", false) {
		t.Fatalf("expected SetWired(orders,false) to flip state")
	}
	if !r.SetWired("identity", true) {
		t.Fatalf("expected SetWired(identity,true) to flip state")
	}
	if got := len(r.Wired()); got != 4 {
		t.Fatalf("after mutations, Wired = %d services, want 4 (gateway, payments, analytics, identity)", got)
	}

	r.ResetWired()

	if got := len(r.Wired()); got != 4 {
		t.Fatalf("after ResetWired, Wired = %d, want 4 (WiredIn==0 defaults)", got)
	}
	for _, id := range []string{"gateway", "orders", "payments", "analytics"} {
		if s := r.Get(id); s == nil || !s.Wired {
			t.Errorf("ResetWired: %s should be Wired again, got %+v", id, s)
		}
	}
	for _, id := range []string{"identity", "notifications", "audit"} {
		if s := r.Get(id); s == nil || s.Wired {
			t.Errorf("ResetWired: %s should be unwired (stub), got %+v", id, s)
		}
	}
}

func TestResetWiredIdempotent(t *testing.T) {
	r := AllServices()
	r.ResetWired()
	r.ResetWired()
	if got := len(r.Wired()); got != 4 {
		t.Fatalf("double ResetWired left %d wired services, want 4", got)
	}
}