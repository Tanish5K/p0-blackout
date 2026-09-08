package rabbitmq

import "sync"

// Service is a named Lumen subsystem in the ops room. Every service exists as
// a visible node from run one, but only a Wired service contributes RabbitMQ
// topology (exchanges/queues/bindings). Unwired services are stubs (§5.2): they
// report idle state and carry no MQ until their incident flips Wired on.
type Service struct {
	ID       string // stable id, e.g. "orders"
	Name     string // display name
	Critical bool   // Payments/Identity/Orders/Audit are critical
	Wired    bool   // has real MQ topology + consumers
	WiredIn  int    // campaign incident where Wired becomes true (0 = from start)
	Topology Topology
}

// Registry holds every known Lumen service, wired or not (§5.2 rollout). It is
// the single source of truth for which services currently have live MQ. Pool
// owners subscribe to Wired changes so toggling a service (e.g. a player pause)
// starts or stops its consumers without restarting the process.
type Registry struct {
	mu       sync.RWMutex
	Services []Service
	subs     []chan struct{}
}

// AllServices returns the full 7-service Lumen registry. The four core
// services (Gateway, Orders, Payments, Analytics) are Wired from incident 1
// (§5.1); Identity, Notifications, and Audit start as stubs and go live in
// incidents 2, 3, and 4 respectively.
func AllServices() *Registry {
	r := &Registry{Services: make([]Service, 0, 7)}
	r.Services = append(r.Services,
		Service{
			ID: "gateway", Name: "Gateway", Critical: true, Wired: true, WiredIn: 0,
			Topology: Topology{Exchanges: []Exchange{
				{Name: "gateway.events", Type: ExchangeTopic, Durable: true},
			}},
		},
		Service{
			ID: "orders", Name: "Orders", Critical: true, Wired: true, WiredIn: 0,
			Topology: Topology{
				Exchanges: []Exchange{
					{Name: "order.events", Type: ExchangeTopic, Durable: true},
				},
				Queues: []Queue{
					{Name: "orders.work", Durable: true},
				},
				Bindings: []Binding{
					{Queue: "orders.work", Exchange: "order.events", RoutingKey: "order.created"},
					{Queue: "analytics.events", Exchange: "order.events", RoutingKey: "order.created"},
				},
			},
		},
		Service{
			ID: "payments", Name: "Payments", Critical: true, Wired: true, WiredIn: 0,
			Topology: Topology{
				Exchanges: []Exchange{
					{Name: "payment.events", Type: ExchangeDirect, Durable: true},
				},
				Queues: []Queue{
					{Name: "payments.work", Durable: true},
				},
				Bindings: []Binding{
					{Queue: "payments.work", Exchange: "payment.events", RoutingKey: "payment.auth"},
				},
			},
		},
		Service{
			ID: "analytics", Name: "Analytics", Critical: false, Wired: true, WiredIn: 0,
			Topology: Topology{
				Queues: []Queue{
					{Name: "analytics.events", Durable: true},
				},
			},
		},
		Service{
			ID: "identity", Name: "Identity", Critical: true, Wired: false, WiredIn: 2,
		},
		Service{
			ID: "notifications", Name: "Notifications", Critical: false, Wired: false, WiredIn: 3,
		},
		Service{
			ID: "audit", Name: "Audit", Critical: true, Wired: false, WiredIn: 4,
		},
	)
	return r
}

// Get returns the current service with the given ID, or nil.
// The returned copy is a snapshot; mutate via SetWired.
func (r *Registry) Get(id string) *Service {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for i := range r.Services {
		if r.Services[i].ID == id {
			s := r.Services[i]
			return &s
		}
	}
	return nil
}

// SetWired toggles a service's live-MQ state and notifies subscribers if it
// changed. Returns true when the state actually flipped.
func (r *Registry) SetWired(id string, wired bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.Services {
		if r.Services[i].ID == id {
			if r.Services[i].Wired == wired {
				return false
			}
			r.Services[i].Wired = wired
			// Snapshot subscribers and notify outside the lock.
			for _, sub := range r.subs {
				select {
				case sub <- struct{}{}:
				default:
				}
			}
			return true
		}
	}
	return false
}

// Wired returns snapshots of the services currently marked Wired.
func (r *Registry) Wired() []Service {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Service, 0, len(r.Services))
	for _, s := range r.Services {
		if s.Wired {
			out = append(out, s)
		}
	}
	return out
}

// WiredTopology merges the topology snippets of Wired services. Unwired
// services contribute nothing, so no stray MQ resources exist for stubs.
func (r *Registry) WiredTopology() Topology {
	r.mu.RLock()
	defer r.mu.RUnlock()
	top := Topology{Name: "lumen"}
	for _, s := range r.Services {
		if !s.Wired {
			continue
		}
		top.Exchanges = append(top.Exchanges, s.Topology.Exchanges...)
		top.Queues = append(top.Queues, s.Topology.Queues...)
		top.Bindings = append(top.Bindings, s.Topology.Bindings...)
	}
	return top
}

// WiredQueues returns the distinct durable queue names owned by Wired
// services, in a stable order.
func (r *Registry) WiredQueues() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]bool{}
	var out []string
	for _, s := range r.Services {
		if !s.Wired {
			continue
		}
		for _, q := range s.Topology.Queues {
			if seen[q.Name] {
				continue
			}
			seen[q.Name] = true
			out = append(out, q.Name)
		}
	}
	return out
}

// HasWiredQueue reports whether queue belongs to a currently-Wired service.
// Pool-scaling targets fall back here: an unknown or unwired queue must be
// rejected rather than silently creating workers against a stub service.
func (r *Registry) HasWiredQueue(queue string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.Services {
		if !s.Wired {
			continue
		}
		for _, q := range s.Topology.Queues {
			if q.Name == queue {
				return true
			}
		}
	}
	return false
}

// ResetWired restores every service's Wired flag to its campaign default
// (WiredIn==0) and notifies subscribers. A paused/resumed service from a prior
// run must not leak its state into the next run's topology or pool set.
func (r *Registry) ResetWired() {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := false
	for i := range r.Services {
		want := r.Services[i].WiredIn == 0
		if r.Services[i].Wired != want {
			r.Services[i].Wired = want
			changed = true
		}
	}
	if changed {
		for _, sub := range r.subs {
			select {
			case sub <- struct{}{}:
			default:
			}
		}
	}
}

// Subscribe registers a channel that receives a broadcast whenever a Wired
// flag changes. The channel is buffered(1); slow consumers may miss a change
// and should reconcile by comparing desired vs actual state on any signal.
func (r *Registry) Subscribe() <-chan struct{} {
	ch := make(chan struct{}, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subs = append(r.subs, ch)
	return ch
}
