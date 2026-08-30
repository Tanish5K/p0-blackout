package rabbitmq

// Service is a named Lumen subsystem in the ops room. Every service exists as
// a visible node from run one, but only a Wired service contributes RabbitMQ
// topology (exchanges/queues/bindings). Unwired services are stubs (§5.2): they
// report idle state and carry no MQ until their incident flips Wired on.
//
// Going "live" in a later incident is flipping Wired=true and appending the
// service's topology snippet — no registry retrofit needed.
type Service struct {
	ID         string
	Name       string
	Critical   bool   // Payments/Identity/Orders/Audit are critical
	Wired      bool
	WiredIn    int    // campaign incident where Wired becomes true (0 = from start)
	Topology   Topology // MQ resources this service owns (only meaningfully declared when Wired)
}

// Registry holds every known Lumen service, wired or not (§5.2 rollout).
type Registry struct {
	Services []Service
}

// AllServices returns the full 7-service Lumen registry. The four core
// services (Gateway, Orders, Payments, Analytics) are Wired from incident 1
// (§5.1); Identity, Notifications, and Audit start as stubs and go live in
// incidents 2, 3, and 4 respectively.
func AllServices() Registry {
	reg := Registry{Services: make([]Service, 0, 7)}
	reg.Services = append(reg.Services,
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
	return reg
}

// Get returns the service with the given ID, or nil.
func (r Registry) Get(id string) *Service {
	for i := range r.Services {
		if r.Services[i].ID == id {
			return &r.Services[i]
		}
	}
	return nil
}

// Wired returns the services currently marked Wired.
func (r Registry) Wired() []Service {
	out := make([]Service, 0, len(r.Services))
	for _, s := range r.Services {
		if s.Wired {
			out = append(out, s)
		}
	}
	return out
}

// WiredTopology merges the topology snippets of every Wired service into a
// single topology suitable for Declare. Unwired services contribute nothing,
// so no stray exchanges/queues/bindings are created for stub nodes.
func (r Registry) WiredTopology() Topology {
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
