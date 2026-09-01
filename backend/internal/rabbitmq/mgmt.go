package rabbitmq

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Mgmt is a minimal client for the RabbitMQ management HTTP plugin. It is used
// by the simulation bridge to read real queue depth, unacknowledged count,
// consumer count, and cumulative message_stats counters, which the AMQP
// protocol itself cannot expose. It is intentionally separate from the AMQP
// connection: a management plugin outage does not affect consume/publish, and
// the bridge must degrade to stale values rather than crash on HTTP failure.
type Mgmt struct {
	base string
	user string
	pass string
	hc   *http.Client
}

// NewMgmt builds a management client for the given base URL (e.g.
// http://localhost:15673). Empty base or creds are allowed; calls will fail,
// which the caller treats as "no telemetry, hold last-known value".
func NewMgmt(base, user, pass string) *Mgmt {
	return &Mgmt{
		base: strings.TrimRight(base, "/"),
		user: user,
		pass: pass,
		hc:   &http.Client{Timeout: 3 * time.Second},
	}
}

// QueueStats is a snapshot of a single queue from the management API.
type QueueStats struct {
	Messages        int64
	MessagesReady   int64
	MessagesUnacked int64
	Consumers       int
	// cumulative counters; diff across polls to get per-interval rates
	PublishCount uint64
	AckCount     uint64
	DeliverCount uint64
}

type mgmtQueue struct {
	Messages      int64 `json:"messages"`
	MessagesReady int64 `json:"messages_ready"`
	Unacked       int64 `json:"messages_unacknowledged"`
	Consumers     int   `json:"consumers"`
	Stats         *struct {
		PublishCount uint64 `json:"publish_count"`
		AckCount     uint64 `json:"ack_count"`
		DeliverCount uint64 `json:"deliver_count"`
	} `json:"message_stats"`
}

// QueueStats fetches stats for one queue in the given vhost. The vhost is
// percent-encoded (the default "/" becomes "%2f"). Returns an error on any
// HTTP or decode failure so the caller can hold last-known values.
func (m *Mgmt) QueueStats(vhost, name string) (QueueStats, error) {
	var out QueueStats
	if m.base == "" {
		return out, fmt.Errorf("management base URL not configured")
	}
	encVhost := url.PathEscape(vhost)
	encName := url.PathEscape(name)
	u := fmt.Sprintf("%s/api/queues/%s/%s", m.base, encVhost, encName)

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return out, err
	}
	req.SetBasicAuth(m.user, m.pass)

	resp, err := m.hc.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("management %s: status %d", u, resp.StatusCode)
	}

	var q mgmtQueue
	if err := json.NewDecoder(resp.Body).Decode(&q); err != nil {
		return out, fmt.Errorf("decode queue %q: %w", name, err)
	}
	out.Messages = q.Messages
	out.MessagesReady = q.MessagesReady
	out.MessagesUnacked = q.Unacked
	out.Consumers = q.Consumers
	if q.Stats != nil {
		out.PublishCount = q.Stats.PublishCount
		out.AckCount = q.Stats.AckCount
		out.DeliverCount = q.Stats.DeliverCount
	}
	return out, nil
}
