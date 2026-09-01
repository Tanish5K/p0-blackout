package simulation

import "time"

// deriveMetrics recomputes the operational metrics from queue depths and pool
// utilisation. The model is intentionally simple but internally consistent:
// latency rises with queue depth and saturation, success rate falls when
// queues back up, and system health tracks the weakest service.
func deriveMetrics(s *GameState) {
	var depthTotal, depthMax int64
	workers := 0.0
	for _, q := range s.Queues {
		depthTotal += q.Depth
		if q.Depth > depthMax {
			depthMax = q.Depth
		}
	}
	for _, p := range s.Pools {
		workers += float64(p.Workers)
	}

	// Latency: baseline service time plus backlog-proportional queuing delay.
	// More consumers means more total throughput, so delay per unit backlog
	// shrinks as workers grow. workers is real telemetry (PoolManager + mgmt),
	// not a fabricated CapPerTick.
	base := 8 * time.Millisecond
	delay := time.Duration(float64(depthTotal)/maxf(workers, 1)*50) * time.Millisecond
	p50 := base + time.Duration(float64(delay)*0.4)
	p99 := base + time.Duration(float64(delay)*2.2)

	// Success rate degrades as the deepest queue backs up (drops start once a
	// queue is materially behind).
	success := 1.0
	if depthMax > 0 {
		over := float64(depthMax)
		success = 1.0 - 0.9*(over/(over+400.0))
	}
	if success < 0.05 {
		success = 0.05
	}

	// System health: worst service health drives it down; degraded queues
	// further erode it.
	health := s.Metrics.SystemHealth
	worst := 100.0
	for _, sv := range s.Services {
		if sv.Health < worst {
			worst = sv.Health
		}
	}
	health = worst
	if depthMax > 200 {
		health -= (float64(depthMax) - 200) / 20.0
	}
	if health < 0 {
		health = 0
	}

	s.Metrics.LatencyP50 = p50
	s.Metrics.LatencyP99 = p99
	s.Metrics.SuccessRate = success
	s.Metrics.SystemHealth = health
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
