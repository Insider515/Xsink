// breaker.go — обгортка над sony/gobreaker з експорту стану у Prometheus.
package breaker

import (
	"time"

	"github.com/sony/gobreaker/v2"

	"xsink/internal/metrics"
)

// New — конструктор іменованого CB. При зміні стану — оновлюємо gauge.
func New[T any](name string, maxFail uint32, openTimeout time.Duration) *gobreaker.CircuitBreaker[T] {
	settings := gobreaker.Settings{
		Name:        name,
		MaxRequests: 1,
		Interval:    60 * time.Second,
		Timeout:     openTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= maxFail
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			var v float64
			switch to {
			case gobreaker.StateClosed:
				v = 0
			case gobreaker.StateHalfOpen:
				v = 1
			case gobreaker.StateOpen:
				v = 2
			}
			metrics.BreakerState.WithLabelValues(name).Set(v)
		},
	}
	return gobreaker.NewCircuitBreaker[T](settings)
}
