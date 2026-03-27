package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	Published           prometheus.Counter
	PublishErrors       prometheus.Counter
	PublishDuration     prometheus.Histogram
	Consumed            prometheus.Counter
	ConsumeErrors       prometheus.Counter
	ConsumerLag         prometheus.Gauge
	RetryTotal          prometheus.Counter
	CircuitBreakerState prometheus.Gauge
	DeadLettered        prometheus.Counter
	ProcessingDuration  prometheus.Histogram
	ActiveWorkers       prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		Published: factory.NewCounter(prometheus.CounterOpts{
			Name: "stream_published_total",
			Help: "Total number of messages published to the stream.",
		}),
		PublishErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "stream_publish_errors_total",
			Help: "Total number of publish errors.",
		}),
		PublishDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "stream_publish_duration_seconds",
			Help:    "Duration of publish operations.",
			Buckets: prometheus.DefBuckets,
		}),
		Consumed: factory.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumed_total",
			Help: "Total number of messages consumed from the stream.",
		}),
		ConsumeErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "stream_consume_errors_total",
			Help: "Total number of consume errors.",
		}),
		ConsumerLag: factory.NewGauge(prometheus.GaugeOpts{
			Name: "stream_consumer_lag",
			Help: "Approximate number of pending messages in the stream.",
		}),
		RetryTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "stream_retry_total",
			Help: "Total number of message processing retry attempts.",
		}),
		CircuitBreakerState: factory.NewGauge(prometheus.GaugeOpts{
			Name: "stream_circuit_breaker_state",
			Help: "Current circuit breaker state: 0=closed 1=open 2=half-open.",
		}),
		DeadLettered: factory.NewCounter(prometheus.CounterOpts{
			Name: "stream_dead_lettered_total",
			Help: "Total number of messages moved to the dead letter stream.",
		}),
		ProcessingDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "stream_processing_duration_seconds",
			Help:    "Duration of message processing.",
			Buckets: prometheus.DefBuckets,
		}),
		ActiveWorkers: factory.NewGauge(prometheus.GaugeOpts{
			Name: "stream_active_workers",
			Help: "Number of currently active worker goroutines.",
		}),
	}
}
