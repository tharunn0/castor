package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

// GatewayMetrics holds Prometheus metric collectors specific to gateway-svc operations.
type GatewayMetrics struct {
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	BufferPoolInUse prometheus.Gauge
	ActiveUploads   prometheus.Gauge
}

// NewGatewayMetrics initializes and registers gateway-specific Prometheus metrics
// into the provided telemetry registry (or default Prometheus registry if nil).
func NewGatewayMetrics(telMetrics *telemetry.Metrics) *GatewayMetrics {
	m := &GatewayMetrics{
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "s3",
				Name:      "requests_total",
				Help:      "Total S3 requests processed by gateway partitioned by method and status code class",
			},
			[]string{"method", "status_code_class"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "s3",
				Name:      "request_duration_seconds",
				Help:      "Latency of S3 operations processed by gateway in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method"},
		),
		BufferPoolInUse: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "s3",
				Name:      "buffer_pool_in_use_bytes",
				Help:      "Current memory in bytes allocated and in-use by the chunk buffer pool",
			},
		),
		ActiveUploads: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "s3",
				Name:      "active_in_flight_uploads",
				Help:      "Number of currently active in-flight upload operations",
			},
		),
	}

	collectors := []prometheus.Collector{
		m.RequestsTotal,
		m.RequestDuration,
		m.BufferPoolInUse,
		m.ActiveUploads,
	}

	if telMetrics != nil {
		_ = telMetrics.Register(collectors...)
	} else {
		for _, c := range collectors {
			_ = prometheus.Register(c)
		}
	}

	return m
}
