package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

// AuthMetrics holds Prometheus metric collectors specific to auth-svc operations.
type AuthMetrics struct {
	LoginAttemptsTotal   *prometheus.CounterVec
	KeysCreatedTotal     prometheus.Counter
	KeysRevokedTotal     prometheus.Counter
	KeyValidationsTotal  *prometheus.CounterVec
	KeyValidationLatency prometheus.Histogram
}

// NewAuthMetrics initializes and registers auth-specific Prometheus metrics
// into the provided telemetry registry (or default Prometheus registry if nil).
func NewAuthMetrics(telMetrics *telemetry.Metrics) *AuthMetrics {
	m := &AuthMetrics{
		LoginAttemptsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "castor_auth",
				Name:      "login_attempts_total",
				Help:      "Total login attempts partitioned by status (success or failure)",
			},
			[]string{"status"},
		),
		KeysCreatedTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "castor_auth",
				Name:      "keys_created_total",
				Help:      "Total S3 credentials created",
			},
		),
		KeysRevokedTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "castor_auth",
				Name:      "keys_revoked_total",
				Help:      "Total S3 credentials revoked",
			},
		),
		KeyValidationsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "castor_auth",
				Name:      "key_validations_total",
				Help:      "Total S3 credential validations partitioned by result (valid, invalid, revoked)",
			},
			[]string{"result"},
		),
		KeyValidationLatency: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "castor_auth",
				Name:      "key_validation_duration_seconds",
				Help:      "Latency of internal S3 credential validation in seconds",
				Buckets:   prometheus.DefBuckets,
			},
		),
	}

	collectors := []prometheus.Collector{
		m.LoginAttemptsTotal,
		m.KeysCreatedTotal,
		m.KeysRevokedTotal,
		m.KeyValidationsTotal,
		m.KeyValidationLatency,
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
