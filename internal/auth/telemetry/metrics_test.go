package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

func TestAuthMetrics_Registration(t *testing.T) {
	telMetrics := telemetry.NewMetrics("test_auth_metrics", "dev")
	authMetrics := NewAuthMetrics(telMetrics)

	if authMetrics == nil {
		t.Fatal("expected non-nil AuthMetrics")
	}

	authMetrics.LoginAttemptsTotal.WithLabelValues("success").Inc()
	authMetrics.LoginAttemptsTotal.WithLabelValues("failure").Inc()
	authMetrics.KeysCreatedTotal.Inc()
	authMetrics.KeysRevokedTotal.Inc()
	authMetrics.KeyValidationsTotal.WithLabelValues("valid").Inc()
	authMetrics.KeyValidationsTotal.WithLabelValues("invalid").Inc()
	authMetrics.KeyValidationsTotal.WithLabelValues("revoked").Inc()
	authMetrics.KeyValidationLatency.Observe(0.015)

	// Duplicate registration on a new registry should be clean
	standaloneMetrics := NewAuthMetrics(&telemetry.Metrics{Registry: prometheus.NewRegistry()})
	if standaloneMetrics == nil {
		t.Fatal("expected non-nil standalone AuthMetrics")
	}
}
