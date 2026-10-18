package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

func TestGatewayMetrics_Registration(t *testing.T) {
	telMetrics := telemetry.NewMetrics("test_gw_metrics", "dev")
	gwMetrics := NewGatewayMetrics(telMetrics)

	if gwMetrics == nil {
		t.Fatal("expected non-nil GatewayMetrics")
	}

	gwMetrics.RequestsTotal.WithLabelValues("PUT", "2xx").Inc()
	gwMetrics.RequestsTotal.WithLabelValues("PUT", "5xx").Inc()
	gwMetrics.RequestDuration.WithLabelValues("PUT").Observe(0.025)
	gwMetrics.BufferPoolInUse.Set(4 * 1024 * 1024)
	gwMetrics.ActiveUploads.Inc()
	gwMetrics.ActiveUploads.Dec()

	// Registration on a fresh standalone registry
	standaloneMetrics := NewGatewayMetrics(&telemetry.Metrics{Registry: prometheus.NewRegistry()})
	if standaloneMetrics == nil {
		t.Fatal("expected non-nil standalone GatewayMetrics")
	}
}
