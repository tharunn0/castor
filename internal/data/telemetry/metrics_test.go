package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

func TestDataMetrics_Registration(t *testing.T) {
	telMetrics := telemetry.NewMetrics("test_data_metrics", "dev")
	metrics := NewDataMetrics("data-1", telMetrics)

	if metrics == nil {
		t.Fatal("expected non-nil DataMetrics")
	}

	metrics.ChunkWritesTotal.WithLabelValues("success").Inc()
	metrics.ChunkWritesTotal.WithLabelValues("error").Inc()
	metrics.ChunkWriteDuration.Observe(0.015)
	metrics.ChecksumMismatchesTotal.Inc()
	metrics.DiskCapacity.WithLabelValues("total").Set(1000)
	metrics.DiskCapacity.WithLabelValues("used").Set(400)
	metrics.DiskCapacity.WithLabelValues("free").Set(600)

	standaloneMetrics := NewDataMetrics("data-2", &telemetry.Metrics{Registry: prometheus.NewRegistry()})
	if standaloneMetrics == nil {
		t.Fatal("expected non-nil standalone DataMetrics")
	}

	standaloneMetrics.ChunkWritesTotal.WithLabelValues("success").Inc()
	standaloneMetrics.ChecksumMismatchesTotal.Inc()
}
