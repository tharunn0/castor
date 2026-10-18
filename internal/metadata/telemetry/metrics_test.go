package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

func TestMetadataMetrics_Registration(t *testing.T) {
	telMetrics := telemetry.NewMetrics("test_metadata_metrics", "dev")
	metrics := NewMetadataMetrics("metadata-1", telMetrics)

	if metrics == nil {
		t.Fatal("expected non-nil MetadataMetrics")
	}

	metrics.IsLeader.Set(1)
	metrics.LeaderElections.Inc()
	metrics.CommitsTotal.WithLabelValues("create_bucket", "success").Inc()
	metrics.CommitsTotal.WithLabelValues("commit_manifest", "error").Inc()
	metrics.CommitDuration.WithLabelValues("commit_manifest").Observe(0.015)

	standaloneMetrics := NewMetadataMetrics("metadata-2", &telemetry.Metrics{Registry: prometheus.NewRegistry()})
	if standaloneMetrics == nil {
		t.Fatal("expected non-nil standalone MetadataMetrics")
	}

	standaloneMetrics.IsLeader.Set(0)
	standaloneMetrics.CommitsTotal.WithLabelValues("complete_mp", "success").Inc()
}
