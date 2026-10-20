package storage

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tharunn0/castor/internal/data/telemetry"
	telpkg "github.com/tharunn0/castor/internal/telemetry"
)

func TestStorage_DiskStats(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := New(tmpDir, MaxChunkSize)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if store.RootDir() != tmpDir {
		t.Errorf("RootDir() = %q, want %q", store.RootDir(), tmpDir)
	}

	stats, err := store.GetDiskStats()
	if err != nil {
		t.Fatalf("GetDiskStats() failed: %v", err)
	}

	if stats.TotalBytes == 0 {
		t.Error("expected non-zero TotalBytes")
	}
	if stats.FreeBytes == 0 {
		t.Error("expected non-zero FreeBytes")
	}

	reg := prometheus.NewRegistry()
	metrics := telemetry.NewDataMetrics("test-node", &telpkg.Metrics{Registry: reg})

	if err := store.UpdateCapacityMetrics(metrics); err != nil {
		t.Fatalf("UpdateCapacityMetrics() failed: %v", err)
	}

	totalMetric := testutil.ToFloat64(metrics.DiskCapacity.WithLabelValues("total"))
	if totalMetric != float64(stats.TotalBytes) {
		t.Errorf("total metric = %f, want %f", totalMetric, float64(stats.TotalBytes))
	}
}
