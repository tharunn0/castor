package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

type DataMetrics struct {
	ChunkWritesTotal        *prometheus.CounterVec
	ChunkWriteDuration      prometheus.Histogram
	ChecksumMismatchesTotal prometheus.Counter
	DiskCapacity            *prometheus.GaugeVec
}

func NewDataMetrics(nodeID string, telMetrics *telemetry.Metrics) *DataMetrics {
	writeBuckets := []float64{0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5}

	chunkWritesTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "storage_chunk_writes_total",
			Help: "Total chunk writes attempted and completed",
		},
		[]string{"status"},
	)

	chunkWriteDuration := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "storage_chunk_write_duration_seconds",
			Help:    "Storage-only latency (staging write, sha256, fsync, rename)",
			Buckets: writeBuckets,
		},
	)

	checksumMismatchesTotal := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "storage_checksum_mismatches_total",
			Help: "Critical alert counter for checksum mismatches",
		},
	)

	diskCapacity := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "storage_disk_capacity_bytes",
			Help: "Local filesystem utilization on data directory",
		},
		[]string{"type"},
	)

	collectors := []prometheus.Collector{
		chunkWritesTotal,
		chunkWriteDuration,
		checksumMismatchesTotal,
		diskCapacity,
	}

	if telMetrics != nil && telMetrics.Registry != nil {
		_ = telMetrics.Register(collectors...)
	} else {
		for _, c := range collectors {
			_ = prometheus.Register(c)
		}
	}

	return &DataMetrics{
		ChunkWritesTotal:        chunkWritesTotal,
		ChunkWriteDuration:      chunkWriteDuration,
		ChecksumMismatchesTotal: checksumMismatchesTotal,
		DiskCapacity:            diskCapacity,
	}
}
