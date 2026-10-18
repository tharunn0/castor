package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/telemetry"
)

// MetadataMetrics holds Prometheus metric collectors for metadata-svc operations.
type MetadataMetrics struct {
	IsLeader        prometheus.Gauge
	LeaderElections prometheus.Counter
	CommitsTotal    *prometheus.CounterVec
	CommitDuration  *prometheus.HistogramVec
}

// NewMetadataMetrics initializes and registers metadata-specific Prometheus metrics.
func NewMetadataMetrics(nodeID string, telMetrics *telemetry.Metrics) *MetadataMetrics {
	if nodeID == "" {
		nodeID = "unknown"
	}

	isLeaderVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "raft_is_leader",
			Help: "1 if this node is the active Raft leader, 0 otherwise",
		},
		[]string{"node_id"},
	)

	leaderElectionsVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raft_leader_elections_total",
			Help: "Detects cluster instability and frequent election cycles",
		},
		[]string{"node_id"},
	)

	commitsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "metadata_commits_total",
			Help: "Total mutations applied through consensus",
		},
		[]string{"type", "status"},
	)

	commitDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "metadata_commit_duration_seconds",
			Help:    "Time taken to serialize, replicate across majority, and apply to Badger",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	collectors := []prometheus.Collector{
		isLeaderVec,
		leaderElectionsVec,
		commitsTotal,
		commitDuration,
	}

	if telMetrics != nil {
		_ = telMetrics.Register(collectors...)
	} else {
		for _, c := range collectors {
			_ = prometheus.Register(c)
		}
	}

	isLeader := isLeaderVec.WithLabelValues(nodeID)
	isLeader.Set(0)
	leaderElections := leaderElectionsVec.WithLabelValues(nodeID)

	return &MetadataMetrics{
		IsLeader:        isLeader,
		LeaderElections: leaderElections,
		CommitsTotal:    commitsTotal,
		CommitDuration:  commitDuration,
	}
}
