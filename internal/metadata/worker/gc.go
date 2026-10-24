package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"github.com/tharunn0/castor/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type OrphanScanner interface {
	ScanOrphanedChunks(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error)
}

type CommandApplier interface {
	Apply(cmd *consensus.Command, timeout time.Duration) (any, error)
}

type DataClientDialer func(ctx context.Context, target string) (castorv1.DataServiceClient, io.Closer, error)

func defaultDataClientDialer(_ context.Context, target string) (castorv1.DataServiceClient, io.Closer, error) {
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		telemetry.ClientDialOption(),
	)
	if err != nil {
		return nil, nil, err
	}
	return castorv1.NewDataServiceClient(conn), conn, nil
}

type GCMetrics struct {
	OrphansScanned prometheus.Counter
	ChunksPurged   prometheus.Counter
	ErrorsTotal    *prometheus.CounterVec
}

func NewGCMetrics(reg prometheus.Registerer) *GCMetrics {
	m := &GCMetrics{
		OrphansScanned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "castor_metadata_gc_orphans_scanned_total",
			Help: "Total count of orphaned chunks scanned by quarantine GC worker",
		}),
		ChunksPurged: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "castor_metadata_gc_chunks_purged_total",
			Help: "Total count of orphaned chunks successfully deleted from data nodes and consensus",
		}),
		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "castor_metadata_gc_errors_total",
			Help: "Total errors encountered during quarantine GC operations",
		}, []string{"op"}),
	}

	if reg != nil {
		_ = reg.Register(m.OrphansScanned)
		_ = reg.Register(m.ChunksPurged)
		_ = reg.Register(m.ErrorsTotal)
	}

	return m
}

type QuarantineGCWorker struct {
	scanner       OrphanScanner
	applier       CommandApplier
	dialer        DataClientDialer
	interval      time.Duration
	quarantineTTL time.Duration
	batchLimit    int
	pacingDelay   time.Duration
	applyTimeout  time.Duration
	callTimeout   time.Duration
	maxRetries    int
	logger        *slog.Logger
	metrics       *GCMetrics
}

type GCOption func(*QuarantineGCWorker)

func WithGCInterval(d time.Duration) GCOption {
	return func(w *QuarantineGCWorker) {
		if d > 0 {
			w.interval = d
		}
	}
}

func WithQuarantineTTL(d time.Duration) GCOption {
	return func(w *QuarantineGCWorker) {
		w.quarantineTTL = d
	}
}

func WithGCBatchLimit(limit int) GCOption {
	return func(w *QuarantineGCWorker) {
		if limit > 0 {
			w.batchLimit = limit
		}
	}
}

func WithPacingDelay(d time.Duration) GCOption {
	return func(w *QuarantineGCWorker) {
		w.pacingDelay = d
	}
}

func WithApplyTimeout(d time.Duration) GCOption {
	return func(w *QuarantineGCWorker) {
		if d > 0 {
			w.applyTimeout = d
		}
	}
}

func WithCallTimeout(d time.Duration) GCOption {
	return func(w *QuarantineGCWorker) {
		if d > 0 {
			w.callTimeout = d
		}
	}
}

func WithMaxRetries(retries int) GCOption {
	return func(w *QuarantineGCWorker) {
		if retries >= 0 {
			w.maxRetries = retries
		}
	}
}

func WithDataClientDialer(dialer DataClientDialer) GCOption {
	return func(w *QuarantineGCWorker) {
		if dialer != nil {
			w.dialer = dialer
		}
	}
}

func WithGCLogger(logger *slog.Logger) GCOption {
	return func(w *QuarantineGCWorker) {
		if logger != nil {
			w.logger = logger
		}
	}
}

func WithGCMetrics(metrics *GCMetrics) GCOption {
	return func(w *QuarantineGCWorker) {
		w.metrics = metrics
	}
}

func NewQuarantineGCWorker(scanner OrphanScanner, applier CommandApplier, opts ...GCOption) *QuarantineGCWorker {
	w := &QuarantineGCWorker{
		scanner:       scanner,
		applier:       applier,
		dialer:        defaultDataClientDialer,
		interval:      60 * time.Second,
		quarantineTTL: 24 * time.Hour,
		batchLimit:    100,
		pacingDelay:   10 * time.Millisecond,
		applyTimeout:  5 * time.Second,
		callTimeout:   5 * time.Second,
		maxRetries:    2,
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (w *QuarantineGCWorker) Name() string {
	return "quarantine-gc"
}

func (w *QuarantineGCWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				w.logger.Error("quarantine gc cycle failed", "error", err)
			}
		}
	}
}

func (w *QuarantineGCWorker) RunOnce(ctx context.Context) error {
	orphans, err := w.scanner.ScanOrphanedChunks(ctx, w.quarantineTTL, w.batchLimit)
	if err != nil {
		if w.metrics != nil {
			w.metrics.ErrorsTotal.WithLabelValues("scan").Inc()
		}
		return err
	}

	if len(orphans) == 0 {
		return nil
	}

	if w.metrics != nil {
		w.metrics.OrphansScanned.Add(float64(len(orphans)))
	}

	for _, orphan := range orphans {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if orphan == nil || orphan.ChunkHash == "" {
			continue
		}

		for _, nodeAddr := range orphan.Nodes {
			w.deleteChunkFromNode(ctx, nodeAddr, orphan.ChunkHash)

			if w.pacingDelay > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(w.pacingDelay):
				}
			}
		}

		cmd, err := consensus.NewRemoveChunkLocationsCommand(&castorv1.RemoveChunkLocationsRequest{
			ChunkHash:     orphan.ChunkHash,
			NodeAddresses: orphan.Nodes,
		})
		if err != nil {
			w.logger.Error("failed to create RemoveChunkLocations command", "hash", orphan.ChunkHash, "error", err)
			if w.metrics != nil {
				w.metrics.ErrorsTotal.WithLabelValues("encode_cmd").Inc()
			}
			continue
		}

		if _, err := w.applier.Apply(cmd, w.applyTimeout); err != nil {
			w.logger.Error("failed to apply RemoveChunkLocations to Raft", "hash", orphan.ChunkHash, "error", err)
			if w.metrics != nil {
				w.metrics.ErrorsTotal.WithLabelValues("apply_raft").Inc()
			}
			continue
		}

		if w.metrics != nil {
			w.metrics.ChunksPurged.Inc()
		}
		w.logger.Debug("quarantine gc purged chunk", "hash", orphan.ChunkHash)
	}

	return nil
}

func (w *QuarantineGCWorker) deleteChunkFromNode(ctx context.Context, nodeAddr, chunkHash string) {
	for attempt := range w.maxRetries + 1 {
		if ctx.Err() != nil {
			return
		}

		client, closer, err := w.dialer(ctx, nodeAddr)
		if err != nil {
			if attempt == w.maxRetries {
				w.logger.Warn("failed to dial data node for chunk deletion", "node", nodeAddr, "hash", chunkHash, "error", err)
				if w.metrics != nil {
					w.metrics.ErrorsTotal.WithLabelValues("dial").Inc()
				}
			}
			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, w.callTimeout)
		_, err = client.DeleteChunk(callCtx, &castorv1.DeleteChunkRequest{ChunkHash: chunkHash})
		cancel()
		if closer != nil {
			_ = closer.Close()
		}

		if err != nil {
			if status.Code(err) == codes.NotFound {
				return
			}
			if attempt == w.maxRetries {
				w.logger.Warn("failed to delete chunk from data node", "node", nodeAddr, "hash", chunkHash, "error", err)
				if w.metrics != nil {
					w.metrics.ErrorsTotal.WithLabelValues("delete_rpc").Inc()
				}
			}
			continue
		}

		return
	}
}
