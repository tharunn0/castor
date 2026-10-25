package worker

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/consensus"
)

type UnderReplicatedScanner interface {
	ScanUnderReplicatedChunks(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error)
	GetChunkLocation(ctx context.Context, sha256 string) (*castorv1.ChunkLocationRecord, error)
}

type ActiveNodeProvider interface {
	GetActiveNodes() []*castorv1.NodeCapacity
}

type HealerMetrics struct {
	ChunksScanned  prometheus.Counter
	ChunksRepaired prometheus.Counter
	ErrorsTotal    *prometheus.CounterVec
}

func NewHealerMetrics(reg prometheus.Registerer) *HealerMetrics {
	m := &HealerMetrics{
		ChunksScanned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "castor_metadata_healer_chunks_scanned_total",
			Help: "Total count of under-replicated chunks scanned by replica healer",
		}),
		ChunksRepaired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "castor_metadata_healer_chunks_repaired_total",
			Help: "Total count of under-replicated chunks successfully replicated and updated in consensus",
		}),
		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "castor_metadata_healer_errors_total",
			Help: "Total errors encountered during replica healer operations",
		}, []string{"op"}),
	}

	if reg != nil {
		_ = reg.Register(m.ChunksScanned)
		_ = reg.Register(m.ChunksRepaired)
		_ = reg.Register(m.ErrorsTotal)
	}

	return m
}

type ActiveReplicaHealer struct {
	scanner        UnderReplicatedScanner
	nodeProvider   ActiveNodeProvider
	applier        CommandApplier
	dialer         DataClientDialer
	interval       time.Duration
	targetReplicas int
	batchLimit     int
	pacingDelay    time.Duration
	applyTimeout   time.Duration
	callTimeout    time.Duration
	logger         *slog.Logger
	metrics        *HealerMetrics
	triggerCh      chan string
}

type HealerOption func(*ActiveReplicaHealer)

func WithHealInterval(d time.Duration) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if d > 0 {
			h.interval = d
		}
	}
}

func WithTargetReplicas(n int) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if n > 0 {
			h.targetReplicas = n
		}
	}
}

func WithHealerBatchLimit(limit int) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if limit > 0 {
			h.batchLimit = limit
		}
	}
}

func WithHealerPacingDelay(d time.Duration) HealerOption {
	return func(h *ActiveReplicaHealer) {
		h.pacingDelay = d
	}
}

func WithHealerApplyTimeout(d time.Duration) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if d > 0 {
			h.applyTimeout = d
		}
	}
}

func WithHealerCallTimeout(d time.Duration) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if d > 0 {
			h.callTimeout = d
		}
	}
}

func WithHealerDialer(dialer DataClientDialer) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if dialer != nil {
			h.dialer = dialer
		}
	}
}

func WithHealerLogger(logger *slog.Logger) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if logger != nil {
			h.logger = logger
		}
	}
}

func WithHealerMetrics(metrics *HealerMetrics) HealerOption {
	return func(h *ActiveReplicaHealer) {
		h.metrics = metrics
	}
}

func WithTriggerBufferSize(size int) HealerOption {
	return func(h *ActiveReplicaHealer) {
		if size > 0 {
			h.triggerCh = make(chan string, size)
		}
	}
}

func NewActiveReplicaHealer(scanner UnderReplicatedScanner, nodeProvider ActiveNodeProvider, applier CommandApplier, opts ...HealerOption) *ActiveReplicaHealer {
	h := &ActiveReplicaHealer{
		scanner:        scanner,
		nodeProvider:   nodeProvider,
		applier:        applier,
		dialer:         defaultDataClientDialer,
		interval:       30 * time.Second,
		targetReplicas: 3,
		batchLimit:     50,
		pacingDelay:    10 * time.Millisecond,
		applyTimeout:   5 * time.Second,
		callTimeout:    10 * time.Second,
		logger:         slog.Default(),
		triggerCh:      make(chan string, 100),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *ActiveReplicaHealer) Name() string {
	return "active-replica-healer"
}

func (h *ActiveReplicaHealer) TriggerRepair(chunkHash string) {
	if chunkHash == "" {
		return
	}
	select {
	case h.triggerCh <- chunkHash:
	default:
	}
}

func (h *ActiveReplicaHealer) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				h.logger.Error("replica healer cycle failed", "error", err)
			}
		case hash := <-h.triggerCh:
			if err := h.HealChunk(ctx, hash); err != nil && !errors.Is(err, context.Canceled) {
				h.logger.Error("on-demand chunk healing failed", "hash", hash, "error", err)
			}
		}
	}
}

func (h *ActiveReplicaHealer) RunOnce(ctx context.Context) error {
	underReplicated, err := h.scanner.ScanUnderReplicatedChunks(ctx, h.targetReplicas, h.batchLimit)
	if err != nil {
		if h.metrics != nil {
			h.metrics.ErrorsTotal.WithLabelValues("scan").Inc()
		}
		return err
	}

	if len(underReplicated) == 0 {
		return nil
	}

	if h.metrics != nil {
		h.metrics.ChunksScanned.Add(float64(len(underReplicated)))
	}

	for _, chunk := range underReplicated {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := h.healRecord(ctx, chunk); err != nil && !errors.Is(err, context.Canceled) {
			h.logger.Warn("failed to heal chunk", "hash", chunk.ChunkHash, "error", err)
		}
	}

	return nil
}

func (h *ActiveReplicaHealer) HealChunk(ctx context.Context, chunkHash string) error {
	if chunkHash == "" {
		return nil
	}
	record, err := h.scanner.GetChunkLocation(ctx, chunkHash)
	if err != nil {
		if h.metrics != nil {
			h.metrics.ErrorsTotal.WithLabelValues("get_location").Inc()
		}
		return err
	}
	if record == nil || record.RefCount == 0 || len(record.Nodes) >= h.targetReplicas {
		return nil
	}

	return h.healRecord(ctx, record)
}

func (h *ActiveReplicaHealer) healRecord(ctx context.Context, chunk *castorv1.ChunkLocationRecord) error {
	if chunk == nil || chunk.ChunkHash == "" || len(chunk.Nodes) >= h.targetReplicas {
		return nil
	}

	activeNodes := h.nodeProvider.GetActiveNodes()
	if len(activeNodes) == 0 {
		return nil
	}

	activeMap := make(map[string]*castorv1.NodeCapacity, len(activeNodes))
	for _, n := range activeNodes {
		if n.Status != castorv1.NodeStatus_NODE_STATUS_OFFLINE {
			activeMap[n.GrpcAddress] = n
		}
	}

	var sourceNode string
	for _, n := range chunk.Nodes {
		if _, ok := activeMap[n]; ok {
			sourceNode = n
			break
		}
	}
	if sourceNode == "" {
		h.logger.Warn("cannot heal chunk: no active replica source node available", "hash", chunk.ChunkHash)
		return nil
	}

	existingNodes := make(map[string]bool, len(chunk.Nodes))
	for _, n := range chunk.Nodes {
		existingNodes[n] = true
	}

	var candidates []*castorv1.NodeCapacity
	for _, n := range activeNodes {
		if !existingNodes[n.GrpcAddress] && n.Status != castorv1.NodeStatus_NODE_STATUS_OFFLINE {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		h.logger.Warn("cannot heal chunk: no candidate target nodes available", "hash", chunk.ChunkHash)
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].FreeBytes > candidates[j].FreeBytes
	})

	needed := min(len(candidates), h.targetReplicas-len(chunk.Nodes))

	for i := range needed {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		target := candidates[i]
		if err := h.replicateToTarget(ctx, chunk.ChunkHash, sourceNode, target.GrpcAddress); err != nil {
			h.logger.Warn("replication to target failed", "target", target.GrpcAddress, "hash", chunk.ChunkHash, "error", err)
			continue
		}

		if h.pacingDelay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(h.pacingDelay):
			}
		}
	}

	return nil
}

func (h *ActiveReplicaHealer) replicateToTarget(ctx context.Context, chunkHash, sourceAddr, targetAddr string) error {
	client, closer, err := h.dialer(ctx, targetAddr)
	if err != nil {
		if h.metrics != nil {
			h.metrics.ErrorsTotal.WithLabelValues("dial").Inc()
		}
		return err
	}

	callCtx, cancel := context.WithTimeout(ctx, h.callTimeout)
	resp, err := client.ReplicateChunk(callCtx, &castorv1.ReplicateChunkRequest{
		ChunkHash:         chunkHash,
		SourceNodeAddress: sourceAddr,
	})
	cancel()
	if closer != nil {
		_ = closer.Close()
	}

	if err != nil {
		if h.metrics != nil {
			h.metrics.ErrorsTotal.WithLabelValues("replicate_rpc").Inc()
		}
		return err
	}

	if resp == nil || !resp.Success {
		if h.metrics != nil {
			h.metrics.ErrorsTotal.WithLabelValues("replicate_failed").Inc()
		}
		return errors.New("target node reported replication unsuccessful")
	}

	cmd, err := consensus.NewUpdateChunkLocationCommand(&castorv1.UpdateChunkLocationRequest{
		ChunkHash:     chunkHash,
		NodeAddresses: []string{targetAddr},
	})
	if err != nil {
		if h.metrics != nil {
			h.metrics.ErrorsTotal.WithLabelValues("encode_cmd").Inc()
		}
		return err
	}

	if _, err := h.applier.Apply(cmd, h.applyTimeout); err != nil {
		if h.metrics != nil {
			h.metrics.ErrorsTotal.WithLabelValues("apply_raft").Inc()
		}
		return err
	}

	if h.metrics != nil {
		h.metrics.ChunksRepaired.Inc()
	}
	h.logger.Debug("chunk replica healed successfully", "hash", chunkHash, "target", targetAddr)
	return nil
}
