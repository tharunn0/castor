package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"github.com/tharunn0/castor/internal/metadata/registry"
	"github.com/tharunn0/castor/internal/metadata/store"
	"github.com/tharunn0/castor/internal/metadata/telemetry"
	telemetryPkg "github.com/tharunn0/castor/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RaftApplier specifies consensus operations required by the metadata gRPC server.
type RaftApplier interface {
	Apply(cmd *consensus.Command, timeout time.Duration) (any, error)
	IsLeader() bool
	LeaderAddr() string
}

type Server struct {
	castorv1.UnimplementedMetadataServiceServer
	store    *store.Store
	raft     RaftApplier
	registry *registry.Registry
	timeout  time.Duration
	logger   *slog.Logger
	tracer   trace.Tracer
	metrics  *telemetry.MetadataMetrics
}

type Option func(*Server)

func WithTimeout(d time.Duration) Option {
	return func(s *Server) {
		s.timeout = d
	}
}

func WithRegistry(reg *registry.Registry) Option {
	return func(s *Server) {
		s.registry = reg
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) {
		s.logger = logger
	}
}

func WithTracer(tracer trace.Tracer) Option {
	return func(s *Server) {
		s.tracer = tracer
	}
}

func WithMetrics(metrics *telemetry.MetadataMetrics) Option {
	return func(s *Server) {
		s.metrics = metrics
	}
}

func New(s *store.Store, r RaftApplier, opts ...Option) *Server {
	defaultMetrics := telemetry.NewMetadataMetrics("server", &telemetryPkg.Metrics{
		Registry: prometheus.NewRegistry(),
	})

	srv := &Server{
		store:    s,
		raft:     r,
		registry: registry.New(),
		timeout:  5 * time.Second,
		logger:   slog.Default(),
		tracer:   otel.GetTracerProvider().Tracer("metadata-svc"),
		metrics:  defaultMetrics,
	}
	for _, opt := range opts {
		opt(srv)
	}
	return srv
}

func (s *Server) apply(ctx context.Context, cmd *consensus.Command) (any, error) {
	if s.raft == nil {
		return nil, status.Error(codes.Unavailable, "raft consensus not configured")
	}
	timeout := s.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	res, err := s.raft.Apply(cmd, timeout)
	if err != nil {
		return nil, err
	}
	if applyErr, ok := res.(error); ok && applyErr != nil {
		return nil, applyErr
	}
	return res, nil
}

func (s *Server) CreateBucket(ctx context.Context, req *castorv1.CreateBucketMetadataRequest) (*castorv1.CreateBucketMetadataResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Metadata/CreateBucket",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("s3.bucket", req.GetBucket()),
		),
	)
	defer span.End()

	if req.GetBucket() == "" {
		err := status.Error(codes.InvalidArgument, "bucket name is required")
		span.RecordError(err)
		return nil, err
	}
	cmd, err := consensus.NewCreateBucketCommand(req)
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "encode create bucket command: %v", err)
	}

	start := time.Now()
	res, err := s.apply(ctx, cmd)
	duration := time.Since(start)
	s.metrics.CommitDuration.WithLabelValues("create_bucket").Observe(duration.Seconds())

	if err != nil {
		s.metrics.CommitsTotal.WithLabelValues("create_bucket", "error").Inc()
		s.logger.ErrorContext(ctx, "failed to apply metadata command",
			"operation", "create_bucket",
			"bucket", req.GetBucket(),
			"error", err,
		)
		grpcErr := toGRPCError(err)
		span.RecordError(grpcErr)
		return nil, grpcErr
	}

	s.metrics.CommitsTotal.WithLabelValues("create_bucket", "success").Inc()
	s.logger.InfoContext(ctx, "bucket created successfully", "bucket", req.GetBucket())

	record, ok := res.(*castorv1.BucketRecord)
	if !ok || record == nil {
		return &castorv1.CreateBucketMetadataResponse{Created: true}, nil
	}
	return &castorv1.CreateBucketMetadataResponse{
		Created:   true,
		CreatedAt: record.CreatedAt,
	}, nil
}

func (s *Server) DeleteBucket(ctx context.Context, req *castorv1.DeleteBucketMetadataRequest) (*castorv1.DeleteBucketMetadataResponse, error) {
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}
	cmd, err := consensus.NewDeleteBucketCommand(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode delete bucket command: %v", err)
	}
	if _, err := s.apply(ctx, cmd); err != nil {
		return nil, toGRPCError(err)
	}
	s.logger.InfoContext(ctx, "bucket deleted successfully", "bucket", req.GetBucket())
	return &castorv1.DeleteBucketMetadataResponse{Deleted: true}, nil
}

func (s *Server) ListBuckets(ctx context.Context, req *castorv1.ListBucketsMetadataRequest) (*castorv1.ListBucketsMetadataResponse, error) {
	buckets, err := s.store.ListBuckets(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &castorv1.ListBucketsMetadataResponse{Buckets: buckets}, nil
}

func (s *Server) CheckBucketExists(ctx context.Context, req *castorv1.CheckBucketExistsRequest) (*castorv1.CheckBucketExistsResponse, error) {
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}
	exists, err := s.store.CheckBucketExists(ctx, req.GetBucket())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &castorv1.CheckBucketExistsResponse{Exists: exists}, nil
}

func (s *Server) CheckChunks(ctx context.Context, req *castorv1.CheckChunksRequest) (*castorv1.CheckChunksResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Metadata/CheckChunks",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.Int("chunks.count", len(req.GetChunkHashes())),
		),
	)
	defer span.End()

	if len(req.GetChunkHashes()) == 0 {
		return &castorv1.CheckChunksResponse{
			ExistingChunks: make(map[string]bool),
		}, nil
	}

	existsMap, err := s.store.CheckChunks(ctx, req.GetChunkHashes())
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to check chunks", "error", err)
		grpcErr := toGRPCError(err)
		span.RecordError(grpcErr)
		return nil, grpcErr
	}

	return &castorv1.CheckChunksResponse{
		ExistingChunks: existsMap,
	}, nil
}

func (s *Server) CommitManifest(ctx context.Context, req *castorv1.CommitManifestRequest) (*castorv1.CommitManifestResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Metadata/CommitManifest",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("s3.bucket", req.GetBucket()),
			attribute.String("s3.key", req.GetKey()),
			attribute.Int64("s3.size_bytes", req.GetSize()),
			attribute.Int("s3.parts", len(req.GetChunkIds())),
		),
	)
	defer span.End()

	if req.GetBucket() == "" {
		err := status.Error(codes.InvalidArgument, "bucket name is required")
		span.RecordError(err)
		return nil, err
	}
	if req.GetKey() == "" {
		err := status.Error(codes.InvalidArgument, "object key is required")
		span.RecordError(err)
		return nil, err
	}
	cmd, err := consensus.NewCommitManifestCommand(req)
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "encode commit manifest command: %v", err)
	}

	start := time.Now()
	res, err := s.apply(ctx, cmd)
	duration := time.Since(start)
	s.metrics.CommitDuration.WithLabelValues("commit_manifest").Observe(duration.Seconds())

	if err != nil {
		s.metrics.CommitsTotal.WithLabelValues("commit_manifest", "error").Inc()
		s.logger.ErrorContext(ctx, "failed to apply metadata command",
			"operation", "commit_manifest",
			"bucket", req.GetBucket(),
			"key", req.GetKey(),
			"error", err,
		)
		grpcErr := toGRPCError(err)
		span.RecordError(grpcErr)
		return nil, grpcErr
	}

	s.metrics.CommitsTotal.WithLabelValues("commit_manifest", "success").Inc()
	s.logger.DebugContext(ctx, "manifest committed successfully",
		"bucket", req.GetBucket(),
		"key", req.GetKey(),
		"size_bytes", req.GetSize(),
	)

	record, ok := res.(*castorv1.ManifestRecord)
	if !ok || record == nil {
		return &castorv1.CommitManifestResponse{Committed: true}, nil
	}
	return &castorv1.CommitManifestResponse{
		Committed:   true,
		CommittedAt: record.CreatedAt,
	}, nil
}

func (s *Server) GetManifest(ctx context.Context, req *castorv1.GetManifestRequest) (*castorv1.GetManifestResponse, error) {
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "object key is required")
	}
	manifest, err := s.store.GetManifest(ctx, req.GetBucket(), req.GetKey())
	if err != nil {
		return nil, toGRPCError(err)
	}

	chunks := make([]*castorv1.ChunkWithLocations, 0, len(manifest.ChunkIds))
	for _, chunkID := range manifest.ChunkIds {
		loc, err := s.store.GetChunkLocation(ctx, chunkID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				chunks = append(chunks, &castorv1.ChunkWithLocations{
					ChunkHash: chunkID,
				})
				continue
			}
			return nil, toGRPCError(err)
		}
		chunks = append(chunks, &castorv1.ChunkWithLocations{
			ChunkHash:     chunkID,
			NodeAddresses: loc.Nodes,
			Size:          loc.Size,
		})
	}

	return &castorv1.GetManifestResponse{
		Bucket:      manifest.Bucket,
		Key:         manifest.Key,
		Size:        manifest.Size,
		Etag:        manifest.Etag,
		ContentType: manifest.ContentType,
		Status:      manifest.Status,
		CreatedAt:   manifest.CreatedAt,
		UpdatedAt:   manifest.UpdatedAt,
		Chunks:      chunks,
		OwnerId:     manifest.OwnerId,
	}, nil
}

func (s *Server) DeleteManifest(ctx context.Context, req *castorv1.DeleteManifestRequest) (*castorv1.DeleteManifestResponse, error) {
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "object key is required")
	}
	cmd, err := consensus.NewDeleteManifestCommand(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode delete manifest command: %v", err)
	}
	if _, err := s.apply(ctx, cmd); err != nil {
		return nil, toGRPCError(err)
	}
	return &castorv1.DeleteManifestResponse{Deleted: true}, nil
}

func (s *Server) ListManifests(ctx context.Context, req *castorv1.ListManifestsRequest) (*castorv1.ListManifestsResponse, error) {
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}
	res, err := s.store.ListManifests(ctx, req.GetBucket(), req.GetPrefix(), req.GetDelimiter(), req.GetMarker(), req.GetMaxKeys())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &castorv1.ListManifestsResponse{
		Manifests:      res.Manifests,
		CommonPrefixes: res.CommonPrefixes,
		NextMarker:     res.NextMarker,
		IsTruncated:    res.IsTruncated,
	}, nil
}

func (s *Server) InitiateMultipart(ctx context.Context, req *castorv1.InitiateMultipartMetaRequest) (*castorv1.InitiateMultipartMetaResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Metadata/InitiateMultipart",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("s3.bucket", req.GetBucket()),
			attribute.String("s3.key", req.GetKey()),
		),
	)
	defer span.End()

	if req.GetBucket() == "" {
		err := status.Error(codes.InvalidArgument, "bucket name is required")
		span.RecordError(err)
		return nil, err
	}
	if req.GetKey() == "" {
		err := status.Error(codes.InvalidArgument, "object key is required")
		span.RecordError(err)
		return nil, err
	}
	if req.GetUploadId() == "" {
		err := status.Error(codes.InvalidArgument, "upload_id is required")
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.String("s3.upload_id", req.GetUploadId()))

	cmd, err := consensus.NewInitiateMultipartCommand(req)
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "encode initiate multipart command: %v", err)
	}

	start := time.Now()
	res, err := s.apply(ctx, cmd)
	duration := time.Since(start)
	s.metrics.CommitDuration.WithLabelValues("initiate_mp").Observe(duration.Seconds())

	if err != nil {
		s.metrics.CommitsTotal.WithLabelValues("initiate_mp", "error").Inc()
		s.logger.ErrorContext(ctx, "failed to apply metadata command",
			"operation", "initiate_mp",
			"bucket", req.GetBucket(),
			"key", req.GetKey(),
			"upload_id", req.GetUploadId(),
			"error", err,
		)
		grpcErr := toGRPCError(err)
		span.RecordError(grpcErr)
		return nil, grpcErr
	}

	s.metrics.CommitsTotal.WithLabelValues("initiate_mp", "success").Inc()
	s.logger.DebugContext(ctx, "multipart upload initiated",
		"bucket", req.GetBucket(),
		"key", req.GetKey(),
		"upload_id", req.GetUploadId(),
	)

	record, ok := res.(*castorv1.MultipartRecord)
	if !ok || record == nil {
		return &castorv1.InitiateMultipartMetaResponse{UploadId: req.GetUploadId()}, nil
	}
	return &castorv1.InitiateMultipartMetaResponse{
		UploadId:  record.UploadId,
		CreatedAt: record.CreatedAt,
	}, nil
}

func (s *Server) CommitPart(ctx context.Context, req *castorv1.CommitPartMetaRequest) (*castorv1.CommitPartMetaResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Metadata/CommitPart",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("s3.upload_id", req.GetUploadId()),
			attribute.Int64("s3.part_number", int64(req.GetPartNumber())),
		),
	)
	defer span.End()

	if req.GetUploadId() == "" {
		err := status.Error(codes.InvalidArgument, "upload_id is required")
		span.RecordError(err)
		return nil, err
	}
	if req.GetPartNumber() <= 0 {
		err := status.Error(codes.InvalidArgument, "part_number must be greater than zero")
		span.RecordError(err)
		return nil, err
	}
	if req.GetEtag() == "" {
		err := status.Error(codes.InvalidArgument, "etag is required")
		span.RecordError(err)
		return nil, err
	}

	cmd, err := consensus.NewCommitPartCommand(req)
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "encode commit part command: %v", err)
	}

	start := time.Now()
	res, err := s.apply(ctx, cmd)
	duration := time.Since(start)
	s.metrics.CommitDuration.WithLabelValues("commit_part").Observe(duration.Seconds())

	if err != nil {
		s.metrics.CommitsTotal.WithLabelValues("commit_part", "error").Inc()
		s.logger.ErrorContext(ctx, "failed to apply metadata command",
			"operation", "commit_part",
			"upload_id", req.GetUploadId(),
			"part_number", req.GetPartNumber(),
			"error", err,
		)
		grpcErr := toGRPCError(err)
		span.RecordError(grpcErr)
		return nil, grpcErr
	}

	s.metrics.CommitsTotal.WithLabelValues("commit_part", "success").Inc()
	s.logger.DebugContext(ctx, "part committed successfully",
		"upload_id", req.GetUploadId(),
		"part_number", req.GetPartNumber(),
		"etag", req.GetEtag(),
	)

	part, ok := res.(*castorv1.PartRecord)
	etag := req.GetEtag()
	if ok && part != nil && part.Etag != "" {
		etag = part.Etag
	}
	return &castorv1.CommitPartMetaResponse{
		Committed: true,
		Etag:      etag,
	}, nil
}

func (s *Server) CompleteMultipart(ctx context.Context, req *castorv1.CompleteMultipartMetaRequest) (*castorv1.CommitManifestResponse, error) {
	ctx, span := s.tracer.Start(ctx, "Metadata/CompleteMultipart",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("s3.upload_id", req.GetUploadId()),
			attribute.Int("s3.total_parts", len(req.GetParts())),
		),
	)
	defer span.End()

	if req.GetUploadId() == "" {
		err := status.Error(codes.InvalidArgument, "upload_id is required")
		span.RecordError(err)
		return nil, err
	}

	cmd, err := consensus.NewCompleteMultipartCommand(req)
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "encode complete multipart command: %v", err)
	}

	start := time.Now()
	res, err := s.apply(ctx, cmd)
	duration := time.Since(start)
	s.metrics.CommitDuration.WithLabelValues("complete_mp").Observe(duration.Seconds())

	if err != nil {
		s.metrics.CommitsTotal.WithLabelValues("complete_mp", "error").Inc()
		s.logger.ErrorContext(ctx, "failed to apply metadata command",
			"operation", "complete_mp",
			"upload_id", req.GetUploadId(),
			"error", err,
		)
		grpcErr := toGRPCError(err)
		span.RecordError(grpcErr)
		return nil, grpcErr
	}

	s.metrics.CommitsTotal.WithLabelValues("complete_mp", "success").Inc()

	record, ok := res.(*castorv1.ManifestRecord)
	if !ok || record == nil {
		return &castorv1.CommitManifestResponse{Committed: true}, nil
	}

	span.SetAttributes(
		attribute.String("s3.bucket", record.Bucket),
		attribute.String("s3.key", record.Key),
	)
	s.logger.DebugContext(ctx, "multipart upload completed",
		"upload_id", req.GetUploadId(),
		"bucket", record.Bucket,
		"key", record.Key,
	)

	return &castorv1.CommitManifestResponse{
		Committed:   true,
		CommittedAt: record.CreatedAt,
	}, nil
}

func (s *Server) AbortMultipart(ctx context.Context, req *castorv1.AbortMultipartMetaRequest) (*castorv1.AbortMultipartMetaResponse, error) {
	if req.GetUploadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "upload_id is required")
	}
	cmd, err := consensus.NewAbortMultipartCommand(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode abort multipart command: %v", err)
	}
	if _, err := s.apply(ctx, cmd); err != nil {
		return nil, toGRPCError(err)
	}
	return &castorv1.AbortMultipartMetaResponse{Aborted: true}, nil
}

func (s *Server) RegisterNode(ctx context.Context, req *castorv1.RegisterNodeRequest) (*castorv1.RegisterNodeResponse, error) {
	if !s.raft.IsLeader() {
		return nil, toGRPCError(&consensus.NotLeaderError{LeaderAddr: s.raft.LeaderAddr()})
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.GetGrpcAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "grpc_address is required")
	}
	if err := s.registry.Register(req.GetNodeId(), req.GetGrpcAddress(), req.GetTotalBytes(), req.GetFreeBytes()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &castorv1.RegisterNodeResponse{Registered: true}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *castorv1.HeartbeatRequest) (*castorv1.HeartbeatResponse, error) {
	if !s.raft.IsLeader() {
		return nil, toGRPCError(&consensus.NotLeaderError{LeaderAddr: s.raft.LeaderAddr()})
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	var ts time.Time
	if req.GetTimestamp() != nil {
		ts = req.GetTimestamp().AsTime()
	}
	if err := s.registry.Heartbeat(req.GetNodeId(), req.GetGrpcAddress(), req.GetTotalBytes(), req.GetFreeBytes(), ts); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &castorv1.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *Server) GetActiveNodes(ctx context.Context, req *castorv1.GetActiveNodesRequest) (*castorv1.GetActiveNodesResponse, error) {
	if !s.raft.IsLeader() {
		return nil, toGRPCError(&consensus.NotLeaderError{LeaderAddr: s.raft.LeaderAddr()})
	}
	nodes := s.registry.GetActiveNodes()
	return &castorv1.GetActiveNodesResponse{Nodes: nodes}, nil
}

func (s *Server) Registry() *registry.Registry {
	return s.registry
}

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, consensus.ErrNotLeader), errors.Is(err, consensus.ErrLeaderUnavailable), errors.Is(err, consensus.ErrRaftNotInitialized):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrBucketNotFound), errors.Is(err, store.ErrMultipartNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrBucketExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrBucketNotEmpty), errors.Is(err, store.ErrMultipartNotPending):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrInvalidKey), errors.Is(err, store.ErrInvalidPart):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
