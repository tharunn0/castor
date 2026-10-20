package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"

	pb "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/storage"
	"github.com/tharunn0/castor/internal/data/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedDataServiceServer
	store   *storage.Storage
	cfg     config.NodeConfig
	logger  *slog.Logger
	metrics *telemetry.DataMetrics
}

type Option func(*Server)

func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) {
		s.logger = logger
	}
}

func WithMetrics(metrics *telemetry.DataMetrics) Option {
	return func(s *Server) {
		s.metrics = metrics
	}
}

func New(s *storage.Storage, cfg config.NodeConfig, opts ...Option) *Server {
	srv := &Server{
		store:  s,
		cfg:    cfg,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(srv)
	}
	if srv.logger == nil {
		srv.logger = slog.Default()
	}
	return srv
}

func (s *Server) PutChunk(stream grpc.ClientStreamingServer[pb.PutChunkRequest, pb.PutChunkResponse]) error {
	var totalBytes int64
	var metaChunkHash string

	chunkBuf := make([]byte, 0, s.store.MaxChunkSize)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive chunk stream: %v", err)
		}

		if meta := req.GetMetadata(); meta != nil && meta.GetChunkHash() != "" {
			metaChunkHash = meta.GetChunkHash()
		}
		if chunkBytes := req.GetChunkBytes(); len(chunkBytes) > 0 {
			chunkBuf = append(chunkBuf, chunkBytes...)
			totalBytes += int64(len(chunkBytes))
		}
	}

	span := trace.SpanFromContext(stream.Context())
	if span.IsRecording() {
		span.SetName("Data/PutChunk")
		span.SetAttributes(
			attribute.String("data.node_id", s.cfg.NodeId),
			attribute.String("chunk.hash", metaChunkHash),
			attribute.Int64("chunk.size_bytes", totalBytes),
			attribute.Bool("chunk.already_existed", false),
		)
	}

	if totalBytes > int64(s.store.MaxChunkSize) {
		return status.Errorf(
			codes.ResourceExhausted,
			"payload size exceeds max limit of %d bytes",
			s.store.MaxChunkSize,
		)
	}

	res, err := s.store.WriteChunk(stream.Context(), bytes.NewReader(chunkBuf), metaChunkHash)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "write chunk: %v", err)
	}

	resp := &pb.PutChunkResponse{
		ChunkHash:      res.Hash,
		BytesWritten:   int64(res.Size),
		AlreadyExisted: false,
	}

	return stream.SendAndClose(resp)
}

func (s *Server) GetChunk(req *pb.GetChunkRequest, stream grpc.ServerStreamingServer[pb.GetChunkResponse]) error {
	chunkHash := req.GetChunkHash()
	if chunkHash == "" {
		return status.Errorf(
			codes.InvalidArgument,
			"chunk hash is required",
		)
	}

	rc, err := s.store.ReadChunk(req.GetChunkHash())
	if err != nil {
		return status.Errorf(codes.NotFound, "chunk not found: %v", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			s.logger.Warn("failed to close chunk reader", "error", err)
		}
	}()

	buf := make([]byte, 64*1024)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.GetChunkResponse{ChunkBytes: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	chunkHash := req.GetChunkHash()
	if chunkHash == "" {
		return nil, status.Errorf(codes.InvalidArgument, "chunk hash is required")
	}

	deleted, err := s.store.DeleteChunk(chunkHash)
	if err != nil {
		if errors.Is(err, storage.ErrInvalidHash) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid chunk hash: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "delete chunk: %v", err)
	}

	return &pb.DeleteChunkResponse{
		Deleted: deleted,
	}, nil
}

func (s *Server) ReplicateChunk(ctx context.Context, req *pb.ReplicateChunkRequest) (*pb.ReplicateChunkResponse, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetName("Data/ReplicateChunk")
		span.SetAttributes(
			attribute.String("data.node_id", s.cfg.NodeId),
			attribute.String("chunk.hash", req.GetChunkHash()),
			attribute.String("chunk.source_node", req.GetSourceNodeAddress()),
		)
	}

	return &pb.ReplicateChunkResponse{
		Success:         true,
		BytesReplicated: 4 * 1024 * 1024,
	}, nil
}

func (s *Server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	stats, err := s.store.GetDiskStats()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get disk stats: %v", err)
	}

	if s.metrics != nil && s.metrics.DiskCapacity != nil {
		s.metrics.DiskCapacity.WithLabelValues("total").Set(float64(stats.TotalBytes))
		s.metrics.DiskCapacity.WithLabelValues("used").Set(float64(stats.UsedBytes))
		s.metrics.DiskCapacity.WithLabelValues("free").Set(float64(stats.FreeBytes))
	}

	return &pb.HealthCheckResponse{
		NodeId:     s.cfg.NodeId,
		Status:     "SERVING",
		TotalBytes: int64(stats.TotalBytes),
		FreeBytes:  int64(stats.FreeBytes),
	}, nil
}
