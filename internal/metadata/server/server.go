package server

import (
	"context"
	"errors"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"github.com/tharunn0/castor/internal/metadata/registry"
	"github.com/tharunn0/castor/internal/metadata/store"
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

func New(s *store.Store, r RaftApplier, opts ...Option) *Server {
	srv := &Server{
		store:    s,
		raft:     r,
		registry: registry.New(),
		timeout:  5 * time.Second,
	}
	for _, opt := range opts {
		opt(srv)
	}
	return srv
}

func (s *Server) apply(cmd *consensus.Command) (any, error) {
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
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}
	cmd, err := consensus.NewCreateBucketCommand(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode create bucket command: %v", err)
	}
	res, err := s.apply(cmd)
	if err != nil {
		return nil, toGRPCError(err)
	}
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
	if _, err := s.apply(cmd); err != nil {
		return nil, toGRPCError(err)
	}
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

func (s *Server) CommitManifest(ctx context.Context, req *castorv1.CommitManifestRequest) (*castorv1.CommitManifestResponse, error) {
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "object key is required")
	}
	cmd, err := consensus.NewCommitManifestCommand(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode commit manifest command: %v", err)
	}
	res, err := s.apply(cmd)
	if err != nil {
		return nil, toGRPCError(err)
	}
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
	if _, err := s.apply(cmd); err != nil {
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

func (s *Server) RegisterNode(ctx context.Context, req *castorv1.RegisterNodeRequest) (*castorv1.RegisterNodeResponse, error) {
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
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrBucketNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrBucketExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrBucketNotEmpty):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrInvalidKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
