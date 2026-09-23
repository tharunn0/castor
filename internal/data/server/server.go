package server

import (
	"bytes"
	"context"
	"io"

	pb "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedDataServiceServer
	store *storage.Storage
	cfg   config.NodeConfig
}

func New(s *storage.Storage, cfg config.NodeConfig) *Server {
	return &Server{store: s, cfg: cfg}
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

	if totalBytes > int64(s.store.MaxChunkSize) {
		return status.Errorf(
			codes.ResourceExhausted,
			"payload size exceeds max limit of %d bytes",
			s.store.MaxChunkSize,
		)
	}

	res, err := s.store.WriteChunk(bytes.NewReader(chunkBuf), metaChunkHash)
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
	defer rc.Close()

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
	return &pb.DeleteChunkResponse{
		Deleted: true,
	}, nil
}

func (s *Server) ReplicateChunk(ctx context.Context, req *pb.ReplicateChunkRequest) (*pb.ReplicateChunkResponse, error) {
	return &pb.ReplicateChunkResponse{
		Success:         true,
		BytesReplicated: 4 * 1024 * 1024,
	}, nil
}

func (s *Server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {

	stats, err := s.store.GetDiskStats()
	if err != nil {
		// log error
	}

	return &pb.HealthCheckResponse{
		NodeId:     s.cfg.NodeId,
		Status:     "SERVING",
		TotalBytes: int64(stats.TotalBytes),
		FreeBytes:  int64(stats.FreeBytes),
	}, nil
}
