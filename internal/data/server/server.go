package server

import (
	"context"
	"fmt"
	"io"

	pb "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedDataServiceServer
}

func New() *Server {
	return &Server{}
}

func (s *Server) PutChunk(stream grpc.ClientStreamingServer[pb.PutChunkRequest, pb.PutChunkResponse]) error {
	var totalBytes int64
	chunkHash := "dummy-chunk-hash-001"

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk stream: %w", err)
		}

		if meta := req.GetMetadata(); meta != nil && meta.GetChunkHash() != "" {
			chunkHash = meta.GetChunkHash()
		}
		if chunkBytes := req.GetChunkBytes(); len(chunkBytes) > 0 {
			totalBytes += int64(len(chunkBytes))
		}
	}

	if totalBytes == 0 {
		totalBytes = 4096
	}

	resp := &pb.PutChunkResponse{
		ChunkHash:      chunkHash,
		BytesWritten:   totalBytes,
		AlreadyExisted: false,
	}

	return stream.SendAndClose(resp)
}

func (s *Server) GetChunk(req *pb.GetChunkRequest, stream grpc.ServerStreamingServer[pb.GetChunkResponse]) error {
	chunkHash := req.GetChunkHash()
	if chunkHash == "" {
		chunkHash = "dummy-chunk-hash-001"
	}

	for i := range 5 {
		payload := fmt.Sprintf("dummy chunk data for %s [part %d]\n", chunkHash, i)
		resp := &pb.GetChunkResponse{
			ChunkBytes: []byte(payload),
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("failed to send chunk stream: %w", err)
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
	return &pb.HealthCheckResponse{
		NodeId:     "storage-node-1",
		Status:     "SERVING",
		TotalBytes: 500 * 1024 * 1024 * 1024,
		FreeBytes:  350 * 1024 * 1024 * 1024,
	}, nil
}
