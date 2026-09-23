package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"testing"

	pb "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func hashBytes(data []byte) string {
	b := sha256.Sum256(data)
	return hex.EncodeToString(b[:])
}

func setupTestServer(t *testing.T) (pb.DataServiceClient, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	store, err := storage.New(tmpDir, 4<<20)
	if err != nil {
		t.Fatalf("setup storage: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	dataSrv := New(store, config.NodeConfig{NodeId: "test-node"})
	pb.RegisterDataServiceServer(srv, dataSrv)

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}

	return pb.NewDataServiceClient(conn), cleanup
}

// 1. check if write succeeds
func TestPutChunk_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	payload := make([]byte, 128*1024)
	_, _ = rand.Read(payload)
	expectedHash := hashBytes(payload)

	stream, err := client.PutChunk(context.Background())
	if err != nil {
		t.Fatalf("open PutChunk stream: %v", err)
	}

	// Frame 1: metadata
	err = stream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_Metadata{
			Metadata: &pb.PutChunkMetadata{
				ChunkHash: expectedHash,
				ChunkSize: int64(len(payload)),
			},
		},
	})
	if err != nil {
		t.Fatalf("send metadata: %v", err)
	}

	// Frame 2: chunk bytes
	err = stream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_ChunkBytes{
			ChunkBytes: payload,
		},
	})
	if err != nil {
		t.Fatalf("send chunk bytes: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("PutChunk expected success, got error: %v", err)
	}

	if resp.GetChunkHash() != expectedHash {
		t.Errorf("chunk hash mismatch: got %s, want %s", resp.GetChunkHash(), expectedHash)
	}
	if resp.GetBytesWritten() != int64(len(payload)) {
		t.Errorf("bytes written mismatch: got %d, want %d", resp.GetBytesWritten(), len(payload))
	}
}

// 2. check if write return error for wrong writes (e.g. hash mismatch)
func TestPutChunk_WrongWrite_Error(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	payload := []byte("hello corrupt world chunk")
	wrongHash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	stream, err := client.PutChunk(context.Background())
	if err != nil {
		t.Fatalf("open PutChunk stream: %v", err)
	}

	// Send wrong expected hash in metadata
	_ = stream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_Metadata{
			Metadata: &pb.PutChunkMetadata{
				ChunkHash: wrongHash,
				ChunkSize: int64(len(payload)),
			},
		},
	})

	_ = stream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_ChunkBytes{
			ChunkBytes: payload,
		},
	})

	_, err = stream.CloseAndRecv()
	if err == nil {
		t.Fatal("expected error on hash mismatch, got nil")
	}

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument code, got: %v", status.Code(err))
	}
}

// 3. check if read success normally after successful writes
func TestGetChunk_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	payload := make([]byte, 64*1024)
	_, _ = rand.Read(payload)
	expectedHash := hashBytes(payload)

	// Write chunk first
	putStream, err := client.PutChunk(context.Background())
	if err != nil {
		t.Fatalf("open PutChunk stream: %v", err)
	}
	_ = putStream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_Metadata{
			Metadata: &pb.PutChunkMetadata{
				ChunkHash: expectedHash,
				ChunkSize: int64(len(payload)),
			},
		},
	})
	_ = putStream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_ChunkBytes{
			ChunkBytes: payload,
		},
	})
	if _, err := putStream.CloseAndRecv(); err != nil {
		t.Fatalf("setup write failed: %v", err)
	}

	// Read chunk back
	getStream, err := client.GetChunk(context.Background(), &pb.GetChunkRequest{
		ChunkHash: expectedHash,
	})
	if err != nil {
		t.Fatalf("call GetChunk: %v", err)
	}

	var received []byte
	for {
		resp, err := getStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream recv error: %v", err)
		}
		received = append(received, resp.GetChunkBytes()...)
	}

	if !bytes.Equal(received, payload) {
		t.Errorf("read payload does not match written payload")
	}
}

// 4. check if read fails on non existing chunks
func TestGetChunk_NotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	nonExistentHash := "0000000000000000000000000000000000000000000000000000000000000000"

	stream, err := client.GetChunk(context.Background(), &pb.GetChunkRequest{
		ChunkHash: nonExistentHash,
	})
	if err != nil {
		t.Fatalf("call GetChunk: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error reading nonexistent chunk, got nil")
	}

	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound code, got: %v", status.Code(err))
	}
}
