package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	pb "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/storage"
	"github.com/tharunn0/castor/internal/data/telemetry"
	telpkg "github.com/tharunn0/castor/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
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

func TestDeleteChunk_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	payload := make([]byte, 128*1024)
	_, _ = rand.Read(payload)
	expectedHash := hashBytes(payload)

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

	deleteResp, err := client.DeleteChunk(context.Background(), &pb.DeleteChunkRequest{
		ChunkHash: expectedHash,
	})
	if err != nil {
		t.Fatalf("DeleteChunk failed: %v", err)
	}
	if !deleteResp.GetDeleted() {
		t.Fatal("expected deleted = true")
	}

	getStream, err := client.GetChunk(context.Background(), &pb.GetChunkRequest{
		ChunkHash: expectedHash,
	})
	if err != nil {
		t.Fatalf("call GetChunk: %v", err)
	}
	_, err = getStream.Recv()
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound code after deletion, got: %v", status.Code(err))
	}

	deleteAgainResp, err := client.DeleteChunk(context.Background(), &pb.DeleteChunkRequest{
		ChunkHash: expectedHash,
	})
	if err != nil {
		t.Fatalf("second DeleteChunk failed: %v", err)
	}
	if deleteAgainResp.GetDeleted() {
		t.Fatal("expected deleted = false on subsequent delete of same chunk")
	}
}

func TestDeleteChunk_NotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	const nonExistentHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	resp, err := client.DeleteChunk(context.Background(), &pb.DeleteChunkRequest{
		ChunkHash: nonExistentHash,
	})
	if err != nil {
		t.Fatalf("DeleteChunk unexpected error: %v", err)
	}
	if resp.GetDeleted() {
		t.Fatal("expected deleted = false for non-existent chunk")
	}
}

func TestDeleteChunk_InvalidArgument(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	invalidHashes := []string{
		"",
		"short",
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8555",
	}

	for _, hash := range invalidHashes {
		_, err := client.DeleteChunk(context.Background(), &pb.DeleteChunkRequest{
			ChunkHash: hash,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument for hash %q, got: %v", hash, status.Code(err))
		}
	}
}

func TestHealthCheck(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.New(tmpDir, 4<<20)
	if err != nil {
		t.Fatalf("setup storage: %v", err)
	}

	reg := prometheus.NewRegistry()
	metrics := telemetry.NewDataMetrics("test-node-hc", &telpkg.Metrics{Registry: reg})

	srv := New(store, config.NodeConfig{NodeId: "test-node-hc"}, WithMetrics(metrics))
	resp, err := srv.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck unexpected error: %v", err)
	}

	if resp.GetNodeId() != "test-node-hc" {
		t.Errorf("expected NodeId 'test-node-hc', got %q", resp.GetNodeId())
	}
	if resp.GetStatus() != "SERVING" {
		t.Errorf("expected Status 'SERVING', got %q", resp.GetStatus())
	}
	if resp.GetTotalBytes() == 0 {
		t.Error("expected non-zero TotalBytes")
	}

	totalMetric := testutil.ToFloat64(metrics.DiskCapacity.WithLabelValues("total"))
	if totalMetric != float64(resp.GetTotalBytes()) {
		t.Errorf("metric TotalBytes %f != resp TotalBytes %d", totalMetric, resp.GetTotalBytes())
	}
}

func TestServer_TelemetryAndTracing(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	otel.SetTracerProvider(tp)

	reg := prometheus.NewRegistry()
	metrics := telemetry.NewDataMetrics("test-node-trace", &telpkg.Metrics{Registry: reg})

	tmpDir := t.TempDir()
	store, err := storage.New(tmpDir, 4<<20, storage.WithMetrics(metrics))
	if err != nil {
		t.Fatalf("setup storage: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
				return info == nil || info.FullMethodName != "/castor.v1.DataService/HealthCheck"
			}),
		)),
	)
	dataSrv := New(store, config.NodeConfig{NodeId: "test-node-trace"}, WithMetrics(metrics))
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
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	defer func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}()

	client := pb.NewDataServiceClient(conn)

	// 1. HealthCheck should be filtered out from tracing
	_, err = client.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}

	spans := exporter.GetSpans()
	for _, span := range spans {
		if span.SpanKind == trace.SpanKindServer && span.Name == "castor.v1.DataService/HealthCheck" {
			t.Errorf("expected HealthCheck server span to be filtered out of traces, but found: %s", span.Name)
		}
	}

	// 2. PutChunk should generate Data/PutChunk span with attributes
	payload := make([]byte, 64*1024)
	_, _ = rand.Read(payload)
	expectedHash := hashBytes(payload)

	stream, err := client.PutChunk(context.Background())
	if err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}
	_ = stream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_Metadata{
			Metadata: &pb.PutChunkMetadata{
				ChunkHash: expectedHash,
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
	if err != nil {
		t.Fatalf("stream.CloseAndRecv failed: %v", err)
	}

	// 3. ReplicateChunk should generate Data/ReplicateChunk span
	repResp, err := client.ReplicateChunk(context.Background(), &pb.ReplicateChunkRequest{
		ChunkHash:         expectedHash,
		SourceNodeAddress: "127.0.0.1:9102",
	})
	if err != nil || !repResp.GetSuccess() {
		t.Fatalf("ReplicateChunk failed: %v", err)
	}

	spans = exporter.GetSpans()
	foundPutChunk := false
	foundReplicateChunk := false

	for _, s := range spans {
		if s.Name == "Data/PutChunk" {
			foundPutChunk = true
			attrs := make(map[string]any)
			for _, a := range s.Attributes {
				attrs[string(a.Key)] = a.Value.AsInterface()
			}
			if attrs["data.node_id"] != "test-node-trace" {
				t.Errorf("expected data.node_id 'test-node-trace', got %v", attrs["data.node_id"])
			}
			if attrs["chunk.hash"] != expectedHash {
				t.Errorf("expected chunk.hash %q, got %v", expectedHash, attrs["chunk.hash"])
			}
			if attrs["chunk.size_bytes"] != int64(len(payload)) {
				t.Errorf("expected chunk.size_bytes %d, got %v", len(payload), attrs["chunk.size_bytes"])
			}
		}
		if s.Name == "Data/ReplicateChunk" {
			foundReplicateChunk = true
			attrs := make(map[string]any)
			for _, a := range s.Attributes {
				attrs[string(a.Key)] = a.Value.AsInterface()
			}
			if attrs["data.node_id"] != "test-node-trace" {
				t.Errorf("expected data.node_id 'test-node-trace', got %v", attrs["data.node_id"])
			}
			if attrs["chunk.hash"] != expectedHash {
				t.Errorf("expected chunk.hash %q, got %v", expectedHash, attrs["chunk.hash"])
			}
			if attrs["chunk.source_node"] != "127.0.0.1:9102" {
				t.Errorf("expected chunk.source_node '127.0.0.1:9102', got %v", attrs["chunk.source_node"])
			}
		}
		if s.SpanKind == trace.SpanKindServer && s.Name == "castor.v1.DataService/HealthCheck" {
			t.Errorf("found filtered HealthCheck server span: %s", s.Name)
		}
	}

	if !foundPutChunk {
		t.Error("expected Data/PutChunk span not found")
	}
	if !foundReplicateChunk {
		t.Error("expected Data/ReplicateChunk span not found")
	}

	writesSuccess := testutil.ToFloat64(metrics.ChunkWritesTotal.WithLabelValues("success"))
	if writesSuccess != 1 {
		t.Errorf("expected writesSuccess 1, got %f", writesSuccess)
	}
}

func TestGrpcChunkReader(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	payload := make([]byte, 128*1024)
	_, _ = rand.Read(payload)
	expectedHash := hashBytes(payload)

	stream, err := client.PutChunk(context.Background())
	if err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}
	if err := stream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_Metadata{
			Metadata: &pb.PutChunkMetadata{
				ChunkHash: expectedHash,
				ChunkSize: int64(len(payload)),
			},
		},
	}); err != nil {
		t.Fatalf("send metadata failed: %v", err)
	}
	if err := stream.Send(&pb.PutChunkRequest{
		Data: &pb.PutChunkRequest_ChunkBytes{
			ChunkBytes: payload,
		},
	}); err != nil {
		t.Fatalf("send chunk bytes failed: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv failed: %v", err)
	}

	getStream, err := client.GetChunk(context.Background(), &pb.GetChunkRequest{
		ChunkHash: expectedHash,
	})
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	reader := &grpcChunkReader{stream: getStream}

	zeroBuf := make([]byte, 0)
	n, err := reader.Read(zeroBuf)
	if n != 0 || err != nil {
		t.Fatalf("expected Read(empty) = (0, nil), got (%d, %v)", n, err)
	}

	var readBytes []byte
	buf := make([]byte, 1024)
	for {
		nr, rerr := reader.Read(buf)
		if nr > 0 {
			readBytes = append(readBytes, buf[:nr]...)
		}
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			t.Fatalf("reader.Read failed: %v", rerr)
		}
	}

	if len(readBytes) != len(payload) {
		t.Fatalf("read length mismatch: got %d, want %d", len(readBytes), len(payload))
	}
	if hashBytes(readBytes) != expectedHash {
		t.Fatalf("read hash mismatch: got %q, want %q", hashBytes(readBytes), expectedHash)
	}
}

func TestWithPeerDialer(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.New(tmpDir, 4<<20)
	if err != nil {
		t.Fatalf("setup storage: %v", err)
	}

	called := false
	customDialer := func(ctx context.Context, target string) (pb.DataServiceClient, io.Closer, error) {
		called = true
		return nil, nil, nil
	}

	srv := New(store, config.NodeConfig{NodeId: "test-node"}, WithPeerDialer(customDialer))
	if srv.peerDialer == nil {
		t.Fatal("expected peerDialer to be set")
	}

	_, _, _ = srv.peerDialer(context.Background(), "mock:1234")
	if !called {
		t.Fatal("expected custom peer dialer to be invoked")
	}
}
