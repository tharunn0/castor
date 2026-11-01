package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type mockMetaServer struct {
	castorv1.UnimplementedMetadataServiceServer
	getManifestFunc func(ctx context.Context, req *castorv1.GetManifestRequest) (*castorv1.GetManifestResponse, error)
}

func (m *mockMetaServer) GetManifest(ctx context.Context, req *castorv1.GetManifestRequest) (*castorv1.GetManifestResponse, error) {
	if m.getManifestFunc != nil {
		return m.getManifestFunc(ctx, req)
	}
	return nil, status.Error(codes.NotFound, "not found")
}

type mockDataServer struct {
	castorv1.UnimplementedDataServiceServer
	getChunkFunc func(req *castorv1.GetChunkRequest, stream grpc.ServerStreamingServer[castorv1.GetChunkResponse]) error
}

func (m *mockDataServer) GetChunk(req *castorv1.GetChunkRequest, stream grpc.ServerStreamingServer[castorv1.GetChunkResponse]) error {
	if m.getChunkFunc != nil {
		return m.getChunkFunc(req, stream)
	}
	return status.Error(codes.NotFound, "chunk not found")
}

func setupMockMeta(t *testing.T, srv *mockMetaServer) (castorv1.MetadataServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(s, srv)

	go func() {
		_ = s.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial mock meta: %v", err)
	}

	return castorv1.NewMetadataServiceClient(conn), func() {
		_ = conn.Close()
		s.Stop()
	}
}

func setupMockData(t *testing.T, srv *mockDataServer) (castorv1.DataServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	castorv1.RegisterDataServiceServer(s, srv)

	go func() {
		_ = s.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial mock data: %v", err)
	}

	return castorv1.NewDataServiceClient(conn), func() {
		_ = conn.Close()
		s.Stop()
	}
}

func TestAuditManifest_Healthy(t *testing.T) {
	chunkPayload := []byte("hello-castor-chunk-payload")
	sum := sha256.Sum256(chunkPayload)
	chunkHash := hex.EncodeToString(sum[:])

	metaSrv := &mockMetaServer{
		getManifestFunc: func(ctx context.Context, req *castorv1.GetManifestRequest) (*castorv1.GetManifestResponse, error) {
			return &castorv1.GetManifestResponse{
				Bucket:      req.GetBucket(),
				Key:         req.GetKey(),
				Size:        int64(len(chunkPayload)),
				Etag:        chunkHash,
				ContentType: "text/plain",
				Status:      "committed",
				Chunks: []*castorv1.ChunkWithLocations{
					{
						ChunkHash:     chunkHash,
						NodeAddresses: []string{"node1:9101", "node2:9102"},
						Size:          int64(len(chunkPayload)),
					},
				},
			}, nil
		},
	}
	metaClient, metaCleanup := setupMockMeta(t, metaSrv)
	defer metaCleanup()

	dataSrv := &mockDataServer{
		getChunkFunc: func(req *castorv1.GetChunkRequest, stream grpc.ServerStreamingServer[castorv1.GetChunkResponse]) error {
			if req.GetChunkHash() == chunkHash {
				return stream.Send(&castorv1.GetChunkResponse{ChunkBytes: chunkPayload})
			}
			return status.Error(codes.NotFound, "not found")
		},
	}
	dataClient, dataCleanup := setupMockData(t, dataSrv)
	defer dataCleanup()

	cfg := AuditConfig{
		Bucket:     "test-bucket",
		Key:        "file.txt",
		Verify:     true,
		VerifyHash: true,
		Timeout:    2 * time.Second,
	}

	dialer := func(_ context.Context, addr string) (castorv1.DataServiceClient, func(), error) {
		return dataClient, func() {}, nil
	}

	report, err := AuditManifest(context.Background(), cfg, metaClient, dialer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !report.AllHealthy {
		t.Errorf("expected report to be healthy, got %v", report.AuditSummary)
	}
	if len(report.Chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(report.Chunks))
	}
	if report.Chunks[0].ActiveNodes != 2 {
		t.Errorf("expected 2 active nodes, got %d", report.Chunks[0].ActiveNodes)
	}

	var buf bytes.Buffer
	RenderReport(&buf, report, cfg)
	output := buf.String()
	if !strings.Contains(output, "RESULT: HEALTHY") {
		t.Errorf("expected render output to contain RESULT: HEALTHY, got:\n%s", output)
	}
}

func TestAuditManifest_DegradedQuorum(t *testing.T) {
	chunkHash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	metaSrv := &mockMetaServer{
		getManifestFunc: func(ctx context.Context, req *castorv1.GetManifestRequest) (*castorv1.GetManifestResponse, error) {
			return &castorv1.GetManifestResponse{
				Bucket: req.GetBucket(),
				Key:    req.GetKey(),
				Size:   100,
				Chunks: []*castorv1.ChunkWithLocations{
					{
						ChunkHash:     chunkHash,
						NodeAddresses: []string{"node1:9101", "node2:9102"},
						Size:          100,
					},
				},
			}, nil
		},
	}
	metaClient, metaCleanup := setupMockMeta(t, metaSrv)
	defer metaCleanup()

	// data server where chunk is missing on node2
	dialer := func(_ context.Context, addr string) (castorv1.DataServiceClient, func(), error) {
		if addr == "node2:9102" {
			dataSrvMissing := &mockDataServer{
				getChunkFunc: func(req *castorv1.GetChunkRequest, stream grpc.ServerStreamingServer[castorv1.GetChunkResponse]) error {
					return status.Error(codes.NotFound, "missing chunk")
				},
			}
			client, cleanup := setupMockData(t, dataSrvMissing)
			return client, cleanup, nil
		}
		dataSrvOK := &mockDataServer{
			getChunkFunc: func(req *castorv1.GetChunkRequest, stream grpc.ServerStreamingServer[castorv1.GetChunkResponse]) error {
				return stream.Send(&castorv1.GetChunkResponse{ChunkBytes: []byte("chunk-data")})
			},
		}
		client, cleanup := setupMockData(t, dataSrvOK)
		return client, cleanup, nil
	}

	cfg := AuditConfig{
		Bucket:  "test-bucket",
		Key:     "file.txt",
		Verify:  true,
		Timeout: 2 * time.Second,
	}

	report, err := AuditManifest(context.Background(), cfg, metaClient, dialer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if report.AllHealthy {
		t.Errorf("expected report to be degraded (quorum unsatisfied), got healthy")
	}
	if report.Chunks[0].ActiveNodes != 1 {
		t.Errorf("expected 1 active node, got %d", report.Chunks[0].ActiveNodes)
	}
}

func TestAuditManifest_NotFound(t *testing.T) {
	metaSrv := &mockMetaServer{
		getManifestFunc: func(ctx context.Context, req *castorv1.GetManifestRequest) (*castorv1.GetManifestResponse, error) {
			return nil, status.Error(codes.NotFound, "manifest not found")
		},
	}
	metaClient, metaCleanup := setupMockMeta(t, metaSrv)
	defer metaCleanup()

	dialer := func(_ context.Context, addr string) (castorv1.DataServiceClient, func(), error) {
		return nil, nil, fmt.Errorf("unexpected dial")
	}

	cfg := AuditConfig{
		Bucket:  "missing-bucket",
		Key:     "missing-key",
		Timeout: 2 * time.Second,
	}

	_, err := AuditManifest(context.Background(), cfg, metaClient, dialer)
	if err == nil {
		t.Fatalf("expected NotFound error, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got: %v", status.Code(err))
	}
}

func TestRemapNodeAddress(t *testing.T) {
	tests := []struct {
		in          string
		dockerRemap bool
		expected    string
	}{
		{"data-1:9101", true, "127.0.0.1:9101"},
		{"data-2:9101", true, "127.0.0.1:9102"},
		{"data-3:9101", true, "127.0.0.1:9103"},
		{"data-4:9101", true, "127.0.0.1:9104"},
		{"data-1:9101", false, "data-1:9101"},
		{"custom.host:9101", true, "custom.host:9101"},
	}

	for _, tt := range tests {
		actual := RemapNodeAddress(tt.in, tt.dockerRemap)
		if actual != tt.expected {
			t.Errorf("RemapNodeAddress(%q, %v) = %q, expected %q", tt.in, tt.dockerRemap, actual, tt.expected)
		}
	}
}

func TestRenderReport_LimitAndOnlyDegraded(t *testing.T) {
	report := &ManifestAuditReport{
		Bucket:      "b",
		Key:         "k",
		Size:        500,
		TotalChunks: 5,
		AllHealthy:  false,
		Chunks: []ChunkAudit{
			{Index: 0, ChunkHash: "hash0", Size: 100, Healthy: true},
			{Index: 1, ChunkHash: "hash1", Size: 100, Healthy: false},
			{Index: 2, ChunkHash: "hash2", Size: 100, Healthy: true},
			{Index: 3, ChunkHash: "hash3", Size: 100, Healthy: false},
			{Index: 4, ChunkHash: "hash4", Size: 100, Healthy: true},
		},
	}

	// Test 1: Limit = 2
	var bufLimit bytes.Buffer
	cfgLimit := AuditConfig{Limit: 2}
	RenderReport(&bufLimit, report, cfgLimit)
	outLimit := bufLimit.String()
	if !strings.Contains(outLimit, "[Chunk #0]") || !strings.Contains(outLimit, "[Chunk #1]") {
		t.Errorf("expected chunk 0 and 1 to be displayed, got:\n%s", outLimit)
	}
	if strings.Contains(outLimit, "[Chunk #2]") {
		t.Errorf("expected chunk 2 to be omitted due to limit=2, got:\n%s", outLimit)
	}
	if !strings.Contains(outLimit, "3 more chunks omitted") {
		t.Errorf("expected omission message, got:\n%s", outLimit)
	}

	// Test 2: OnlyDegraded
	var bufDegraded bytes.Buffer
	cfgDegraded := AuditConfig{OnlyDegraded: true}
	RenderReport(&bufDegraded, report, cfgDegraded)
	outDegraded := bufDegraded.String()
	if strings.Contains(outDegraded, "[Chunk #0]") || strings.Contains(outDegraded, "[Chunk #2]") {
		t.Errorf("expected healthy chunks to be omitted, got:\n%s", outDegraded)
	}
	if !strings.Contains(outDegraded, "[Chunk #1]") || !strings.Contains(outDegraded, "[Chunk #3]") {
		t.Errorf("expected degraded chunks #1 and #3, got:\n%s", outDegraded)
	}
}

func TestAuditManifest_WorkerPool(t *testing.T) {
	const numChunks = 25
	chunks := make([]*castorv1.ChunkWithLocations, numChunks)
	for i := range numChunks {
		chunks[i] = &castorv1.ChunkWithLocations{
			ChunkHash:     fmt.Sprintf("hash-%02d", i),
			NodeAddresses: []string{"node1:9101", "node2:9102"},
			Size:          4096,
		}
	}

	metaSrv := &mockMetaServer{
		getManifestFunc: func(ctx context.Context, req *castorv1.GetManifestRequest) (*castorv1.GetManifestResponse, error) {
			return &castorv1.GetManifestResponse{
				Bucket: req.GetBucket(),
				Key:    req.GetKey(),
				Size:   numChunks * 4096,
				Chunks: chunks,
			}, nil
		},
	}
	metaClient, metaCleanup := setupMockMeta(t, metaSrv)
	defer metaCleanup()

	dataSrv := &mockDataServer{
		getChunkFunc: func(req *castorv1.GetChunkRequest, stream grpc.ServerStreamingServer[castorv1.GetChunkResponse]) error {
			return stream.Send(&castorv1.GetChunkResponse{ChunkBytes: []byte("chunk")})
		},
	}
	dataClient, dataCleanup := setupMockData(t, dataSrv)
	defer dataCleanup()

	cfg := AuditConfig{
		Bucket:  "test-bucket",
		Key:     "big-file.bin",
		Verify:  true,
		Workers: 5,
		Timeout: 2 * time.Second,
	}

	dialer := func(_ context.Context, addr string) (castorv1.DataServiceClient, func(), error) {
		return dataClient, func() {}, nil
	}

	report, err := AuditManifest(context.Background(), cfg, metaClient, dialer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(report.Chunks) != numChunks {
		t.Fatalf("expected %d chunks, got %d", numChunks, len(report.Chunks))
	}
	for i, c := range report.Chunks {
		if c.Index != i {
			t.Errorf("expected chunk index %d, got %d", i, c.Index)
		}
		if !c.Healthy {
			t.Errorf("expected chunk %d to be healthy", i)
		}
	}
}
