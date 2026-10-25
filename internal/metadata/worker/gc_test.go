package worker

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockOrphanScanner struct {
	scanFunc func(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error)
}

func (m *mockOrphanScanner) ScanOrphanedChunks(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
	if m.scanFunc != nil {
		return m.scanFunc(ctx, quarantineTTL, limit)
	}
	return nil, nil
}

type mockCommandApplier struct {
	mu        sync.Mutex
	applied   []*consensus.Command
	applyFunc func(cmd *consensus.Command, timeout time.Duration) (any, error)
}

func (m *mockCommandApplier) Apply(cmd *consensus.Command, timeout time.Duration) (any, error) {
	m.mu.Lock()
	m.applied = append(m.applied, cmd)
	m.mu.Unlock()
	if m.applyFunc != nil {
		return m.applyFunc(cmd, timeout)
	}
	return nil, nil
}

type mockDataClient struct {
	deleteFunc    func(ctx context.Context, in *castorv1.DeleteChunkRequest) (*castorv1.DeleteChunkResponse, error)
	replicateFunc func(ctx context.Context, in *castorv1.ReplicateChunkRequest) (*castorv1.ReplicateChunkResponse, error)
}

func (m *mockDataClient) PutChunk(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[castorv1.PutChunkRequest, castorv1.PutChunkResponse], error) {
	return nil, nil
}
func (m *mockDataClient) GetChunk(ctx context.Context, in *castorv1.GetChunkRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[castorv1.GetChunkResponse], error) {
	return nil, nil
}
func (m *mockDataClient) DeleteChunk(ctx context.Context, in *castorv1.DeleteChunkRequest, opts ...grpc.CallOption) (*castorv1.DeleteChunkResponse, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, in)
	}
	return &castorv1.DeleteChunkResponse{Deleted: true}, nil
}
func (m *mockDataClient) ReplicateChunk(ctx context.Context, in *castorv1.ReplicateChunkRequest, opts ...grpc.CallOption) (*castorv1.ReplicateChunkResponse, error) {
	if m.replicateFunc != nil {
		return m.replicateFunc(ctx, in)
	}
	return &castorv1.ReplicateChunkResponse{Success: true}, nil
}
func (m *mockDataClient) HealthCheck(ctx context.Context, in *castorv1.HealthCheckRequest, opts ...grpc.CallOption) (*castorv1.HealthCheckResponse, error) {
	return nil, nil
}

func TestQuarantineGCWorker_Name(t *testing.T) {
	w := NewQuarantineGCWorker(&mockOrphanScanner{}, &mockCommandApplier{})
	if w.Name() != "quarantine-gc" {
		t.Fatalf("expected 'quarantine-gc', got %q", w.Name())
	}
}

func TestQuarantineGCWorker_RunOnce_Success(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := NewGCMetrics(reg)

	orphans := []*castorv1.ChunkLocationRecord{
		{
			ChunkHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Nodes:     []string{"node1:9000", "node2:9000"},
		},
		{
			ChunkHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Nodes:     []string{"node3:9000"},
		},
	}

	scanner := &mockOrphanScanner{
		scanFunc: func(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			if quarantineTTL != 24*time.Hour {
				t.Errorf("expected 24h quarantine TTL, got %v", quarantineTTL)
			}
			if limit != 50 {
				t.Errorf("expected batch limit 50, got %d", limit)
			}
			return orphans, nil
		},
	}

	applier := &mockCommandApplier{}

	var deletedMu sync.Mutex
	deletedChunks := make(map[string][]string)

	mockClient := &mockDataClient{
		deleteFunc: func(ctx context.Context, in *castorv1.DeleteChunkRequest) (*castorv1.DeleteChunkResponse, error) {
			deletedMu.Lock()
			deletedChunks[in.ChunkHash] = append(deletedChunks[in.ChunkHash], "deleted")
			deletedMu.Unlock()
			return &castorv1.DeleteChunkResponse{Deleted: true}, nil
		},
	}

	dialer := func(ctx context.Context, target string) (castorv1.DataServiceClient, io.Closer, error) {
		return mockClient, nil, nil
	}

	worker := NewQuarantineGCWorker(scanner, applier,
		WithQuarantineTTL(24*time.Hour),
		WithGCBatchLimit(50),
		WithPacingDelay(0),
		WithDataClientDialer(dialer),
		WithGCMetrics(metrics),
	)

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	deletedMu.Lock()
	if len(deletedChunks["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]) != 2 {
		t.Errorf("expected chunk 'a' deleted on 2 nodes, got %d", len(deletedChunks["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]))
	}
	if len(deletedChunks["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]) != 1 {
		t.Errorf("expected chunk 'b' deleted on 1 node, got %d", len(deletedChunks["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]))
	}
	deletedMu.Unlock()

	applier.mu.Lock()
	if len(applier.applied) != 2 {
		t.Fatalf("expected 2 applied commands, got %d", len(applier.applied))
	}
	for _, cmd := range applier.applied {
		if cmd.Type != consensus.CmdRemoveChunkLocations {
			t.Errorf("expected CmdRemoveChunkLocations, got %v", cmd.Type)
		}
	}
	applier.mu.Unlock()

	if val := testutil.ToFloat64(metrics.OrphansScanned); val != 2 {
		t.Errorf("expected 2 orphans scanned, got %f", val)
	}
	if val := testutil.ToFloat64(metrics.ChunksPurged); val != 2 {
		t.Errorf("expected 2 chunks purged, got %f", val)
	}
}

func TestQuarantineGCWorker_RunOnce_NotFoundTreatedAsDeleted(t *testing.T) {
	ctx := context.Background()
	orphans := []*castorv1.ChunkLocationRecord{
		{
			ChunkHash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			Nodes:     []string{"node1:9000"},
		},
	}

	scanner := &mockOrphanScanner{
		scanFunc: func(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return orphans, nil
		},
	}
	applier := &mockCommandApplier{}

	mockClient := &mockDataClient{
		deleteFunc: func(ctx context.Context, in *castorv1.DeleteChunkRequest) (*castorv1.DeleteChunkResponse, error) {
			return nil, status.Error(codes.NotFound, "chunk already removed")
		},
	}

	dialer := func(ctx context.Context, target string) (castorv1.DataServiceClient, io.Closer, error) {
		return mockClient, nil, nil
	}

	worker := NewQuarantineGCWorker(scanner, applier,
		WithPacingDelay(0),
		WithDataClientDialer(dialer),
	)

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	applier.mu.Lock()
	if len(applier.applied) != 1 {
		t.Fatalf("expected 1 applied command, got %d", len(applier.applied))
	}
	applier.mu.Unlock()
}

func TestQuarantineGCWorker_RunOnce_ScanError(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := NewGCMetrics(reg)

	expectedErr := errors.New("scan failure")
	scanner := &mockOrphanScanner{
		scanFunc: func(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return nil, expectedErr
		},
	}
	applier := &mockCommandApplier{}

	worker := NewQuarantineGCWorker(scanner, applier,
		WithGCMetrics(metrics),
	)

	err := worker.RunOnce(ctx)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected scan error, got %v", err)
	}

	applier.mu.Lock()
	if len(applier.applied) != 0 {
		t.Fatalf("expected 0 commands applied, got %d", len(applier.applied))
	}
	applier.mu.Unlock()

	if val := testutil.ToFloat64(metrics.ErrorsTotal.WithLabelValues("scan")); val != 1 {
		t.Errorf("expected 1 scan error metric, got %f", val)
	}
}

func TestQuarantineGCWorker_RunOnce_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	scanner := &mockOrphanScanner{
		scanFunc: func(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return []*castorv1.ChunkLocationRecord{
				{ChunkHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Nodes: []string{"node1:9000"}},
			}, nil
		},
	}
	applier := &mockCommandApplier{}

	worker := NewQuarantineGCWorker(scanner, applier,
		WithPacingDelay(0),
	)

	err := worker.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestQuarantineGCWorker_Run_StopOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	scanner := &mockOrphanScanner{
		scanFunc: func(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return nil, nil
		},
	}
	applier := &mockCommandApplier{}

	worker := NewQuarantineGCWorker(scanner, applier,
		WithGCInterval(10*time.Millisecond),
	)

	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not exit upon context cancellation")
	}
}

func TestQuarantineGCWorker_RaftApplyError(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := NewGCMetrics(reg)

	orphans := []*castorv1.ChunkLocationRecord{
		{
			ChunkHash: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			Nodes:     []string{},
		},
	}

	scanner := &mockOrphanScanner{
		scanFunc: func(ctx context.Context, quarantineTTL time.Duration, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return orphans, nil
		},
	}
	applier := &mockCommandApplier{
		applyFunc: func(cmd *consensus.Command, timeout time.Duration) (any, error) {
			return nil, errors.New("raft quorum lost")
		},
	}

	worker := NewQuarantineGCWorker(scanner, applier,
		WithGCMetrics(metrics),
	)

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("expected RunOnce to continue despite raft error, got %v", err)
	}

	if val := testutil.ToFloat64(metrics.ErrorsTotal.WithLabelValues("apply_raft")); val != 1 {
		t.Errorf("expected 1 apply_raft error, got %f", val)
	}
}
