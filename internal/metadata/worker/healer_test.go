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
)

type mockUnderReplicatedScanner struct {
	scanFunc func(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error)
	getFunc  func(ctx context.Context, sha256 string) (*castorv1.ChunkLocationRecord, error)
}

func (m *mockUnderReplicatedScanner) ScanUnderReplicatedChunks(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
	if m.scanFunc != nil {
		return m.scanFunc(ctx, targetReplicas, limit)
	}
	return nil, nil
}

func (m *mockUnderReplicatedScanner) GetChunkLocation(ctx context.Context, sha256 string) (*castorv1.ChunkLocationRecord, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, sha256)
	}
	return nil, nil
}

type mockNodeProvider struct {
	nodes []*castorv1.NodeCapacity
}

func (m *mockNodeProvider) GetActiveNodes() []*castorv1.NodeCapacity {
	return m.nodes
}

func TestActiveReplicaHealer_Name(t *testing.T) {
	h := NewActiveReplicaHealer(&mockUnderReplicatedScanner{}, &mockNodeProvider{}, &mockCommandApplier{})
	if h.Name() != "active-replica-healer" {
		t.Fatalf("expected 'active-replica-healer', got %q", h.Name())
	}
}

func TestActiveReplicaHealer_RunOnce_Success(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := NewHealerMetrics(reg)

	chunkHash := "1111111111111111111111111111111111111111111111111111111111111111"
	underReplicated := []*castorv1.ChunkLocationRecord{
		{
			ChunkHash: chunkHash,
			Nodes:     []string{"node1:9000"},
			RefCount:  1,
		},
	}

	scanner := &mockUnderReplicatedScanner{
		scanFunc: func(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			if targetReplicas != 3 {
				t.Errorf("expected targetReplicas 3, got %d", targetReplicas)
			}
			return underReplicated, nil
		},
	}

	nodeProvider := &mockNodeProvider{
		nodes: []*castorv1.NodeCapacity{
			{NodeId: "node1", GrpcAddress: "node1:9000", FreeBytes: 100, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node2", GrpcAddress: "node2:9000", FreeBytes: 300, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node3", GrpcAddress: "node3:9000", FreeBytes: 500, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		},
	}

	applier := &mockCommandApplier{}

	var mu sync.Mutex
	replicatedCalls := make(map[string]string) // targetAddr -> sourceAddr

	dialer := func(ctx context.Context, target string) (castorv1.DataServiceClient, io.Closer, error) {
		return &mockDataClient{
			replicateFunc: func(ctx context.Context, in *castorv1.ReplicateChunkRequest) (*castorv1.ReplicateChunkResponse, error) {
				mu.Lock()
				replicatedCalls[target] = in.SourceNodeAddress
				mu.Unlock()
				return &castorv1.ReplicateChunkResponse{Success: true, BytesReplicated: 4096}, nil
			},
		}, nil, nil
	}

	healer := NewActiveReplicaHealer(scanner, nodeProvider, applier,
		WithTargetReplicas(3),
		WithHealerPacingDelay(0),
		WithHealerDialer(dialer),
		WithHealerMetrics(metrics),
	)

	if err := healer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	mu.Lock()
	if len(replicatedCalls) != 2 {
		t.Fatalf("expected 2 replicate calls, got %d", len(replicatedCalls))
	}
	if replicatedCalls["node3:9000"] != "node1:9000" {
		t.Errorf("expected node3 to replicate from node1, got %q", replicatedCalls["node3:9000"])
	}
	if replicatedCalls["node2:9000"] != "node1:9000" {
		t.Errorf("expected node2 to replicate from node1, got %q", replicatedCalls["node2:9000"])
	}
	mu.Unlock()

	applier.mu.Lock()
	if len(applier.applied) != 2 {
		t.Fatalf("expected 2 applied commands, got %d", len(applier.applied))
	}
	for _, cmd := range applier.applied {
		if cmd.Type != consensus.CmdUpdateChunkLocation {
			t.Errorf("expected CmdUpdateChunkLocation, got %v", cmd.Type)
		}
	}
	applier.mu.Unlock()

	if val := testutil.ToFloat64(metrics.ChunksScanned); val != 1 {
		t.Errorf("expected 1 chunk scanned, got %f", val)
	}
	if val := testutil.ToFloat64(metrics.ChunksRepaired); val != 2 {
		t.Errorf("expected 2 replicas repaired, got %f", val)
	}
}

func TestActiveReplicaHealer_RunOnce_NoActiveSource(t *testing.T) {
	ctx := context.Background()
	scanner := &mockUnderReplicatedScanner{
		scanFunc: func(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return []*castorv1.ChunkLocationRecord{
				{
					ChunkHash: "deadbeef",
					Nodes:     []string{"offline-node:9000"},
					RefCount:  1,
				},
			}, nil
		},
	}

	nodeProvider := &mockNodeProvider{
		nodes: []*castorv1.NodeCapacity{
			{NodeId: "node2", GrpcAddress: "node2:9000", FreeBytes: 300, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		},
	}
	applier := &mockCommandApplier{}

	healer := NewActiveReplicaHealer(scanner, nodeProvider, applier,
		WithHealerPacingDelay(0),
	)

	if err := healer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	applier.mu.Lock()
	if len(applier.applied) != 0 {
		t.Errorf("expected 0 commands applied when source is offline, got %d", len(applier.applied))
	}
	applier.mu.Unlock()
}

func TestActiveReplicaHealer_RunOnce_NoCandidateTargets(t *testing.T) {
	ctx := context.Background()
	scanner := &mockUnderReplicatedScanner{
		scanFunc: func(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return []*castorv1.ChunkLocationRecord{
				{
					ChunkHash: "chunk222",
					Nodes:     []string{"node1:9000", "node2:9000"},
					RefCount:  1,
				},
			}, nil
		},
	}

	nodeProvider := &mockNodeProvider{
		nodes: []*castorv1.NodeCapacity{
			{NodeId: "node1", GrpcAddress: "node1:9000", FreeBytes: 300, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node2", GrpcAddress: "node2:9000", FreeBytes: 200, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		},
	}
	applier := &mockCommandApplier{}

	healer := NewActiveReplicaHealer(scanner, nodeProvider, applier,
		WithTargetReplicas(3),
		WithHealerPacingDelay(0),
	)

	if err := healer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	applier.mu.Lock()
	if len(applier.applied) != 0 {
		t.Errorf("expected 0 commands applied when no candidates available, got %d", len(applier.applied))
	}
	applier.mu.Unlock()
}

func TestActiveReplicaHealer_HealChunk_OnDemand(t *testing.T) {
	ctx := context.Background()
	chunkHash := "hash-ondemand"
	scanner := &mockUnderReplicatedScanner{
		getFunc: func(ctx context.Context, sha256 string) (*castorv1.ChunkLocationRecord, error) {
			if sha256 != chunkHash {
				return nil, errors.New("not found")
			}
			return &castorv1.ChunkLocationRecord{
				ChunkHash: chunkHash,
				Nodes:     []string{"node1:9000"},
				RefCount:  2,
			}, nil
		},
	}

	nodeProvider := &mockNodeProvider{
		nodes: []*castorv1.NodeCapacity{
			{NodeId: "node1", GrpcAddress: "node1:9000", FreeBytes: 100, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node2", GrpcAddress: "node2:9000", FreeBytes: 400, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		},
	}

	applier := &mockCommandApplier{}

	dialer := func(ctx context.Context, target string) (castorv1.DataServiceClient, io.Closer, error) {
		return &mockDataClient{
			replicateFunc: func(ctx context.Context, in *castorv1.ReplicateChunkRequest) (*castorv1.ReplicateChunkResponse, error) {
				return &castorv1.ReplicateChunkResponse{Success: true}, nil
			},
		}, nil, nil
	}

	healer := NewActiveReplicaHealer(scanner, nodeProvider, applier,
		WithTargetReplicas(2),
		WithHealerPacingDelay(0),
		WithHealerDialer(dialer),
	)

	if err := healer.HealChunk(ctx, chunkHash); err != nil {
		t.Fatalf("HealChunk failed: %v", err)
	}

	applier.mu.Lock()
	if len(applier.applied) != 1 {
		t.Fatalf("expected 1 command applied, got %d", len(applier.applied))
	}
	applier.mu.Unlock()
}

func TestActiveReplicaHealer_TriggerRepair_Channel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chunkHash := "hash-triggered"
	repairedCh := make(chan struct{}, 1)

	scanner := &mockUnderReplicatedScanner{
		scanFunc: func(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return nil, nil
		},
		getFunc: func(ctx context.Context, sha256 string) (*castorv1.ChunkLocationRecord, error) {
			return &castorv1.ChunkLocationRecord{
				ChunkHash: chunkHash,
				Nodes:     []string{"node1:9000"},
				RefCount:  1,
			}, nil
		},
	}

	nodeProvider := &mockNodeProvider{
		nodes: []*castorv1.NodeCapacity{
			{NodeId: "node1", GrpcAddress: "node1:9000", FreeBytes: 100, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node2", GrpcAddress: "node2:9000", FreeBytes: 300, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		},
	}

	applier := &mockCommandApplier{
		applyFunc: func(cmd *consensus.Command, timeout time.Duration) (any, error) {
			select {
			case repairedCh <- struct{}{}:
			default:
			}
			return nil, nil
		},
	}

	dialer := func(ctx context.Context, target string) (castorv1.DataServiceClient, io.Closer, error) {
		return &mockDataClient{
			replicateFunc: func(ctx context.Context, in *castorv1.ReplicateChunkRequest) (*castorv1.ReplicateChunkResponse, error) {
				return &castorv1.ReplicateChunkResponse{Success: true}, nil
			},
		}, nil, nil
	}

	healer := NewActiveReplicaHealer(scanner, nodeProvider, applier,
		WithHealInterval(10*time.Second), // Long interval so tick doesn't fire
		WithTargetReplicas(2),
		WithHealerPacingDelay(0),
		WithHealerDialer(dialer),
	)

	go healer.Run(ctx)

	// Send trigger
	healer.TriggerRepair(chunkHash)

	select {
	case <-repairedCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for triggered repair")
	}
}

func TestActiveReplicaHealer_ScanError(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := NewHealerMetrics(reg)

	expectedErr := errors.New("database disk full")
	scanner := &mockUnderReplicatedScanner{
		scanFunc: func(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return nil, expectedErr
		},
	}

	healer := NewActiveReplicaHealer(scanner, &mockNodeProvider{}, &mockCommandApplier{},
		WithHealerMetrics(metrics),
	)

	err := healer.RunOnce(ctx)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}

	if val := testutil.ToFloat64(metrics.ErrorsTotal.WithLabelValues("scan")); val != 1 {
		t.Errorf("expected 1 scan error, got %f", val)
	}
}

func TestActiveReplicaHealer_ReplicateFailure(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := NewHealerMetrics(reg)

	scanner := &mockUnderReplicatedScanner{
		scanFunc: func(ctx context.Context, targetReplicas int, limit int) ([]*castorv1.ChunkLocationRecord, error) {
			return []*castorv1.ChunkLocationRecord{
				{
					ChunkHash: "fail-hash",
					Nodes:     []string{"node1:9000"},
					RefCount:  1,
				},
			}, nil
		},
	}

	nodeProvider := &mockNodeProvider{
		nodes: []*castorv1.NodeCapacity{
			{NodeId: "node1", GrpcAddress: "node1:9000", FreeBytes: 100, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node2", GrpcAddress: "node2:9000", FreeBytes: 300, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		},
	}

	applier := &mockCommandApplier{}

	dialer := func(ctx context.Context, target string) (castorv1.DataServiceClient, io.Closer, error) {
		return &mockDataClient{
			replicateFunc: func(ctx context.Context, in *castorv1.ReplicateChunkRequest) (*castorv1.ReplicateChunkResponse, error) {
				return &castorv1.ReplicateChunkResponse{Success: false}, nil
			},
		}, nil, nil
	}

	healer := NewActiveReplicaHealer(scanner, nodeProvider, applier,
		WithTargetReplicas(2),
		WithHealerPacingDelay(0),
		WithHealerDialer(dialer),
		WithHealerMetrics(metrics),
	)

	if err := healer.RunOnce(ctx); err != nil {
		t.Fatalf("expected RunOnce to not abort on single chunk failure, got %v", err)
	}

	applier.mu.Lock()
	if len(applier.applied) != 0 {
		t.Errorf("expected 0 commands applied on replicate failure, got %d", len(applier.applied))
	}
	applier.mu.Unlock()

	if val := testutil.ToFloat64(metrics.ErrorsTotal.WithLabelValues("replicate_failed")); val != 1 {
		t.Errorf("expected 1 replicate_failed error, got %f", val)
	}
}

func TestActiveReplicaHealer_Run_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	healer := NewActiveReplicaHealer(&mockUnderReplicatedScanner{}, &mockNodeProvider{}, &mockCommandApplier{},
		WithHealInterval(10*time.Millisecond),
	)

	done := make(chan struct{})
	go func() {
		healer.Run(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("healer did not exit on context cancel")
	}
}
