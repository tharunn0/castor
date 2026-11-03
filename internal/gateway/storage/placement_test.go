package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"testing"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	dataconfig "github.com/tharunn0/castor/internal/data/config"
	dataserver "github.com/tharunn0/castor/internal/data/server"
	datastorage "github.com/tharunn0/castor/internal/data/storage"
	"google.golang.org/grpc"
)

func startTestDataNode(t *testing.T, nodeID string) (string, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	casStore, err := datastorage.New(tmpDir, 4<<20)
	if err != nil {
		t.Fatalf("[%s] failed to initialize CAS storage: %v", nodeID, err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("[%s] failed to listen on loopback port: %v", nodeID, err)
	}

	addr := lis.Addr().String()
	nodeCfg := dataconfig.NodeConfig{
		NodeId:   nodeID,
		GRPCAddr: addr,
		DataDir:  tmpDir,
	}

	grpcServer := grpc.NewServer()
	srv := dataserver.New(casStore, nodeCfg)
	castorv1.RegisterDataServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	}

	return addr, cleanup
}

func computeSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func registerTestDataNode(t *testing.T, client castorv1.MetadataServiceClient, nodeID, addr string) {
	t.Helper()
	_, err := client.RegisterNode(context.Background(), &castorv1.RegisterNodeRequest{
		NodeId:      nodeID,
		GrpcAddress: addr,
		TotalBytes:  100 * (1 << 30),
		FreeBytes:   100 * (1 << 30),
	})
	if err != nil {
		t.Fatalf("failed to register test data node %s: %v", nodeID, err)
	}
}

func TestPlacementSelectNodes(t *testing.T) {
	pmEmpty := NewPlacementManager(2)
	if _, err := pmEmpty.SelectNodes(3); !errors.Is(err, ErrNoNodes) {
		t.Errorf("expected ErrNoNodes, got %v", err)
	}

	providerInsufficient := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return []*castorv1.NodeCapacity{
			{NodeId: "n1", GrpcAddress: "127.0.0.1:9101", FreeBytes: 50 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		}, nil
	}
	pmInsufficient := NewPlacementManager(2, WithActiveNodeProvider(providerInsufficient))
	if _, err := pmInsufficient.SelectNodes(2); !errors.Is(err, ErrQuorumNotMet) {
		t.Errorf("expected ErrQuorumNotMet, got %v", err)
	}

	providerHealthy := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return []*castorv1.NodeCapacity{
			{NodeId: "n1", GrpcAddress: "127.0.0.1:9101", FreeBytes: 20 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "n2", GrpcAddress: "127.0.0.1:9102", FreeBytes: 80 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "n3", GrpcAddress: "127.0.0.1:9103", FreeBytes: 60 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "n4", GrpcAddress: "127.0.0.1:9104", FreeBytes: 10 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		}, nil
	}
	pm := NewPlacementManager(2, WithActiveNodeProvider(providerHealthy))

	selected, err := pm.SelectNodes(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(selected))
	}
	if selected[0] != "127.0.0.1:9102" || selected[1] != "127.0.0.1:9103" {
		t.Errorf("expected ranking [127.0.0.1:9102, 127.0.0.1:9103], got %v", selected)
	}
}

func TestPlacementWriteAndReadQuorum(t *testing.T) {
	addr1, cleanup1 := startTestDataNode(t, "node-1")
	defer cleanup1()
	addr2, cleanup2 := startTestDataNode(t, "node-2")
	defer cleanup2()
	addr3, cleanup3 := startTestDataNode(t, "node-3")
	defer cleanup3()

	provider := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return []*castorv1.NodeCapacity{
			{NodeId: "node-1", GrpcAddress: addr1, FreeBytes: 100 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node-2", GrpcAddress: addr2, FreeBytes: 100 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node-3", GrpcAddress: addr3, FreeBytes: 100 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		}, nil
	}

	pm := NewPlacementManager(2, WithActiveNodeProvider(provider))
	defer pm.Close()

	ctx := context.Background()
	payload := []byte("hello castor distributed placement engine test")
	hash := computeSHA256(payload)

	targets, err := pm.SelectNodes(3)
	if err != nil {
		t.Fatalf("failed to select nodes: %v", err)
	}

	successfulNodes, err := pm.WriteChunkQuorum(ctx, hash, payload, targets)
	if err != nil {
		t.Fatalf("WriteChunkQuorum failed: %v", err)
	}
	if len(successfulNodes) < 2 {
		t.Fatalf("expected at least 2 successful nodes for quorum, got %d", len(successfulNodes))
	}

	readData, err := pm.ReadChunkFromNode(ctx, successfulNodes[0], hash, int64(len(payload)))
	if err != nil {
		t.Fatalf("ReadChunkFromNode failed: %v", err)
	}
	if string(readData) != string(payload) {
		t.Fatalf("data mismatch: expected %q, got %q", string(payload), string(readData))
	}

	partialData, err := pm.ReadChunkFromNode(ctx, successfulNodes[0], hash, 6)
	if err != nil {
		t.Fatalf("partial ReadChunkFromNode failed: %v", err)
	}
	if string(partialData) != "hello " {
		t.Fatalf("partial data mismatch: expected 'castor', got %q", string(partialData))
	}
}

func TestPlacementWriteQuorumFailure(t *testing.T) {
	addr1, cleanup1 := startTestDataNode(t, "node-1")
	defer cleanup1()

	provider := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return []*castorv1.NodeCapacity{
			{NodeId: "node-1", GrpcAddress: addr1, FreeBytes: 100 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node-dead-1", GrpcAddress: "127.0.0.1:54321", FreeBytes: 100 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node-dead-2", GrpcAddress: "127.0.0.1:54322", FreeBytes: 100 << 30, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		}, nil
	}

	pm := NewPlacementManager(2, WithActiveNodeProvider(provider))
	defer pm.Close()

	ctx := context.Background()
	payload := []byte("test-payload")
	hash := computeSHA256(payload)

	_, err := pm.WriteChunkQuorum(ctx, hash, payload, []string{addr1, "127.0.0.1:54321", "127.0.0.1:54322"})
	if !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("expected ErrQuorumNotMet, got %v", err)
	}
}

func TestPlacement_CapacityAwareRanking(t *testing.T) {
	provider := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return []*castorv1.NodeCapacity{
			{NodeId: "node-low", GrpcAddress: "10.0.0.1:9101", FreeBytes: 10 * (1 << 30), Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node-high", GrpcAddress: "10.0.0.2:9102", FreeBytes: 90 * (1 << 30), Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node-mid", GrpcAddress: "10.0.0.3:9103", FreeBytes: 50 * (1 << 30), Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		}, nil
	}

	pm := NewPlacementManager(2, WithActiveNodeProvider(provider))

	selected, err := pm.SelectNodes(2)
	if err != nil {
		t.Fatalf("SelectNodes failed: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(selected))
	}

	// Should select highest capacity nodes first (node-high then node-mid)
	if selected[0] != "10.0.0.2:9102" || selected[1] != "10.0.0.3:9103" {
		t.Errorf("expected ranking [10.0.0.2:9102, 10.0.0.3:9103], got %v", selected)
	}
}

func TestPlacement_OfflineAndLowCapacityFiltering(t *testing.T) {
	provider := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return []*castorv1.NodeCapacity{
			{NodeId: "node-offline", GrpcAddress: "10.0.0.1:9101", FreeBytes: 100 * (1 << 30), Status: castorv1.NodeStatus_NODE_STATUS_OFFLINE},
			{NodeId: "node-full", GrpcAddress: "10.0.0.2:9102", FreeBytes: 1 << 20, Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY}, // 1MB < 4MiB chunk boundary
			{NodeId: "node-healthy1", GrpcAddress: "10.0.0.3:9103", FreeBytes: 50 * (1 << 30), Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
			{NodeId: "node-healthy2", GrpcAddress: "10.0.0.4:9104", FreeBytes: 30 * (1 << 30), Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		}, nil
	}

	pm := NewPlacementManager(2, WithActiveNodeProvider(provider))

	selected, err := pm.SelectNodes(2)
	if err != nil {
		t.Fatalf("SelectNodes failed: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(selected))
	}
	if selected[0] != "10.0.0.3:9103" || selected[1] != "10.0.0.4:9104" {
		t.Errorf("expected healthy candidates [10.0.0.3:9103, 10.0.0.4:9104], got %v", selected)
	}
}

func TestPlacement_NoNodesAvailable(t *testing.T) {
	provider := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return nil, errors.New("metadata cluster unreachable")
	}

	pm := NewPlacementManager(2, WithActiveNodeProvider(provider))

	_, err := pm.SelectNodes(2)
	if !errors.Is(err, ErrNoNodes) {
		t.Fatalf("expected ErrNoNodes when metadata provider fails, got: %v", err)
	}
}

func TestPlacement_QuorumEnforcement(t *testing.T) {
	provider := func(ctx context.Context) ([]*castorv1.NodeCapacity, error) {
		return []*castorv1.NodeCapacity{
			{NodeId: "lone-node", GrpcAddress: "10.0.0.1:9101", FreeBytes: 10 * (1 << 30), Status: castorv1.NodeStatus_NODE_STATUS_HEALTHY},
		}, nil
	}

	pm := NewPlacementManager(2, WithActiveNodeProvider(provider))

	_, err := pm.SelectNodes(2)
	if !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("expected ErrQuorumNotMet, got %v", err)
	}
}
