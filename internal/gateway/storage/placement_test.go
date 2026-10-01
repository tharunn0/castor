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

func TestPlacementSelectNodes(t *testing.T) {
	pmEmpty := NewPlacementManager(nil, 2)
	if _, err := pmEmpty.SelectNodes(3); !errors.Is(err, ErrNoNodes) {
		t.Errorf("expected ErrNoNodes, got %v", err)
	}

	pmInsufficient := NewPlacementManager([]string{"n1"}, 2)
	if _, err := pmInsufficient.SelectNodes(2); !errors.Is(err, ErrQuorumNotMet) {
		t.Errorf("expected ErrQuorumNotMet, got %v", err)
	}

	nodes := []string{"n1", "n2", "n3", "n4"}
	pm := NewPlacementManager(nodes, 2)

	first, err := pm.SelectNodes(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(first))
	}

	second, err := pm.SelectNodes(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(second) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(second))
	}

	if first[0] == second[0] && first[1] == second[1] {
		t.Errorf("expected round-robin rotation, got same nodes: %v vs %v", first, second)
	}
}

func TestPlacementWriteAndReadQuorum(t *testing.T) {
	addr1, cleanup1 := startTestDataNode(t, "node-1")
	defer cleanup1()
	addr2, cleanup2 := startTestDataNode(t, "node-2")
	defer cleanup2()
	addr3, cleanup3 := startTestDataNode(t, "node-3")
	defer cleanup3()

	pm := NewPlacementManager([]string{addr1, addr2, addr3}, 2)
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

	pm := NewPlacementManager([]string{addr1, "127.0.0.1:54321", "127.0.0.1:54322"}, 2)
	defer pm.Close()

	ctx := context.Background()
	payload := []byte("test-payload")
	hash := computeSHA256(payload)

	_, err := pm.WriteChunkQuorum(ctx, hash, payload, []string{addr1, "127.0.0.1:54321", "127.0.0.1:54322"})
	if !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("expected ErrQuorumNotMet, got %v", err)
	}
}
