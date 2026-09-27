package consensus

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/store"
)

func newTestRaftNode(t *testing.T, nodeID string, raftAddr string, peers map[string]string) (*RaftNode, *store.Store, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "raft-test-"+nodeID+"-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	s, err := store.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open in-memory store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	fsm := NewFSM(s)

	cfg := config.Config{
		NodeID:        nodeID,
		RaftAddr:      raftAddr,
		DataDir:       tmpDir,
		RaftBootstrap: true,
		Peers:         peers,
	}

	node, err := NewRaftNode(cfg, fsm)
	if err != nil {
		t.Fatalf("failed to initialize raft node: %v", err)
	}
	t.Cleanup(func() {
		_ = node.Shutdown()
	})

	return node, s, tmpDir
}

func TestRaftNode_Lifecycle(t *testing.T) {
	node, _, _ := newTestRaftNode(t, "node-1", "127.0.0.1:0", nil)

	if node == nil {
		t.Fatal("expected non-nil RaftNode")
	}

	// Verify methods do not panic
	_ = node.IsLeader()
	_ = node.LeaderAddr()
	_ = node.LeaderID()
	_ = node.Raft()

	if err := node.Shutdown(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestRaftNode_Apply(t *testing.T) {
	node, s, _ := newTestRaftNode(t, "node-1", "127.0.0.1:0", nil)

	req := &castorv1.CreateBucketMetadataRequest{
		Bucket:  "test-raft-bucket",
		OwnerId: "user-raft",
	}
	cmd, err := NewCreateBucketCommand(req)
	if err != nil {
		t.Fatalf("failed to create command: %v", err)
	}

	// Attempt Apply with timeout
	_, err = node.Apply(cmd, 3*time.Second)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	// Verify bucket was applied to store
	ctx := context.Background()
	exists, err := s.CheckBucketExists(ctx, "test-raft-bucket")
	if err != nil {
		t.Fatalf("failed checking bucket existence: %v", err)
	}
	if !exists {
		t.Fatal("expected bucket 'test-raft-bucket' to exist after raft apply")
	}
}

func TestRaftNode_MembershipChanges(t *testing.T) {
	node, _, _ := newTestRaftNode(t, "node-1", "127.0.0.1:0", nil)

	// Test Join proposed on node
	err := node.Join("node-2", "127.0.0.1:9092")
	if err != nil {
		t.Logf("join returned (expected if not leader): %v", err)
	}

	// Test Leave proposed on node
	err = node.Leave("node-2")
	if err != nil {
		t.Logf("leave returned (expected if not leader): %v", err)
	}
}
