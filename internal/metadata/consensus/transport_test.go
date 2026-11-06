package consensus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/store"
)

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func newAdvertisingRaftNode(t *testing.T, nodeID, bindAddr, advertise string, peers map[string]string) (*RaftNode, *store.Store) {
	t.Helper()

	tmpDir := t.TempDir()
	s, err := store.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	cfg := config.Config{
		NodeID:            nodeID,
		RaftAddr:          bindAddr,
		RaftAdvertiseAddr: advertise,
		DataDir:           tmpDir,
		RaftBootstrap:     true,
		Peers:             peers,
	}
	node, err := NewRaftNode(cfg, NewFSM(s, newTestLogger()), WithLogger(newTestLogger()))
	if err != nil {
		t.Fatalf("failed to start raft node %s: %v", nodeID, err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node, s
}

func waitForLeader(t *testing.T, nodes ...*RaftNode) *RaftNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func TestValidateAdvertiseAddr(t *testing.T) {
	tests := []struct {
		addr    string
		wantErr bool
	}{
		{addr: "metadata-0.metadata.castor.svc.cluster.local:9091", wantErr: false},
		{addr: "10.0.0.5:9091", wantErr: false},
		{addr: "localhost:9091", wantErr: false},
		{addr: ":9091", wantErr: true},
		{addr: "0.0.0.0:9091", wantErr: true},
		{addr: "[::]:9091", wantErr: true},
		{addr: "metadata-0", wantErr: true},
		{addr: "metadata-0:notaport", wantErr: true},
		{addr: "metadata-0:0", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			err := validateAdvertiseAddr(tt.addr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateAdvertiseAddr(%q) = %v, wantErr %v", tt.addr, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidAdvertiseAddr) {
				t.Fatalf("expected ErrInvalidAdvertiseAddr, got %v", err)
			}
		})
	}
}

func TestNewRaftNode_InvalidAdvertiseAddr(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	cfg := config.Config{
		NodeID:            "node-1",
		RaftAddr:          "127.0.0.1:0",
		RaftAdvertiseAddr: ":9091",
		DataDir:           tmpDir,
		RaftBootstrap:     true,
	}
	if _, err := NewRaftNode(cfg, NewFSM(s, newTestLogger())); !errors.Is(err, ErrInvalidAdvertiseAddr) {
		t.Fatalf("expected ErrInvalidAdvertiseAddr, got %v", err)
	}
}

func TestRaftNode_AdvertisesHostname(t *testing.T) {
	port := freeTCPPort(t)
	advertise := fmt.Sprintf("localhost:%d", port)

	node, _ := newAdvertisingRaftNode(t, "node-1", fmt.Sprintf(":%d", port), advertise, nil)
	waitForLeader(t, node)

	if got := node.LeaderAddr(); got != advertise {
		t.Fatalf("LeaderAddr() = %q, want advertised %q", got, advertise)
	}
}

func TestRaftNode_ClusterOverAdvertisedHostnames(t *testing.T) {
	ids := []string{"node-1", "node-2", "node-3"}
	ports := make(map[string]int, len(ids))
	peers := make(map[string]string, len(ids))
	for _, id := range ids {
		ports[id] = freeTCPPort(t)
		peers[id] = fmt.Sprintf("localhost:%d", ports[id])
	}

	nodes := make([]*RaftNode, 0, len(ids))
	stores := make([]*store.Store, 0, len(ids))
	for _, id := range ids {
		n, s := newAdvertisingRaftNode(t, id, fmt.Sprintf("127.0.0.1:%d", ports[id]), peers[id], peers)
		nodes = append(nodes, n)
		stores = append(stores, s)
	}

	leader := waitForLeader(t, nodes...)
	leaderAddr := leader.LeaderAddr()
	if !strings.HasPrefix(leaderAddr, "localhost:") {
		t.Fatalf("leader advertises %q, want a localhost:<port> hostname", leaderAddr)
	}

	cmd, err := NewCreateBucketCommand(&castorv1.CreateBucketMetadataRequest{Bucket: "advertised", OwnerId: "admin"})
	if err != nil {
		t.Fatalf("failed to build command: %v", err)
	}
	if _, err := leader.Apply(cmd, 5*time.Second); err != nil {
		t.Fatalf("apply on leader failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for i, n := range nodes {
		if n == leader {
			continue
		}
		for {
			exists, err := stores[i].CheckBucketExists(context.Background(), "advertised")
			if err == nil && exists && n.LeaderAddr() == leaderAddr {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("follower %s: bucket replicated=%v leader hint=%q (want %q)", ids[i], exists, n.LeaderAddr(), leaderAddr)
			}
			time.Sleep(20 * time.Millisecond)
		}

		_, err := n.Apply(cmd, time.Second)
		var nle *NotLeaderError
		if !errors.As(err, &nle) || nle.LeaderAddr != leaderAddr {
			t.Fatalf("follower %s apply error = %v, want NotLeaderError with hint %q", ids[i], err, leaderAddr)
		}
	}
}
