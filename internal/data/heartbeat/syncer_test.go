package heartbeat

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type mockDiskStore struct {
	total uint64
	free  uint64
	err   error
}

func (m *mockDiskStore) GetDiskStats() (storage.DiskStats, error) {
	if m.err != nil {
		return storage.DiskStats{}, m.err
	}
	return storage.DiskStats{
		TotalBytes: m.total,
		FreeBytes:  m.free,
	}, nil
}

type mockMetaServer struct {
	castorv1.UnimplementedMetadataServiceServer
	isLeader       bool
	leaderAddr     string
	mu             sync.Mutex
	registeredReqs []*castorv1.RegisterNodeRequest
	heartbeatReqs  []*castorv1.HeartbeatRequest
	heartbeatCount atomic.Int32
}

func (m *mockMetaServer) RegisterNode(_ context.Context, req *castorv1.RegisterNodeRequest) (*castorv1.RegisterNodeResponse, error) {
	if !m.isLeader {
		return nil, status.Errorf(codes.Unavailable, "not the raft leader: leader is %s", m.leaderAddr)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registeredReqs = append(m.registeredReqs, req)
	return &castorv1.RegisterNodeResponse{Registered: true}, nil
}

func (m *mockMetaServer) Heartbeat(_ context.Context, req *castorv1.HeartbeatRequest) (*castorv1.HeartbeatResponse, error) {
	if !m.isLeader {
		return nil, status.Errorf(codes.Unavailable, "not the raft leader: leader is %s", m.leaderAddr)
	}
	m.heartbeatCount.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatReqs = append(m.heartbeatReqs, req)
	return &castorv1.HeartbeatResponse{Acknowledged: true}, nil
}

func startMockServer(t *testing.T, srv *mockMetaServer) (*bufconn.Listener, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}
	return lis, cleanup
}

func TestExtractLeaderHint(t *testing.T) {
	err := status.Errorf(codes.Unavailable, "not the raft leader: leader is 10.0.0.2:9093")
	hint := ExtractLeaderHint(err)
	if hint != "10.0.0.2:9093" {
		t.Fatalf("expected 10.0.0.2:9093, got %q", hint)
	}

	plainErr := errors.New("not the raft leader: leader is 127.0.0.1:9091")
	hint = ExtractLeaderHint(plainErr)
	if hint != "127.0.0.1:9091" {
		t.Fatalf("expected 127.0.0.1:9091, got %q", hint)
	}

	if hint = ExtractLeaderHint(nil); hint != "" {
		t.Fatalf("expected empty hint on nil error, got %q", hint)
	}
	if hint = ExtractLeaderHint(errors.New("connection reset by peer")); hint != "" {
		t.Fatalf("expected empty hint on generic error, got %q", hint)
	}
}

func TestResolveLeaderAddress(t *testing.T) {
	seeds := []string{"127.0.0.1:9090", "127.0.0.1:9092", "127.0.0.1:9094"}

	// Raft port offset (9093 -> 9092)
	resolved := ResolveLeaderAddress("127.0.0.1:9093", seeds)
	if resolved != "127.0.0.1:9092" {
		t.Errorf("expected 127.0.0.1:9092, got %q", resolved)
	}

	// Exact match
	resolved = ResolveLeaderAddress("127.0.0.1:9090", seeds)
	if resolved != "127.0.0.1:9090" {
		t.Errorf("expected 127.0.0.1:9090, got %q", resolved)
	}

	// Hostname match across different containers
	containerSeeds := []string{"metadata-1:9090", "metadata-2:9090", "metadata-3:9090"}
	resolved = ResolveLeaderAddress("metadata-2:9091", containerSeeds)
	if resolved != "metadata-2:9090" {
		t.Errorf("expected metadata-2:9090, got %q", resolved)
	}
}

func TestSyncer_LeaderRedirection(t *testing.T) {
	followerSrv := &mockMetaServer{
		isLeader:   false,
		leaderAddr: "leader-node:9091",
	}
	followerLis, cleanupFollower := startMockServer(t, followerSrv)
	defer cleanupFollower()

	leaderSrv := &mockMetaServer{
		isLeader: true,
	}
	leaderLis, cleanupLeader := startMockServer(t, leaderSrv)
	defer cleanupLeader()

	listeners := map[string]*bufconn.Listener{
		"follower-node:9090": followerLis,
		"leader-node:9090":   leaderLis,
	}

	customDial := func(ctx context.Context, target string) (*grpc.ClientConn, castorv1.MetadataServiceClient, error) {
		lis, ok := listeners[target]
		if !ok {
			return nil, nil, errors.New("unknown target: " + target)
		}
		conn, err := grpc.NewClient("passthrough://bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return conn, castorv1.NewMetadataServiceClient(conn), nil
	}

	store := &mockDiskStore{
		total: 200 * (1 << 30),
		free:  150 * (1 << 30),
	}

	cfg := config.NodeConfig{
		NodeId:            "data-node-1",
		AdvertiseAddr:     "10.0.0.1:9101",
		MetadataNodes:     []string{"follower-node:9090", "leader-node:9090"},
		HeartbeatInterval: 25 * time.Millisecond,
	}

	syncer := NewSyncer(cfg, store, WithDialFunc(customDial))
	defer func() { _ = syncer.Stop() }()

	ctx := context.Background()

	// 1. Initial register should talk to follower, get redirected, and succeed on leader.
	if err := syncer.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if syncer.CurrentAddr() != "leader-node:9090" {
		t.Fatalf("expected syncer current address to be 'leader-node:9090', got %q", syncer.CurrentAddr())
	}

	leaderSrv.mu.Lock()
	if len(leaderSrv.registeredReqs) != 1 {
		t.Fatalf("expected 1 registration on leader, got %d", len(leaderSrv.registeredReqs))
	}
	reg := leaderSrv.registeredReqs[0]
	if reg.NodeId != "data-node-1" || reg.GrpcAddress != "10.0.0.1:9101" {
		t.Errorf("unexpected registration: %+v", reg)
	}
	leaderSrv.mu.Unlock()

	// 2. Heartbeat should now go directly to leader.
	if err := syncer.SendHeartbeat(ctx); err != nil {
		t.Fatalf("SendHeartbeat failed: %v", err)
	}

	leaderSrv.mu.Lock()
	if len(leaderSrv.heartbeatReqs) != 1 {
		t.Fatalf("expected 1 heartbeat on leader, got %d", len(leaderSrv.heartbeatReqs))
	}
	leaderSrv.mu.Unlock()
}

func TestSyncer_FailoverAndSeedRotation(t *testing.T) {
	leaderSrv := &mockMetaServer{isLeader: true}
	leaderLis, cleanupLeader := startMockServer(t, leaderSrv)
	defer cleanupLeader()

	listeners := map[string]*bufconn.Listener{
		"seed-live:9090": leaderLis,
	}

	customDial := func(ctx context.Context, target string) (*grpc.ClientConn, castorv1.MetadataServiceClient, error) {
		lis, ok := listeners[target]
		if !ok {
			return nil, nil, errors.New("connection refused: " + target)
		}
		conn, err := grpc.NewClient("passthrough://bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return conn, castorv1.NewMetadataServiceClient(conn), nil
	}

	cfg := config.NodeConfig{
		NodeId:            "data-node-failover",
		AdvertiseAddr:     "10.0.0.3:9101",
		MetadataNodes:     []string{"seed-dead:9090", "seed-live:9090"},
		HeartbeatInterval: 25 * time.Millisecond,
	}

	syncer := NewSyncer(cfg, nil, WithDialFunc(customDial))
	defer func() { _ = syncer.Stop() }()

	ctx := context.Background()
	// Should fail on seed-dead, rotate to seed-live, and succeed.
	if err := syncer.Register(ctx); err != nil {
		t.Fatalf("Register should succeed after failover: %v", err)
	}

	if syncer.CurrentAddr() != "seed-live:9090" {
		t.Fatalf("expected CurrentAddr to be 'seed-live:9090', got %q", syncer.CurrentAddr())
	}
}

func TestSyncer_PeriodicStartAndStop(t *testing.T) {
	leaderSrv := &mockMetaServer{isLeader: true}
	leaderLis, cleanup := startMockServer(t, leaderSrv)
	defer cleanup()

	customDial := func(ctx context.Context, target string) (*grpc.ClientConn, castorv1.MetadataServiceClient, error) {
		conn, err := grpc.NewClient("passthrough://bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return leaderLis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return conn, castorv1.NewMetadataServiceClient(conn), nil
	}

	store := &mockDiskStore{total: 1000, free: 500}
	cfg := config.NodeConfig{
		NodeId:            "data-node-2",
		AdvertiseAddr:     "10.0.0.2:9101",
		MetadataNodes:     []string{"127.0.0.1:9090"},
		HeartbeatInterval: 15 * time.Millisecond,
	}

	syncer := NewSyncer(cfg, store, WithDialFunc(customDial))
	syncer.Start(context.Background())

	deadline := time.Now().Add(500 * time.Millisecond)
	for leaderSrv.heartbeatCount.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	count := leaderSrv.heartbeatCount.Load()
	if count < 3 {
		t.Fatalf("expected at least 3 heartbeats, got %d", count)
	}

	if err := syncer.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	stoppedCount := leaderSrv.heartbeatCount.Load()
	time.Sleep(50 * time.Millisecond)
	if leaderSrv.heartbeatCount.Load() > stoppedCount+1 {
		t.Errorf("heartbeats continued after Stop: before=%d, after=%d", stoppedCount, leaderSrv.heartbeatCount.Load())
	}
}
