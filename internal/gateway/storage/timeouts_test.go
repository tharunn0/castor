package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scriptedMetadataServer answers CreateBucket and CheckBucketExists with
// whatever respond returns, so tests can model hung, leaderless or recovering nodes.
type scriptedMetadataServer struct {
	castorv1.UnimplementedMetadataServiceServer
	respond func(ctx context.Context) error
	calls   atomic.Int32
}

func (s *scriptedMetadataServer) CreateBucket(ctx context.Context, _ *castorv1.CreateBucketMetadataRequest) (*castorv1.CreateBucketMetadataResponse, error) {
	s.calls.Add(1)
	if err := s.respond(ctx); err != nil {
		return nil, err
	}
	return &castorv1.CreateBucketMetadataResponse{}, nil
}

func (s *scriptedMetadataServer) CheckBucketExists(ctx context.Context, _ *castorv1.CheckBucketExistsRequest) (*castorv1.CheckBucketExistsResponse, error) {
	s.calls.Add(1)
	if err := s.respond(ctx); err != nil {
		return nil, err
	}
	return &castorv1.CheckBucketExistsResponse{Exists: true}, nil
}

func startScriptedMetadataNode(t *testing.T, respond func(ctx context.Context) error) (string, *scriptedMetadataServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	impl := &scriptedMetadataServer{respond: respond}
	srv := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), impl
}

// startBlackholeListener accepts TCP connections but never speaks HTTP/2,
// which is what a dial to a vanished pod IP looks like to gRPC: no RST, no reply.
func startBlackholeListener(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	t.Cleanup(func() { _ = lis.Close() })
	return lis.Addr().String()
}

func hangUntilCancelled(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func noLeader(context.Context) error {
	return status.Error(codes.Unavailable, "raft leader unavailable")
}

func TestExecuteWithLeader_HungLeaderFailsOver(t *testing.T) {
	hungAddr, _ := startScriptedMetadataNode(t, hangUntilCancelled)
	leaderAddr, _, cleanup := startTestMetadataNode(t, "leader-after-hang")
	defer cleanup()

	engine := New(config.Config{
		MetadataNodes:        []string{hungAddr, leaderAddr},
		MetadataCallTimeout:  300 * time.Millisecond,
		ChunkSize:            4096,
		WriteQuorum:          1,
		MaxConcurrentUploads: 2,
	}, nil)
	defer engine.Close()

	start := time.Now()
	if err := engine.CreateBucket(context.Background(), "after-hang", "admin", nil); err != nil {
		t.Fatalf("CreateBucket should fail over past the hung node: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("failover took %v, want about one call timeout (300ms)", elapsed)
	}

	start = time.Now()
	if exists, err := engine.CheckBucketExists(context.Background(), "after-hang"); err != nil || !exists {
		t.Fatalf("CheckBucketExists = %v, %v", exists, err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("second call took %v; the new leader should have been cached", elapsed)
	}
}

func TestExecuteWithLeader_BlackholedNodeFailsOver(t *testing.T) {
	blackhole := startBlackholeListener(t)
	leaderAddr, _, cleanup := startTestMetadataNode(t, "leader-after-blackhole")
	defer cleanup()

	engine := New(config.Config{
		MetadataNodes:        []string{blackhole, leaderAddr},
		ChunkSize:            4096,
		WriteQuorum:          1,
		MaxConcurrentUploads: 2,
	}, nil)
	defer engine.Close()

	start := time.Now()
	if err := engine.CreateBucket(context.Background(), "after-blackhole", "admin", nil); err != nil {
		t.Fatalf("CreateBucket should fail over past the blackholed node: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("failover took %v, want well under gRPC's 20s default connect timeout", elapsed)
	}
}

func TestExecuteWithLeader_NoLeaderFailsFastWithUnavailable(t *testing.T) {
	addr1, _ := startScriptedMetadataNode(t, noLeader)
	addr2, _ := startScriptedMetadataNode(t, noLeader)

	engine := New(config.Config{
		MetadataNodes:      []string{addr1, addr2},
		MetadataLeaderWait: 300 * time.Millisecond,
	}, nil)
	defer engine.Close()

	start := time.Now()
	err := engine.CreateBucket(context.Background(), "no-leader", "admin", nil)
	if !errors.Is(err, ErrMetadataUnavailable) {
		t.Fatalf("expected ErrMetadataUnavailable, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("no-leader failure took %v, want about MetadataLeaderWait (300ms)", elapsed)
	}
}

func TestExecuteWithLeader_RidesThroughElection(t *testing.T) {
	electedAt := time.Now().Add(250 * time.Millisecond)
	addr, impl := startScriptedMetadataNode(t, func(context.Context) error {
		if time.Now().Before(electedAt) {
			return status.Error(codes.Unavailable, "raft leader unavailable")
		}
		return nil
	})

	engine := New(config.Config{
		MetadataNodes:      []string{addr},
		MetadataLeaderWait: 2 * time.Second,
	}, nil)
	defer engine.Close()

	if err := engine.CreateBucket(context.Background(), "during-election", "admin", nil); err != nil {
		t.Fatalf("CreateBucket should succeed once a leader is elected: %v", err)
	}
	if impl.calls.Load() < 2 {
		t.Fatalf("expected retries during the election, got %d call(s)", impl.calls.Load())
	}
}

func TestExecuteWithLeader_CallerCancellationWins(t *testing.T) {
	addr, _ := startScriptedMetadataNode(t, noLeader)

	engine := New(config.Config{
		MetadataNodes:      []string{addr},
		MetadataLeaderWait: 10 * time.Second,
	}, nil)
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := engine.CreateBucket(ctx, "cancelled", "admin", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the caller's deadline error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("returned after %v, want right after the caller's 200ms deadline", elapsed)
	}
}

func TestExecuteWithLeader_IgnoresUnmappedLeaderHint(t *testing.T) {
	leaderAddr, _, cleanup := startTestMetadataNode(t, "leader-unmapped-hint")
	defer cleanup()

	// A raft-port IP that matches no configured gRPC node.
	followerAddr, cleanupFollower := startMockFollowerNode(t, "10.255.0.9:9091")
	defer cleanupFollower()

	engine := New(config.Config{
		MetadataNodes:        []string{followerAddr, leaderAddr},
		ChunkSize:            4096,
		WriteQuorum:          1,
		MaxConcurrentUploads: 2,
	}, nil)
	defer engine.Close()

	if err := engine.CreateBucket(context.Background(), "unmapped-hint", "admin", nil); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if nodes := engine.AvailableMetadataNodes(); len(nodes) != 2 {
		t.Fatalf("unmapped hint was added to the node list: %v", nodes)
	}
}

func TestPlacement_ChunkTimeoutBoundsHungDataNode(t *testing.T) {
	hungAddr, cleanup := startDelayedDataNode(t, "hung-data", 2*time.Second)
	defer cleanup()

	pm := NewPlacementManager(1, WithChunkTimeout(300*time.Millisecond))
	defer pm.Close()

	payload := []byte("chunk-timeout-payload")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	start := time.Now()
	if err := pm.StreamChunkToNode(context.Background(), hungAddr, hash, payload); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("StreamChunkToNode error = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("write to hung node took %v, want about the 300ms chunk timeout", elapsed)
	}

	start = time.Now()
	if _, err := pm.ReadChunkFromNode(context.Background(), hungAddr, hash, int64(len(payload))); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("ReadChunkFromNode error = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("read from hung node took %v, want about the 300ms chunk timeout", elapsed)
	}
}
