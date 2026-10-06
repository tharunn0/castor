package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/config"
	metaconfig "github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	metaserver "github.com/tharunn0/castor/internal/metadata/server"
	"github.com/tharunn0/castor/internal/metadata/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestBufferPoolAcquireRelease(t *testing.T) {
	pool := NewBufferPool(1024, 2)
	buf1 := pool.Acquire()
	if len(*buf1) != 1024 {
		t.Fatalf("expected buffer size 1024, got %d", len(*buf1))
	}
	buf2 := pool.Acquire()
	if len(*buf2) != 1024 {
		t.Fatalf("expected buffer size 1024, got %d", len(*buf2))
	}

	pool.Release(buf1)
	pool.Release(buf2)
}

func TestStorageEngine_DeleteObject(t *testing.T) {
	metaAddr, metaClient, metaCleanup := startTestMetadataNode(t, "meta-del-obj")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{"127.0.0.1:9101"},
		MetadataAddr:         metaAddr,
	}

	engine := New(cfg, metaClient)
	defer engine.Close()

	ctx := context.Background()

	if err := engine.DeleteObject(ctx, "", "file.txt"); err == nil {
		t.Fatal("expected error on empty bucket name")
	}
	if err := engine.DeleteObject(ctx, "bkt", ""); err == nil {
		t.Fatal("expected error on empty object key")
	}

	if err := engine.DeleteObject(ctx, "nonexistent-bkt", "file.txt"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}

	bucketName := "obj-del-bucket"
	if err := engine.CreateBucket(ctx, bucketName, "admin", nil); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	if err := engine.DeleteObject(ctx, bucketName, "missing.txt"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	chunkID := "del-chunk-123"
	_, err := metaClient.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:   bucketName,
		Key:      "sample.txt",
		Size:     100,
		Etag:     "testetag",
		ChunkIds: []string{chunkID},
		OwnerId:  "admin",
	})
	if err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}

	m, err := engine.GetManifest(ctx, bucketName, "sample.txt")
	if err != nil || m == nil {
		t.Fatalf("expected manifest to exist, got %v", err)
	}

	if err := engine.DeleteObject(ctx, bucketName, "sample.txt"); err != nil {
		t.Fatalf("failed to delete object: %v", err)
	}

	_, _, err = engine.GetObject(ctx, bucketName, "sample.txt")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound after deletion, got %v", err)
	}

	if err := engine.DeleteObject(ctx, bucketName, "sample.txt"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound on duplicate delete, got %v", err)
	}

	if err := engine.DeleteBucket(ctx, bucketName); err != nil {
		t.Fatalf("expected DeleteBucket to succeed after object deleted, got %v", err)
	}
}

func startTestMetadataNode(t *testing.T, nodeID string) (string, castorv1.MetadataServiceClient, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	badgerStore, err := store.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open test metadata store: %v", err)
	}

	fsm := consensus.NewFSM(badgerStore)
	raftCfg := metaconfig.Config{
		NodeID:        nodeID,
		RaftAddr:      "127.0.0.1:0",
		DataDir:       tmpDir,
		RaftBootstrap: true,
	}

	raftNode, err := consensus.NewRaftNode(raftCfg, nil, fsm)
	if err != nil {
		_ = badgerStore.Close()
		t.Fatalf("failed to initialize test Raft node: %v", err)
	}

	for range 100 {
		if raftNode.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !raftNode.IsLeader() {
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
		t.Fatal("timed out waiting for metadata node to become Raft leader")
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
		t.Fatalf("failed to listen for metadata gRPC: %v", err)
	}

	addr := lis.Addr().String()
	metaSrv := metaserver.New(badgerStore, raftNode)
	grpcServer := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(grpcServer, metaSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.GracefulStop()
		_ = lis.Close()
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
		t.Fatalf("failed to connect to metadata gRPC: %v", err)
	}
	metaClient := castorv1.NewMetadataServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		grpcServer.GracefulStop()
		_ = lis.Close()
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
	}

	return addr, metaClient, cleanup
}

func TestStorageEngine_PutObject(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-3")
	defer cleanup3()

	_, metaClient, metaCleanup := startTestMetadataNode(t, "meta-1")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            1024,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{node1Addr, node2Addr, node3Addr},
	}

	engine := New(cfg, metaClient)
	defer engine.Close()

	ctx := context.Background()

	// 1. Validation errors
	if _, err := engine.PutObject(ctx, "", "key", "u1", strings.NewReader("hi"), 2); err == nil {
		t.Fatal("expected error for empty bucket")
	}
	if _, err := engine.PutObject(ctx, "b", "", "u1", strings.NewReader("hi"), 2); err == nil {
		t.Fatal("expected error for empty key")
	}

	// 2. Bucket does not exist
	_, err := engine.PutObject(ctx, "nonexistent-bucket", "obj1", "user1", strings.NewReader("test"), 4)
	if !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}

	// 3. Create bucket
	_, err = metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  "test-bucket",
		OwnerId: "user1",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 4. Multi-chunk PutObject (2500 bytes with 1024 chunk size = 3 chunks)
	data := make([]byte, 2500)
	_, _ = rand.Read(data)
	h := sha256.Sum256(data)
	expectedETag := hex.EncodeToString(h[:])

	etag, err := engine.PutObject(ctx, "test-bucket", "multi-chunk.dat", "user1", bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if etag != expectedETag {
		t.Fatalf("etag mismatch: expected %s, got %s", expectedETag, etag)
	}

	// Verify manifest in metadata-svc
	manifestResp, err := metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: "test-bucket",
		Key:    "multi-chunk.dat",
	})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if manifestResp.GetSize() != 2500 {
		t.Fatalf("manifest size mismatch: expected 2500, got %d", manifestResp.GetSize())
	}
	if manifestResp.GetEtag() != expectedETag {
		t.Fatalf("manifest etag mismatch: expected %s, got %s", expectedETag, manifestResp.GetEtag())
	}
	if len(manifestResp.GetChunks()) != 3 {
		t.Fatalf("expected 3 chunks in manifest, got %d", len(manifestResp.GetChunks()))
	}
	for i, chunk := range manifestResp.GetChunks() {
		if len(chunk.GetNodeAddresses()) < 2 {
			t.Fatalf("chunk %d has fewer than write quorum replicas: %v", i, chunk.GetNodeAddresses())
		}
	}

	// 5. Zero-byte PutObject
	emptyETag := hex.EncodeToString(sha256.New().Sum(nil))
	zeroETag, err := engine.PutObject(ctx, "test-bucket", "empty.txt", "user1", bytes.NewReader(nil), 0)
	if err != nil {
		t.Fatalf("zero-byte PutObject failed: %v", err)
	}
	if zeroETag != emptyETag {
		t.Fatalf("zero-byte etag mismatch: expected %s, got %s", emptyETag, zeroETag)
	}
}

func TestStorageEngine_GetObject(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-3")
	defer cleanup3()

	_, metaClient, metaCleanup := startTestMetadataNode(t, "meta-1")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4 << 20,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{node1Addr, node2Addr, node3Addr},
	}

	engine := New(cfg, metaClient)
	defer engine.Close()

	ctx := context.Background()

	// 1. Validation errors
	if _, _, err := engine.GetObject(ctx, "", "key"); err == nil {
		t.Fatal("expected error for empty bucket")
	}
	if _, _, err := engine.GetObject(ctx, "bucket", ""); err == nil {
		t.Fatal("expected error for empty key")
	}

	// 2. Bucket does not exist
	if _, _, err := engine.GetObject(ctx, "nonexistent-bucket", "obj1"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}

	// Create bucket
	bucketName := "test-get-bucket"
	_, err := metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucketName,
		OwnerId: "user1",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 3. Object does not exist
	if _, _, err := engine.GetObject(ctx, bucketName, "nonexistent-key"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}

	// 4. Zero-byte GetObject
	_, err = engine.PutObject(ctx, bucketName, "empty.txt", "user1", bytes.NewReader(nil), 0)
	if err != nil {
		t.Fatalf("failed to put zero-byte object: %v", err)
	}
	rc, info, err := engine.GetObject(ctx, bucketName, "empty.txt")
	if err != nil {
		t.Fatalf("failed to get zero-byte object: %v", err)
	}
	defer rc.Close()
	if info.Size != 0 {
		t.Fatalf("expected size 0, got %d", info.Size)
	}
	emptyData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("failed reading zero-byte object: %v", err)
	}
	if len(emptyData) != 0 {
		t.Fatalf("expected 0 bytes, got %d", len(emptyData))
	}

	// 5. Multi-chunk GetObject (5MiB payload > 4MiB system chunk size)
	payload := make([]byte, 5*1024*1024)
	_, _ = rand.Read(payload)
	h := sha256.Sum256(payload)
	expectedETag := hex.EncodeToString(h[:])

	putETag, err := engine.PutObject(ctx, bucketName, "multi.bin", "user1", bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatalf("failed to put multi-chunk object: %v", err)
	}
	if putETag != expectedETag {
		t.Fatalf("etag mismatch: expected %s, got %s", expectedETag, putETag)
	}

	rc, info, err = engine.GetObject(ctx, bucketName, "multi.bin")
	if err != nil {
		t.Fatalf("failed to get multi-chunk object: %v", err)
	}
	defer rc.Close()

	if info.Size != int64(len(payload)) {
		t.Fatalf("size mismatch: expected %d, got %d", len(payload), info.Size)
	}
	if info.ETag != expectedETag {
		t.Fatalf("etag mismatch: expected %s, got %s", expectedETag, info.ETag)
	}

	readData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("failed to read full object: %v", err)
	}
	if !bytes.Equal(readData, payload) {
		t.Fatal("read content does not match uploaded payload")
	}

	// 6. Range read spanning across chunk boundary
	rangeStart := int64((4 << 20) - 50)
	rangeLen := int64(100)
	expectedRangeBytes := payload[rangeStart : rangeStart+rangeLen]

	rangeRc, rangeInfo, err := engine.GetObjectRange(ctx, bucketName, "multi.bin", rangeStart, rangeLen)
	if err != nil {
		t.Fatalf("failed to get object range: %v", err)
	}
	defer rangeRc.Close()
	if rangeInfo.Size != int64(len(payload)) {
		t.Fatalf("range info size mismatch: expected %d, got %d", len(payload), rangeInfo.Size)
	}

	rangeData, err := io.ReadAll(rangeRc)
	if err != nil {
		t.Fatalf("failed to read range data: %v", err)
	}
	if !bytes.Equal(rangeData, expectedRangeBytes) {
		t.Fatal("range data mismatch across chunk boundary")
	}

	// 7. Replica failover: shut down node 1 and verify GetObject still works from surviving replicas
	cleanup1()

	failoverRc, _, err := engine.GetObject(ctx, bucketName, "multi.bin")
	if err != nil {
		t.Fatalf("failed to get object after replica shutdown: %v", err)
	}
	defer failoverRc.Close()

	failoverData, err := io.ReadAll(failoverRc)
	if err != nil {
		t.Fatalf("failed to read object data with replica failover: %v", err)
	}
	if !bytes.Equal(failoverData, payload) {
		t.Fatal("failover read content does not match uploaded payload")
	}
}

func TestStorageEngine_CreateBucket(t *testing.T) {
	metaAddr, metaClient, metaCleanup := startTestMetadataNode(t, "meta-cb")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{"127.0.0.1:9101"},
		MetadataAddr:         metaAddr,
	}

	engine := New(cfg, metaClient)
	defer engine.Close()

	ctx := context.Background()

	// 1. Validation error on empty bucket name
	if err := engine.CreateBucket(ctx, "", "admin", nil); err == nil {
		t.Fatal("expected error on empty bucket name, got nil")
	}

	// 2. Successful bucket creation
	bucketName := "new-bucket"
	if err := engine.CreateBucket(ctx, bucketName, "admin", map[string]string{"env": "test"}); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 3. Verify bucket exists in metadata
	existsResp, err := metaClient.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: bucketName})
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if !existsResp.GetExists() {
		t.Fatal("expected bucket to exist in metadata-svc")
	}

	// 4. Duplicate bucket creation returns ErrBucketAlreadyExists
	if err := engine.CreateBucket(ctx, bucketName, "admin", nil); !errors.Is(err, ErrBucketAlreadyExists) {
		t.Fatalf("expected ErrBucketAlreadyExists on duplicate create, got %v", err)
	}
}

func TestStorageEngine_ListBuckets(t *testing.T) {
	metaAddr, metaClient, metaCleanup := startTestMetadataNode(t, "meta-list-b")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{"127.0.0.1:9101"},
		MetadataAddr:         metaAddr,
	}

	engine := New(cfg, metaClient)
	defer engine.Close()

	ctx := context.Background()

	initialBuckets, err := engine.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets on empty metadata failed: %v", err)
	}
	if len(initialBuckets) != 0 {
		t.Fatalf("expected 0 buckets, got %d", len(initialBuckets))
	}

	b1 := "alpha-bucket"
	b2 := "beta-bucket"
	if err := engine.CreateBucket(ctx, b1, "user-1", nil); err != nil {
		t.Fatalf("failed to create bucket 1: %v", err)
	}
	if err := engine.CreateBucket(ctx, b2, "user-2", nil); err != nil {
		t.Fatalf("failed to create bucket 2: %v", err)
	}

	buckets, err := engine.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}

	found := make(map[string]string)
	for _, b := range buckets {
		found[b.Name] = b.OwnerID
	}
	if found[b1] != "user-1" {
		t.Fatalf("expected bucket %s owner 'user-1', got '%s'", b1, found[b1])
	}
	if found[b2] != "user-2" {
		t.Fatalf("expected bucket %s owner 'user-2', got '%s'", b2, found[b2])
	}
}

func TestStorageEngine_DeleteBucket(t *testing.T) {
	metaAddr, metaClient, metaCleanup := startTestMetadataNode(t, "meta-del-b")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{"127.0.0.1:9101"},
		MetadataAddr:         metaAddr,
	}

	engine := New(cfg, metaClient)
	defer engine.Close()

	ctx := context.Background()

	// 1. Validation error on empty bucket name
	if err := engine.DeleteBucket(ctx, ""); err == nil {
		t.Fatal("expected error on empty bucket name, got nil")
	}

	// 2. Non-existent bucket returns ErrBucketNotFound
	if err := engine.DeleteBucket(ctx, "nonexistent-bucket"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}

	// 3. Create bucket and commit a manifest to simulate non-empty bucket
	bucketName := "del-test-bucket"
	if err := engine.CreateBucket(ctx, bucketName, "admin", nil); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	_, err := metaClient.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:   bucketName,
		Key:      "sample.txt",
		Size:     10,
		Etag:     "testetag",
		ChunkIds: []string{"chunk1"},
		OwnerId:  "admin",
	})
	if err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}

	// Deleting non-empty bucket returns ErrBucketNotEmpty
	if err := engine.DeleteBucket(ctx, bucketName); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("expected ErrBucketNotEmpty on non-empty bucket, got %v", err)
	}

	// 4. Create empty bucket and successfully delete it
	emptyBucket := "empty-del-bucket"
	if err := engine.CreateBucket(ctx, emptyBucket, "admin", nil); err != nil {
		t.Fatalf("failed to create empty bucket: %v", err)
	}

	if err := engine.DeleteBucket(ctx, emptyBucket); err != nil {
		t.Fatalf("failed to delete empty bucket: %v", err)
	}

	// Verify bucket no longer exists
	existsResp, err := metaClient.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: emptyBucket})
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if existsResp.GetExists() {
		t.Fatal("expected bucket to no longer exist after deletion")
	}
}

type mockFollowerServer struct {
	castorv1.UnimplementedMetadataServiceServer
	leaderAddr string
}

func (m *mockFollowerServer) CreateBucket(ctx context.Context, req *castorv1.CreateBucketMetadataRequest) (*castorv1.CreateBucketMetadataResponse, error) {
	return nil, status.Errorf(codes.Unavailable, "not the raft leader: leader is %s", m.leaderAddr)
}

func (m *mockFollowerServer) CheckBucketExists(ctx context.Context, req *castorv1.CheckBucketExistsRequest) (*castorv1.CheckBucketExistsResponse, error) {
	return nil, status.Errorf(codes.Unavailable, "not the raft leader: leader is %s", m.leaderAddr)
}

func (m *mockFollowerServer) CommitManifest(ctx context.Context, req *castorv1.CommitManifestRequest) (*castorv1.CommitManifestResponse, error) {
	return nil, status.Errorf(codes.Unavailable, "not the raft leader: leader is %s", m.leaderAddr)
}

func (m *mockFollowerServer) DeleteManifest(ctx context.Context, req *castorv1.DeleteManifestRequest) (*castorv1.DeleteManifestResponse, error) {
	return nil, status.Errorf(codes.Unavailable, "not the raft leader: leader is %s", m.leaderAddr)
}

func startMockFollowerNode(t *testing.T, leaderAddr string) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on follower port: %v", err)
	}

	server := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(server, &mockFollowerServer{leaderAddr: leaderAddr})

	go func() {
		_ = server.Serve(lis)
	}()

	return lis.Addr().String(), func() {
		server.Stop()
		_ = lis.Close()
	}
}

func TestStorageEngine_MultiNodeRoutingAndFailover(t *testing.T) {
	leaderAddr, _, cleanupLeader := startTestMetadataNode(t, "real-leader")
	defer cleanupLeader()

	followerAddr, cleanupFollower := startMockFollowerNode(t, leaderAddr)
	defer cleanupFollower()

	cfg := config.Config{
		MetadataNodes:        []string{followerAddr, leaderAddr},
		ChunkSize:            4096,
		WriteQuorum:          1,
		MaxConcurrentUploads: 2,
	}

	engine := New(cfg, nil)
	defer engine.Close()

	ctx := context.Background()

	// 1. Create bucket should hit follower, fail with not leader, extract leader hint, route to leader, and succeed
	bucketName := "failover-bucket"
	err := engine.CreateBucket(ctx, bucketName, "admin", nil)
	if err != nil {
		t.Fatalf("expected CreateBucket to succeed with failover, got %v", err)
	}

	// 2. CheckBucketExists should now hit leader directly and confirm existence
	exists, err := engine.CheckBucketExists(ctx, bucketName)
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if !exists {
		t.Fatal("expected bucket to exist on leader")
	}

	// 3. FindLeader should identify the leader
	_, activeLeader, err := engine.FindLeader(ctx)
	if err != nil {
		t.Fatalf("FindLeader failed: %v", err)
	}
	if activeLeader != leaderAddr {
		t.Fatalf("expected leader %s, got %s", leaderAddr, activeLeader)
	}
}

func TestStorageEngine_MetadataNodesManagement(t *testing.T) {
	cfg := config.Config{
		MetadataNodes: []string{"127.0.0.1:9090", "127.0.0.1:9092"},
	}
	engine := New(cfg, nil)
	defer engine.Close()

	nodes := engine.AvailableMetadataNodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	// Add new node
	engine.AddMetadataNode("127.0.0.1:9094")
	nodes = engine.AvailableMetadataNodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes after add, got %d", len(nodes))
	}

	// Add duplicate node should not add twice
	engine.AddMetadataNode("127.0.0.1:9094")
	nodes = engine.AvailableMetadataNodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes after duplicate add, got %d", len(nodes))
	}
}

