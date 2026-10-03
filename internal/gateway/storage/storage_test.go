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
	"google.golang.org/grpc/credentials/insecure"
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

func TestStorageEngineSignatures(t *testing.T) {
	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{"127.0.0.1:9101"},
	}

	engine := New(cfg, nil)
	defer engine.Close()

	ctx := context.Background()
	err := engine.DeleteObject(ctx, "test-bucket", "test-key")
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented, got %v", err)
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

	raftNode, err := consensus.NewRaftNode(raftCfg, fsm)
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


