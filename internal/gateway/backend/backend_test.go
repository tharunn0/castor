package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	dataconfig "github.com/tharunn0/castor/internal/data/config"
	dataserver "github.com/tharunn0/castor/internal/data/server"
	datastorage "github.com/tharunn0/castor/internal/data/storage"
	"github.com/tharunn0/castor/internal/gateway/config"
	"github.com/tharunn0/castor/internal/gateway/storage"
	metaconfig "github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	metaserver "github.com/tharunn0/castor/internal/metadata/server"
	"github.com/tharunn0/castor/internal/metadata/store"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCastorBackendBasics(t *testing.T) {
	be := &CastorBackend{}
	if be.String() != "castor" {
		t.Errorf("expected string 'castor', got %s", be.String())
	}
	be.Shutdown()
	if normalized := be.NormalizeObjectKey("bucket", "object/path"); normalized != "object/path" {
		t.Errorf("expected object/path, got %s", normalized)
	}
}

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

func TestCastorBackend_PutObject(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-2")
	defer cleanup2()

	_, metaClient, metaCleanup := startTestMetadataNode(t, "meta-1")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{node1Addr, node2Addr},
	}

	engine := storage.New(cfg, metaClient)
	defer engine.Close()

	be := New(engine, metaClient)
	ctx := context.Background()

	// 1. Missing bucket name
	emptyStr := ""
	validKey := "test.txt"
	_, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &emptyStr,
		Key:    &validKey,
	})
	if !errors.Is(err, s3err.GetAPIError(s3err.ErrInvalidBucketName)) {
		t.Fatalf("expected ErrInvalidBucketName, got %v", err)
	}

	// 2. Missing key
	validBucket := "mybucket"
	_, err = be.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &validBucket,
		Key:    &emptyStr,
	})
	if !errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchKey)) {
		t.Fatalf("expected ErrNoSuchKey, got %v", err)
	}

	// 3. Bucket does not exist
	bodyContent := "hello s3 backend"
	size := int64(len(bodyContent))
	_, err = be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &validBucket,
		Key:           &validKey,
		Body:          strings.NewReader(bodyContent),
		ContentLength: &size,
	})
	if !errors.Is(err, s3err.GetBucketErr(s3err.ErrNoSuchBucket, validBucket)) {
		t.Fatalf("expected ErrNoSuchBucket, got %v", err)
	}

	// 4. Create bucket in metadata store
	_, err = metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  validBucket,
		OwnerId: "admin",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 5. Successful PutObject
	h := sha256.Sum256([]byte(bodyContent))
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))

	out, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &validBucket,
		Key:           &validKey,
		Body:          bytes.NewReader([]byte(bodyContent)),
		ContentLength: &size,
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if out.ETag != expectedETag {
		t.Fatalf("etag mismatch: expected %s, got %s", expectedETag, out.ETag)
	}
	if out.Size == nil || *out.Size != size {
		t.Fatalf("size mismatch: expected %d, got %v", size, out.Size)
	}
}
