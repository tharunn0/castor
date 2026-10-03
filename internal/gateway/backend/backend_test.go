package backend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func TestCastorBackend_GetObject(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-2")
	defer cleanup2()

	_, metaClient, metaCleanup := startTestMetadataNode(t, "meta-1")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4 << 20,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{node1Addr, node2Addr},
	}

	engine := storage.New(cfg, metaClient)
	defer engine.Close()

	be := New(engine, metaClient)
	ctx := context.Background()

	// 1. Validation errors
	emptyStr := ""
	validBucket := "backend-get-bucket"
	validKey := "test-file.txt"

	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &emptyStr,
		Key:    &validKey,
	}); !errors.Is(err, s3err.GetAPIError(s3err.ErrInvalidBucketName)) {
		t.Fatalf("expected ErrInvalidBucketName, got %v", err)
	}

	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &validBucket,
		Key:    &emptyStr,
	}); !errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchKey)) {
		t.Fatalf("expected ErrNoSuchKey, got %v", err)
	}

	// 2. Non-existent bucket
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &validBucket,
		Key:    &validKey,
	}); !errors.Is(err, s3err.GetBucketErr(s3err.ErrNoSuchBucket, validBucket)) {
		t.Fatalf("expected ErrNoSuchBucket, got %v", err)
	}

	// Create bucket
	_, err := metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  validBucket,
		OwnerId: "admin",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 3. Non-existent object
	if _, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &validBucket,
		Key:    &validKey,
	}); !errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchKey)) {
		t.Fatalf("expected ErrNoSuchKey, got %v", err)
	}

	// 4. Upload object and perform full GetObject
	content := "Hello Castor Backend GetObject!"
	contentLen := int64(len(content))
	h := sha256.Sum256([]byte(content))
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))

	_, err = be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &validBucket,
		Key:           &validKey,
		Body:          strings.NewReader(content),
		ContentLength: &contentLen,
	})
	if err != nil {
		t.Fatalf("failed to put test object: %v", err)
	}

	getOut, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &validBucket,
		Key:    &validKey,
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer getOut.Body.Close()

	if getOut.ETag == nil || *getOut.ETag != expectedETag {
		t.Fatalf("etag mismatch: expected %s, got %v", expectedETag, getOut.ETag)
	}
	if getOut.ContentLength == nil || *getOut.ContentLength != contentLen {
		t.Fatalf("content length mismatch: expected %d, got %v", contentLen, getOut.ContentLength)
	}
	if getOut.ContentType == nil || *getOut.ContentType != "application/octet-stream" {
		t.Fatalf("content type mismatch: expected application/octet-stream, got %v", getOut.ContentType)
	}
	if getOut.AcceptRanges == nil || *getOut.AcceptRanges != "bytes" {
		t.Fatalf("accept ranges mismatch: expected bytes, got %v", getOut.AcceptRanges)
	}

	bodyData, err := io.ReadAll(getOut.Body)
	if err != nil {
		t.Fatalf("failed to read GetObject body: %v", err)
	}
	if string(bodyData) != content {
		t.Fatalf("body mismatch: expected %q, got %q", content, string(bodyData))
	}

	// 5. Range GetObject
	rangeHeader := "bytes=6-11"
	rangeOut, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &validBucket,
		Key:    &validKey,
		Range:  &rangeHeader,
	})
	if err != nil {
		t.Fatalf("range GetObject failed: %v", err)
	}
	defer rangeOut.Body.Close()

	expectedRange := fmt.Sprintf("bytes 6-11/%d", contentLen)
	if rangeOut.ContentRange == nil || *rangeOut.ContentRange != expectedRange {
		t.Fatalf("content range mismatch: expected %s, got %v", expectedRange, rangeOut.ContentRange)
	}
	if rangeOut.ContentLength == nil || *rangeOut.ContentLength != 6 {
		t.Fatalf("range length mismatch: expected 6, got %v", rangeOut.ContentLength)
	}
	rangeBytes, err := io.ReadAll(rangeOut.Body)
	if err != nil {
		t.Fatalf("failed reading range body: %v", err)
	}
	if string(rangeBytes) != content[6:12] {
		t.Fatalf("range content mismatch: expected %q, got %q", content[6:12], string(rangeBytes))
	}

	// 6. Precondition evaluation
	// Matching IfMatch succeeds
	matchETag := hex.EncodeToString(h[:])
	matchedOut, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket:  &validBucket,
		Key:     &validKey,
		IfMatch: &matchETag,
	})
	if err != nil {
		t.Fatalf("IfMatch GetObject failed: %v", err)
	}
	_ = matchedOut.Body.Close()

	// Mismatched IfMatch fails with PreconditionFailed
	wrongETag := "deadbeef"
	_, err = be.GetObject(ctx, &s3.GetObjectInput{
		Bucket:  &validBucket,
		Key:     &validKey,
		IfMatch: &wrongETag,
	})
	if !errors.Is(err, s3err.GetPreconditionFailedErr(s3err.ConditionIfMatch)) {
		t.Fatalf("expected ConditionIfMatch precondition error, got %v", err)
	}

	// 7. Multi-chunk GetObject (5MiB payload > 4MiB chunk size)
	multiKey := "multi-chunk.dat"
	multiPayload := make([]byte, 5*1024*1024)
	_, _ = rand.Read(multiPayload)
	multiLen := int64(len(multiPayload))
	multiH := sha256.Sum256(multiPayload)
	multiETag := fmt.Sprintf("%q", hex.EncodeToString(multiH[:]))

	_, err = be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &validBucket,
		Key:           &multiKey,
		Body:          bytes.NewReader(multiPayload),
		ContentLength: &multiLen,
	})
	if err != nil {
		t.Fatalf("PutObject for multi-chunk payload failed: %v", err)
	}

	multiOut, err := be.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &validBucket,
		Key:    &multiKey,
	})
	if err != nil {
		t.Fatalf("multi-chunk GetObject failed: %v", err)
	}
	defer multiOut.Body.Close()

	if multiOut.ETag == nil || *multiOut.ETag != multiETag {
		t.Fatalf("multi-chunk etag mismatch: expected %s, got %v", multiETag, multiOut.ETag)
	}
	if multiOut.ContentLength == nil || *multiOut.ContentLength != multiLen {
		t.Fatalf("multi-chunk length mismatch: expected %d, got %v", multiLen, multiOut.ContentLength)
	}

	readMulti, err := io.ReadAll(multiOut.Body)
	if err != nil {
		t.Fatalf("failed to read multi-chunk body: %v", err)
	}
	if !bytes.Equal(readMulti, multiPayload) {
		t.Fatal("multi-chunk body does not match uploaded payload")
	}
}

func TestCastorBackend_CreateBucket(t *testing.T) {
	nodeAddr, cleanupNode := startTestDataNode(t, "data-cb")
	defer cleanupNode()

	metaAddr, metaClient, metaCleanup := startTestMetadataNode(t, "meta-cb")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          1,
		MaxConcurrentUploads: 2,
		DataNodes:            []string{nodeAddr},
		MetadataAddr:         metaAddr,
	}

	engine := storage.New(cfg, metaClient)
	defer engine.Close()

	be := New(engine, metaClient)
	ctx := context.Background()

	// 1. Invalid bucket name (nil, empty)
	if err := be.CreateBucket(ctx, nil, nil); !errors.Is(err, s3err.GetAPIError(s3err.ErrInvalidBucketName)) {
		t.Fatalf("expected ErrInvalidBucketName on nil req, got %v", err)
	}

	emptyBucket := ""
	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &emptyBucket}, nil); !errors.Is(err, s3err.GetAPIError(s3err.ErrInvalidBucketName)) {
		t.Fatalf("expected ErrInvalidBucketName on empty bucket, got %v", err)
	}

	// 2. Successful bucket creation with ACL and tags
	bucketName := "my-new-bucket"
	tagKey := "project"
	tagVal := "castor"
	aclJSON := []byte(`{"Owner":"admin","Grants":[]}`)
	createReq := &s3.CreateBucketInput{
		Bucket: &bucketName,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			Tags: []types.Tag{
				{Key: &tagKey, Value: &tagVal},
			},
		},
	}
	if err := be.CreateBucket(ctx, createReq, aclJSON); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 3. HeadBucket confirms bucket exists
	if _, err := be.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucketName}); err != nil {
		t.Fatalf("HeadBucket failed after CreateBucket: %v", err)
	}

	// 4. Duplicate bucket creation returns ErrBucketAlreadyOwnedByYou
	dupErr := be.CreateBucket(ctx, createReq, aclJSON)
	expectedDupErr := s3err.GetBucketErr(s3err.ErrBucketAlreadyOwnedByYou, bucketName)
	if !errors.Is(dupErr, expectedDupErr) {
		t.Fatalf("expected ErrBucketAlreadyOwnedByYou, got %v", dupErr)
	}
}

func TestCastorBackend_ListBuckets(t *testing.T) {
	nodeAddr, cleanupNode := startTestDataNode(t, "data-list-b")
	defer cleanupNode()

	metaAddr, metaClient, metaCleanup := startTestMetadataNode(t, "meta-list-b")
	defer metaCleanup()

	cfg := config.Config{
		ChunkSize:            4096,
		WriteQuorum:          1,
		MaxConcurrentUploads: 2,
		DataNodes:            []string{nodeAddr},
		MetadataAddr:         metaAddr,
	}

	engine := storage.New(cfg, metaClient)
	defer engine.Close()

	be := New(engine, metaClient)
	ctx := context.Background()

	// 1. Initial listing is empty
	res, err := be.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true})
	if err != nil {
		t.Fatalf("ListBuckets on empty cluster failed: %v", err)
	}
	if len(res.Buckets.Bucket) != 0 {
		t.Fatalf("expected 0 buckets, got %d", len(res.Buckets.Bucket))
	}

	// 2. Create multiple buckets
	b1 := "prod-alpha"
	b2 := "prod-beta"
	b3 := "staging-gamma"
	aclA := []byte(`{"Owner":"user-a","Grants":[]}`)
	aclB := []byte(`{"Owner":"user-b","Grants":[]}`)

	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &b1}, aclA); err != nil {
		t.Fatalf("failed to create b1: %v", err)
	}
	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &b2}, aclB); err != nil {
		t.Fatalf("failed to create b2: %v", err)
	}
	if err := be.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &b3}, aclA); err != nil {
		t.Fatalf("failed to create b3: %v", err)
	}

	// 3. Admin listing returns all buckets
	adminRes, err := be.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true})
	if err != nil {
		t.Fatalf("ListBuckets admin failed: %v", err)
	}
	if len(adminRes.Buckets.Bucket) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(adminRes.Buckets.Bucket))
	}

	// 4. Prefix filter returns only matching buckets
	prefixRes, err := be.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true, Prefix: "prod-"})
	if err != nil {
		t.Fatalf("ListBuckets prefix failed: %v", err)
	}
	if len(prefixRes.Buckets.Bucket) != 2 {
		t.Fatalf("expected 2 prod- buckets, got %d", len(prefixRes.Buckets.Bucket))
	}

	// 5. User owner filter returns only user's buckets
	userRes, err := be.ListBuckets(ctx, s3response.ListBucketsInput{Owner: "user-b"})
	if err != nil {
		t.Fatalf("ListBuckets user failed: %v", err)
	}
	if len(userRes.Buckets.Bucket) != 1 || userRes.Buckets.Bucket[0].Name != b2 {
		t.Fatalf("expected 1 bucket %s for user-b, got %v", b2, userRes.Buckets.Bucket)
	}

	// 6. MaxBuckets pagination limit
	pageRes, err := be.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true, MaxBuckets: 2})
	if err != nil {
		t.Fatalf("ListBuckets pagination failed: %v", err)
	}
	if len(pageRes.Buckets.Bucket) != 2 {
		t.Fatalf("expected 2 buckets with MaxBuckets=2, got %d", len(pageRes.Buckets.Bucket))
	}
	if pageRes.ContinuationToken == "" {
		t.Fatal("expected non-empty ContinuationToken when truncated")
	}
}



