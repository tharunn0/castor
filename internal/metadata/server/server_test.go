package server

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"github.com/tharunn0/castor/internal/metadata/store"
	"github.com/tharunn0/castor/internal/metadata/telemetry"
	telemetryPkg "github.com/tharunn0/castor/internal/telemetry"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

type directApplier struct {
	fsm *consensus.FSM
}

func (d *directApplier) Apply(cmd *consensus.Command, timeout time.Duration) (any, error) {
	data, err := cmd.Encode()
	if err != nil {
		return nil, err
	}
	res := d.fsm.Apply(&raft.Log{Data: data})
	if err, ok := res.(error); ok && err != nil {
		return nil, err
	}
	return res, nil
}

func (d *directApplier) IsLeader() bool {
	return true
}

func (d *directApplier) LeaderAddr() string {
	return "127.0.0.1:9081"
}

func setupTestServer(t *testing.T) (castorv1.MetadataServiceClient, *store.Store, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	s, err := store.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	fsm := consensus.NewFSM(s, newTestLogger())
	applier := &directApplier{fsm: fsm}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	metaServer := New(s, applier)
	castorv1.RegisterMetadataServiceServer(grpcServer, metaServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := castorv1.NewMetadataServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
		_ = s.Close()
	}

	return client, s, cleanup
}

func TestServer_BucketOperations(t *testing.T) {
	client, _, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. CreateBucket validation failure
	_, err := client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty bucket name, got: %v", err)
	}

	// 2. CreateBucket success
	createRes, err := client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  "media-bucket",
		OwnerId: "user-123",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	if !createRes.GetCreated() {
		t.Fatal("expected created=true in response")
	}

	// 3. CreateBucket duplicate -> AlreadyExists
	_, err = client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  "media-bucket",
		OwnerId: "user-123",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists on duplicate bucket, got: %v", err)
	}

	// 4. CheckBucketExists
	existsRes, err := client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{
		Bucket: "media-bucket",
	})
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if !existsRes.GetExists() {
		t.Fatal("expected exists=true for created bucket")
	}

	existsRes, err = client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{
		Bucket: "non-existent",
	})
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if existsRes.GetExists() {
		t.Fatal("expected exists=false for non-existent bucket")
	}

	// 5. ListBuckets
	listRes, err := client.ListBuckets(ctx, &castorv1.ListBucketsMetadataRequest{})
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(listRes.GetBuckets()) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(listRes.GetBuckets()))
	}
	if listRes.GetBuckets()[0].GetName() != "media-bucket" {
		t.Fatalf("unexpected bucket name: %s", listRes.GetBuckets()[0].GetName())
	}

	// 6. DeleteBucket success
	delRes, err := client.DeleteBucket(ctx, &castorv1.DeleteBucketMetadataRequest{
		Bucket: "media-bucket",
	})
	if err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
	if !delRes.GetDeleted() {
		t.Fatal("expected deleted=true in response")
	}

	// 7. Delete non-existent bucket -> NotFound
	_, err = client.DeleteBucket(ctx, &castorv1.DeleteBucketMetadataRequest{
		Bucket: "media-bucket",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound on deleting deleted bucket, got: %v", err)
	}
}

func TestServer_ObjectOperations(t *testing.T) {
	client, _, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create parent bucket
	_, err := client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  "assets",
		OwnerId: "user-456",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 1. CommitManifest validation failure
	_, err = client.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket: "",
		Key:    "image.png",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty bucket, got: %v", err)
	}

	// 2. CommitManifest success
	chunkHash := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	commitRes, err := client.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:      "assets",
		Key:         "photos/vacation.jpg",
		Size:        4096,
		Etag:        "etag-vacation-123",
		ContentType: "image/jpeg",
		ChunkIds:    []string{chunkHash},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{
				ChunkHash:     chunkHash,
				NodeAddresses: []string{"10.0.0.1:9101", "10.0.0.2:9102"},
				Size:          4096,
			},
		},
		OwnerId: "user-456",
	})
	if err != nil {
		t.Fatalf("CommitManifest failed: %v", err)
	}
	if !commitRes.GetCommitted() {
		t.Fatal("expected committed=true")
	}

	// 3. GetManifest success with locations
	getRes, err := client.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: "assets",
		Key:    "photos/vacation.jpg",
	})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if getRes.GetSize() != 4096 || getRes.GetEtag() != "etag-vacation-123" {
		t.Fatalf("unexpected manifest details: %+v", getRes)
	}
	if len(getRes.GetChunks()) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(getRes.GetChunks()))
	}
	if len(getRes.GetChunks()[0].GetNodeAddresses()) != 2 {
		t.Fatalf("expected 2 replica nodes, got %d", len(getRes.GetChunks()[0].GetNodeAddresses()))
	}

	// 4. GetManifest non-existent -> NotFound
	_, err = client.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: "assets",
		Key:    "missing.txt",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got: %v", err)
	}

	// 5. DeleteBucket on non-empty bucket -> FailedPrecondition
	_, err = client.DeleteBucket(ctx, &castorv1.DeleteBucketMetadataRequest{
		Bucket: "assets",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition when deleting non-empty bucket, got: %v", err)
	}

	// 6. ListManifests
	listRes, err := client.ListManifests(ctx, &castorv1.ListManifestsRequest{
		Bucket:    "assets",
		Prefix:    "photos/",
		Delimiter: "/",
	})
	if err != nil {
		t.Fatalf("ListManifests failed: %v", err)
	}
	if len(listRes.GetManifests()) != 1 {
		t.Fatalf("expected 1 manifest in list, got %d", len(listRes.GetManifests()))
	}

	// 7. DeleteManifest success
	delRes, err := client.DeleteManifest(ctx, &castorv1.DeleteManifestRequest{
		Bucket: "assets",
		Key:    "photos/vacation.jpg",
	})
	if err != nil {
		t.Fatalf("DeleteManifest failed: %v", err)
	}
	if !delRes.GetDeleted() {
		t.Fatal("expected deleted=true")
	}

	// 8. GetManifest after delete -> NotFound
	_, err = client.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: "assets",
		Key:    "photos/vacation.jpg",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound after deletion, got: %v", err)
	}
}

func TestServer_CheckChunks(t *testing.T) {
	client, _, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Empty chunk hashes list returns empty map
	emptyRes, err := client.CheckChunks(ctx, &castorv1.CheckChunksRequest{
		ChunkHashes: []string{},
	})
	if err != nil {
		t.Fatalf("CheckChunks with empty hashes failed: %v", err)
	}
	if len(emptyRes.GetExistingChunks()) != 0 {
		t.Fatalf("expected empty map, got: %v", emptyRes.GetExistingChunks())
	}

	// 2. Query non-existent hash returns false
	nonExistentRes, err := client.CheckChunks(ctx, &castorv1.CheckChunksRequest{
		ChunkHashes: []string{"hash-not-present"},
	})
	if err != nil {
		t.Fatalf("CheckChunks with non-existent hash failed: %v", err)
	}
	if nonExistentRes.GetExistingChunks()["hash-not-present"] {
		t.Fatalf("expected false for non-existent hash, got true")
	}

	// 3. Create bucket and commit object with chunks
	_, err = client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  "dedup-bucket",
		OwnerId: "user-123",
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	hashA := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	hashB := "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"

	_, err = client.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:      "dedup-bucket",
		Key:         "file.bin",
		Size:        8192,
		Etag:        "test-etag",
		ContentType: "application/octet-stream",
		ChunkIds:    []string{hashA, hashB},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: hashA, NodeAddresses: []string{"node1:9101"}, Size: 4096},
			{ChunkHash: hashB, NodeAddresses: []string{"node1:9101"}, Size: 4096},
		},
		OwnerId: "user-123",
	})
	if err != nil {
		t.Fatalf("CommitManifest failed: %v", err)
	}

	// 4. Query mixture of existing and non-existing chunk hashes
	checkRes, err := client.CheckChunks(ctx, &castorv1.CheckChunksRequest{
		ChunkHashes: []string{hashA, hashB, "hash-missing"},
	})
	if err != nil {
		t.Fatalf("CheckChunks query failed: %v", err)
	}
	existing := checkRes.GetExistingChunks()
	if !existing[hashA] {
		t.Errorf("expected hashA to exist, got false")
	}
	if !existing[hashB] {
		t.Errorf("expected hashB to exist, got false")
	}
	if existing["hash-missing"] {
		t.Errorf("expected hash-missing to be false, got true")
	}
}

func TestServer_NodeDiscoveryOperations(t *testing.T) {
	client, _, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Register node validation
	_, err := client.RegisterNode(ctx, &castorv1.RegisterNodeRequest{
		NodeId: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty node ID, got: %v", err)
	}

	// 2. Successful registration
	regRes, err := client.RegisterNode(ctx, &castorv1.RegisterNodeRequest{
		NodeId:      "data-node-1",
		GrpcAddress: "10.0.0.1:9101",
		TotalBytes:  100000,
		FreeBytes:   80000,
	})
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}
	if !regRes.GetRegistered() {
		t.Fatal("expected registered=true")
	}

	// 3. Heartbeat from second node (auto-register)
	hbRes, err := client.Heartbeat(ctx, &castorv1.HeartbeatRequest{
		NodeId:      "data-node-2",
		GrpcAddress: "10.0.0.2:9102",
		TotalBytes:  200000,
		FreeBytes:   150000,
	})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if !hbRes.GetAcknowledged() {
		t.Fatal("expected acknowledged=true")
	}

	// 4. GetActiveNodes
	nodesRes, err := client.GetActiveNodes(ctx, &castorv1.GetActiveNodesRequest{})
	if err != nil {
		t.Fatalf("GetActiveNodes failed: %v", err)
	}
	if len(nodesRes.GetNodes()) != 2 {
		t.Fatalf("expected 2 active nodes, got %d", len(nodesRes.GetNodes()))
	}
}

type mockFollowerApplier struct {
	leaderAddr string
}

func (m *mockFollowerApplier) Apply(_ *consensus.Command, _ time.Duration) (any, error) {
	return nil, &consensus.NotLeaderError{LeaderAddr: m.leaderAddr}
}

func (m *mockFollowerApplier) IsLeader() bool {
	return false
}

func (m *mockFollowerApplier) LeaderAddr() string {
	return m.leaderAddr
}

func TestServer_RegistryLeaderEnforcement(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := store.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	follower := &mockFollowerApplier{leaderAddr: "10.0.0.2:9093"}
	srv := New(s, follower, WithLogger(newTestLogger()))

	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	castorv1.RegisterMetadataServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := castorv1.NewMetadataServiceClient(conn)
	ctx := context.Background()

	// 1. RegisterNode on non-leader must fail with Unavailable and leader hint
	_, err = client.RegisterNode(ctx, &castorv1.RegisterNodeRequest{
		NodeId:      "data-node-1",
		GrpcAddress: "10.0.0.1:9101",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on RegisterNode from follower, got: %v", err)
	}
	if !strings.Contains(err.Error(), "leader is 10.0.0.2:9093") {
		t.Fatalf("expected leader address in error, got: %v", err)
	}

	// 2. Heartbeat on non-leader must fail with Unavailable and leader hint
	_, err = client.Heartbeat(ctx, &castorv1.HeartbeatRequest{
		NodeId:      "data-node-1",
		GrpcAddress: "10.0.0.1:9101",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on Heartbeat from follower, got: %v", err)
	}
	if !strings.Contains(err.Error(), "leader is 10.0.0.2:9093") {
		t.Fatalf("expected leader address in error, got: %v", err)
	}

	// 3. GetActiveNodes on non-leader must fail with Unavailable and leader hint
	_, err = client.GetActiveNodes(ctx, &castorv1.GetActiveNodesRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on GetActiveNodes from follower, got: %v", err)
	}
	if !strings.Contains(err.Error(), "leader is 10.0.0.2:9093") {
		t.Fatalf("expected leader address in error, got: %v", err)
	}
}

func TestServer_MultipartOperations(t *testing.T) {
	client, _, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Validation failures for InitiateMultipart
	_, err := client.InitiateMultipart(ctx, &castorv1.InitiateMultipartMetaRequest{
		Bucket: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty bucket, got: %v", err)
	}

	_, err = client.InitiateMultipart(ctx, &castorv1.InitiateMultipartMetaRequest{
		Bucket: "b1",
		Key:    "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty key, got: %v", err)
	}

	_, err = client.InitiateMultipart(ctx, &castorv1.InitiateMultipartMetaRequest{
		Bucket: "b1",
		Key:    "k1",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty upload_id, got: %v", err)
	}

	// 2. Initiate on non-existent bucket -> NotFound
	_, err = client.InitiateMultipart(ctx, &castorv1.InitiateMultipartMetaRequest{
		Bucket:   "missing-bucket",
		Key:      "photo.jpg",
		UploadId: "upload-1",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound on missing bucket, got: %v", err)
	}

	// 3. Create bucket and initiate multipart
	_, err = client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  "media",
		OwnerId: "user-1",
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	initResp, err := client.InitiateMultipart(ctx, &castorv1.InitiateMultipartMetaRequest{
		Bucket:      "media",
		Key:         "video.mp4",
		ContentType: "video/mp4",
		UploadId:    "upload-vid-1",
	})
	if err != nil {
		t.Fatalf("InitiateMultipart failed: %v", err)
	}
	if initResp.GetUploadId() != "upload-vid-1" {
		t.Fatalf("expected upload_id upload-vid-1, got %q", initResp.GetUploadId())
	}
	if initResp.GetCreatedAt() == nil {
		t.Fatal("expected non-nil CreatedAt")
	}

	// 4. CommitPart validation
	_, err = client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty upload_id, got: %v", err)
	}

	_, err = client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   "upload-vid-1",
		PartNumber: 0,
		Etag:       "etag1",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on part_number <= 0, got: %v", err)
	}

	_, err = client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   "upload-vid-1",
		PartNumber: 1,
		Etag:       "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty etag, got: %v", err)
	}

	// CommitPart on unknown upload
	_, err = client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   "unknown-upload",
		PartNumber: 1,
		Etag:       "etag1",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound on unknown upload, got: %v", err)
	}

	// 5. Commit Part 1 and Part 2
	part1Resp, err := client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   "upload-vid-1",
		PartNumber: 1,
		Size:       5000000,
		Etag:       "part1-hash",
		ChunkIds:   []string{"chunk-vid-1"},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{
				ChunkHash:     "chunk-vid-1",
				NodeAddresses: []string{"node-1:9090", "node-2:9090"},
				Size:          5000000,
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitPart 1 failed: %v", err)
	}
	if !part1Resp.GetCommitted() || part1Resp.GetEtag() != "part1-hash" {
		t.Fatalf("unexpected part 1 response: %+v", part1Resp)
	}

	part2Resp, err := client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   "upload-vid-1",
		PartNumber: 2,
		Size:       6000000,
		Etag:       "part2-hash",
		ChunkIds:   []string{"chunk-vid-2"},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{
				ChunkHash:     "chunk-vid-2",
				NodeAddresses: []string{"node-1:9090", "node-2:9090"},
				Size:          6000000,
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitPart 2 failed: %v", err)
	}
	if !part2Resp.GetCommitted() || part2Resp.GetEtag() != "part2-hash" {
		t.Fatalf("unexpected part 2 response: %+v", part2Resp)
	}

	// 6. CompleteMultipart validation
	_, err = client.CompleteMultipart(ctx, &castorv1.CompleteMultipartMetaRequest{
		UploadId: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on empty upload_id, got: %v", err)
	}

	// Complete with mismatched parts -> InvalidArgument
	_, err = client.CompleteMultipart(ctx, &castorv1.CompleteMultipartMetaRequest{
		UploadId: "upload-vid-1",
		Parts: []*castorv1.PartSummary{
			{PartNumber: 1, Etag: "wrong-etag"},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on mismatched part etag, got: %v", err)
	}

	// Complete successfully
	completeResp, err := client.CompleteMultipart(ctx, &castorv1.CompleteMultipartMetaRequest{
		UploadId: "upload-vid-1",
		Parts: []*castorv1.PartSummary{
			{PartNumber: 1, Etag: "part1-hash"},
			{PartNumber: 2, Etag: "part2-hash"},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipart failed: %v", err)
	}
	if !completeResp.GetCommitted() {
		t.Fatal("expected committed=true")
	}

	// Verify committed manifest exists and size is 11000000
	manifest, err := client.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: "media",
		Key:    "video.mp4",
	})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if manifest.GetSize() != 11000000 {
		t.Fatalf("expected size 11000000, got %d", manifest.GetSize())
	}
	if len(manifest.GetChunks()) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(manifest.GetChunks()))
	}

	// 7. CommitPart on already completed upload -> FailedPrecondition
	_, err = client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   "upload-vid-1",
		PartNumber: 3,
		Etag:       "part3-hash",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition on completed upload, got: %v", err)
	}

	// 8. Test AbortMultipart
	initAbort, err := client.InitiateMultipart(ctx, &castorv1.InitiateMultipartMetaRequest{
		Bucket:   "media",
		Key:      "discard.tmp",
		UploadId: "upload-abort-1",
	})
	if err != nil {
		t.Fatalf("InitiateMultipart for abort test failed: %v", err)
	}

	_, err = client.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   initAbort.GetUploadId(),
		PartNumber: 1,
		Size:       1024,
		Etag:       "p1",
		ChunkIds:   []string{"c1"},
	})
	if err != nil {
		t.Fatalf("CommitPart before abort failed: %v", err)
	}

	abortResp, err := client.AbortMultipart(ctx, &castorv1.AbortMultipartMetaRequest{
		UploadId: initAbort.GetUploadId(),
	})
	if err != nil {
		t.Fatalf("AbortMultipart failed: %v", err)
	}
	if !abortResp.GetAborted() {
		t.Fatal("expected aborted=true")
	}

	// Attempt to complete aborted upload -> FailedPrecondition
	_, err = client.CompleteMultipart(ctx, &castorv1.CompleteMultipartMetaRequest{
		UploadId: initAbort.GetUploadId(),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition on aborted upload, got: %v", err)
	}
}

func TestServer_Telemetry(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	reg := prometheus.NewRegistry()
	metrics := telemetry.NewMetadataMetrics("server-test", &telemetryPkg.Metrics{Registry: reg})

	tmpDir := t.TempDir()
	s, err := store.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	fsm := consensus.NewFSM(s, newTestLogger())
	applier := &directApplier{fsm: fsm}

	srv := New(s, applier,
		WithTracer(tp.Tracer("server-telemetry-test")),
		WithMetrics(metrics),
		WithLogger(newTestLogger()),
	)

	ctx := context.Background()

	// 1. CreateBucket
	_, err = srv.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket: "telemetry-bucket",
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 2. CommitManifest
	_, err = srv.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:   "telemetry-bucket",
		Key:      "test.txt",
		Size:     1024,
		ChunkIds: []string{"chunk1", "chunk2"},
	})
	if err != nil {
		t.Fatalf("CommitManifest failed: %v", err)
	}

	// 3. InitiateMultipart
	_, err = srv.InitiateMultipart(ctx, &castorv1.InitiateMultipartMetaRequest{
		Bucket:   "telemetry-bucket",
		Key:      "large.bin",
		UploadId: "upload-tel-1",
	})
	if err != nil {
		t.Fatalf("InitiateMultipart failed: %v", err)
	}

	// 4. CommitPart
	_, err = srv.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   "upload-tel-1",
		PartNumber: 1,
		Size:       512,
		Etag:       "etag1",
		ChunkIds:   []string{"c1"},
	})
	if err != nil {
		t.Fatalf("CommitPart failed: %v", err)
	}

	// 5. CompleteMultipart
	_, err = srv.CompleteMultipart(ctx, &castorv1.CompleteMultipartMetaRequest{
		UploadId: "upload-tel-1",
		Parts: []*castorv1.PartSummary{
			{PartNumber: 1, Etag: "etag1"},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipart failed: %v", err)
	}

	// 6. Error mutation (duplicate bucket)
	_, err = srv.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket: "telemetry-bucket",
	})
	if err == nil {
		t.Fatal("expected error on duplicate bucket creation")
	}

	// 7. Read operations (must NOT emit create spans)
	beforeReadSpanCount := len(exporter.GetSpans())
	_, _ = srv.GetManifest(ctx, &castorv1.GetManifestRequest{Bucket: "telemetry-bucket", Key: "test.txt"})
	_, _ = srv.ListManifests(ctx, &castorv1.ListManifestsRequest{Bucket: "telemetry-bucket"})
	_, _ = srv.ListBuckets(ctx, &castorv1.ListBucketsMetadataRequest{})
	_, _ = srv.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: "telemetry-bucket"})
	if len(exporter.GetSpans()) != beforeReadSpanCount {
		t.Errorf("expected read operations to not create spans, got new spans: %d vs %d", len(exporter.GetSpans()), beforeReadSpanCount)
	}

	// Verify Spans
	spans := exporter.GetSpans()
	spanMap := make(map[string]tracetest.SpanStub)
	for _, span := range spans {
		spanMap[span.Name] = span
	}

	expectedSpans := []string{
		"Metadata/CreateBucket",
		"Metadata/CommitManifest",
		"Metadata/InitiateMultipart",
		"Metadata/CommitPart",
		"Metadata/CompleteMultipart",
	}

	for _, name := range expectedSpans {
		span, ok := spanMap[name]
		if !ok {
			t.Errorf("expected span %s to be recorded", name)
			continue
		}
		if span.SpanKind != trace.SpanKindServer {
			t.Errorf("expected span %s to have SpanKindServer, got %v", name, span.SpanKind)
		}
	}

	// Check specific span attributes
	if s, ok := spanMap["Metadata/CreateBucket"]; ok {
		hasBucket := false
		for _, attr := range s.Attributes {
			if attr.Key == "s3.bucket" && attr.Value.AsString() == "telemetry-bucket" {
				hasBucket = true
			}
		}
		if !hasBucket {
			t.Error("expected Metadata/CreateBucket to have s3.bucket attribute")
		}
	}

	if s, ok := spanMap["Metadata/CommitManifest"]; ok {
		var hasBucket, hasKey, hasSize, hasParts bool
		for _, attr := range s.Attributes {
			if attr.Key == "s3.bucket" && attr.Value.AsString() == "telemetry-bucket" {
				hasBucket = true
			}
			if attr.Key == "s3.key" && attr.Value.AsString() == "test.txt" {
				hasKey = true
			}
			if attr.Key == "s3.size_bytes" && attr.Value.AsInt64() == 1024 {
				hasSize = true
			}
			if attr.Key == "s3.parts" && attr.Value.AsInt64() == 2 {
				hasParts = true
			}
		}
		if !hasBucket || !hasKey || !hasSize || !hasParts {
			t.Errorf("expected CommitManifest attributes, got bucket=%v key=%v size=%v parts=%v", hasBucket, hasKey, hasSize, hasParts)
		}
	}

	// Verify Metrics
	if val := testutil.ToFloat64(metrics.CommitsTotal.WithLabelValues("create_bucket", "success")); val != 1 {
		t.Errorf("expected create_bucket success to be 1, got %v", val)
	}
	if val := testutil.ToFloat64(metrics.CommitsTotal.WithLabelValues("create_bucket", "error")); val != 1 {
		t.Errorf("expected create_bucket error to be 1, got %v", val)
	}
	if val := testutil.ToFloat64(metrics.CommitsTotal.WithLabelValues("commit_manifest", "success")); val != 1 {
		t.Errorf("expected commit_manifest success to be 1, got %v", val)
	}
	if val := testutil.ToFloat64(metrics.CommitsTotal.WithLabelValues("initiate_mp", "success")); val != 1 {
		t.Errorf("expected initiate_mp success to be 1, got %v", val)
	}
	if val := testutil.ToFloat64(metrics.CommitsTotal.WithLabelValues("commit_part", "success")); val != 1 {
		t.Errorf("expected commit_part success to be 1, got %v", val)
	}
	if val := testutil.ToFloat64(metrics.CommitsTotal.WithLabelValues("complete_mp", "success")); val != 1 {
		t.Errorf("expected complete_mp success to be 1, got %v", val)
	}
}
