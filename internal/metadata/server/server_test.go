package server

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"github.com/tharunn0/castor/internal/metadata/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

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

	fsm := consensus.NewFSM(s)
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
