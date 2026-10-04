package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	dataconfig "github.com/tharunn0/castor/internal/data/config"
	dataserver "github.com/tharunn0/castor/internal/data/server"
	"github.com/tharunn0/castor/internal/data/storage"
	metaconfig "github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	metaserver "github.com/tharunn0/castor/internal/metadata/server"
	"github.com/tharunn0/castor/internal/metadata/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func computeSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func startTestDataNode(t *testing.T, nodeID string) (string, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	casStore, err := storage.New(tmpDir, 4<<20)
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

func startTestMetadataNode(t *testing.T, nodeID string) (string, *consensus.RaftNode, func()) {
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

	cleanup := func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
	}

	return addr, raftNode, cleanup
}

func TestIntegration_BucketOperations(t *testing.T) {
	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-bucket-node")
	defer metaCleanup()

	conn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to metadata service: %v", err)
	}
	defer conn.Close()
	client := castorv1.NewMetadataServiceClient(conn)

	ctx := context.Background()
	bucket := "my-app-data"
	ownerID := "user-alice"

	// verify bucket does not exist initially
	existsRes, err := client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{
		Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if existsRes.GetExists() {
		t.Fatalf("expected bucket '%s' to not exist initially", bucket)
	}

	// create bucket
	createRes, err := client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucket,
		OwnerId: ownerID,
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if !createRes.GetCreated() {
		t.Fatal("expected created=true in response")
	}

	// verify bucket exists after creation
	existsRes, err = client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{
		Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if !existsRes.GetExists() {
		t.Fatalf("expected bucket '%s' to exist after creation", bucket)
	}

	// verify duplicate create returns already exists
	_, err = client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucket,
		OwnerId: ownerID,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists on duplicate CreateBucket, got: %v", err)
	}

	// delete bucket
	delRes, err := client.DeleteBucket(ctx, &castorv1.DeleteBucketMetadataRequest{
		Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
	if !delRes.GetDeleted() {
		t.Fatal("expected deleted=true in response")
	}

	// verify bucket does not exist after deletion
	existsRes, err = client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{
		Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("CheckBucketExists failed: %v", err)
	}
	if existsRes.GetExists() {
		t.Fatalf("expected bucket '%s' to not exist after deletion", bucket)
	}

	// verify delete non-existent bucket returns not found
	_, err = client.DeleteBucket(ctx, &castorv1.DeleteBucketMetadataRequest{
		Bucket: bucket,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound on deleting non-existent bucket, got: %v", err)
	}
}

func TestIntegration_ChunkOperations(t *testing.T) {
	dataAddr, dataCleanup := startTestDataNode(t, "data-storage-node")
	defer dataCleanup()

	conn, err := grpc.NewClient(dataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to data node: %v", err)
	}
	defer conn.Close()
	client := castorv1.NewDataServiceClient(conn)

	ctx := context.Background()

	// verify non-existent chunk returns not found
	missingHash := "0000000000000000000000000000000000000000000000000000000000000000"
	getMissingStream, err := client.GetChunk(ctx, &castorv1.GetChunkRequest{
		ChunkHash: missingHash,
	})
	if err != nil {
		t.Fatalf("GetChunk call failed: %v", err)
	}
	_, err = getMissingStream.Recv()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for non-existent chunk, got: %v", err)
	}

	// put chunk
	chunkBytes := make([]byte, 2*1024*1024)
	if _, err := rand.Read(chunkBytes); err != nil {
		t.Fatalf("failed to generate random bytes: %v", err)
	}
	expectedHash := computeSHA256(chunkBytes)

	putStream, err := client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("failed to open PutChunk stream: %v", err)
	}

	err = putStream.Send(&castorv1.PutChunkRequest{
		Data: &castorv1.PutChunkRequest_Metadata{
			Metadata: &castorv1.PutChunkMetadata{
				ChunkHash: expectedHash,
				ChunkSize: int64(len(chunkBytes)),
			},
		},
	})
	if err != nil {
		t.Fatalf("failed sending chunk metadata header: %v", err)
	}

	frameSize := 64 * 1024
	for frameStart := 0; frameStart < len(chunkBytes); frameStart += frameSize {
		frameEnd := frameStart + frameSize
		if frameEnd > len(chunkBytes) {
			frameEnd = len(chunkBytes)
		}
		err = putStream.Send(&castorv1.PutChunkRequest{
			Data: &castorv1.PutChunkRequest_ChunkBytes{
				ChunkBytes: chunkBytes[frameStart:frameEnd],
			},
		})
		if err != nil {
			t.Fatalf("failed sending chunk frame: %v", err)
		}
	}

	putResp, err := putStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("PutChunk CloseAndRecv failed: %v", err)
	}
	if putResp.GetChunkHash() != expectedHash {
		t.Fatalf("hash mismatch in PutChunk response: expected %s, got %s", expectedHash, putResp.GetChunkHash())
	}
	if putResp.GetBytesWritten() != int64(len(chunkBytes)) {
		t.Fatalf("size mismatch in PutChunk response: expected %d, got %d", len(chunkBytes), putResp.GetBytesWritten())
	}

	// verify chunk exists and matches
	getStream, err := client.GetChunk(ctx, &castorv1.GetChunkRequest{
		ChunkHash: expectedHash,
	})
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	var readBuffer bytes.Buffer
	for {
		frame, err := getStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error reading chunk stream: %v", err)
		}
		readBuffer.Write(frame.GetChunkBytes())
	}

	if !bytes.Equal(chunkBytes, readBuffer.Bytes()) {
		t.Fatal("read chunk bytes do not match uploaded bytes")
	}
	if computeSHA256(readBuffer.Bytes()) != expectedHash {
		t.Fatal("checksum of read chunk bytes does not match expected hash")
	}
}

func TestIntegration_ObjectManifestOperations(t *testing.T) {
	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-manifest-node")
	defer metaCleanup()

	dataAddr, dataCleanup := startTestDataNode(t, "data-manifest-node")
	defer dataCleanup()

	metaConn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to metadata service: %v", err)
	}
	defer metaConn.Close()
	metaClient := castorv1.NewMetadataServiceClient(metaConn)

	dataConn, err := grpc.NewClient(dataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to data node: %v", err)
	}
	defer dataConn.Close()
	dataClient := castorv1.NewDataServiceClient(dataConn)

	ctx := context.Background()
	bucket := "documents"
	key := "reports/q3_summary.pdf"
	ownerID := "user-bob"

	_, err = metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucket,
		OwnerId: ownerID,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// verify manifest does not exist before upload
	_, err = metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: bucket,
		Key:    key,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for uncommitted manifest, got: %v", err)
	}

	// put chunk to data node
	payload := []byte("Castor high-performance content-addressed storage engine report content.")
	chunkHash := computeSHA256(payload)

	putStream, err := dataClient.PutChunk(ctx)
	if err != nil {
		t.Fatalf("failed to open PutChunk stream: %v", err)
	}
	_ = putStream.Send(&castorv1.PutChunkRequest{
		Data: &castorv1.PutChunkRequest_Metadata{
			Metadata: &castorv1.PutChunkMetadata{
				ChunkHash: chunkHash,
				ChunkSize: int64(len(payload)),
			},
		},
	})
	_ = putStream.Send(&castorv1.PutChunkRequest{
		Data: &castorv1.PutChunkRequest_ChunkBytes{
			ChunkBytes: payload,
		},
	})
	putResp, err := putStream.CloseAndRecv()
	if err != nil || putResp.GetChunkHash() != chunkHash {
		t.Fatalf("PutChunk failed: %v", err)
	}

	// commit manifest
	commitRes, err := metaClient.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:      bucket,
		Key:         key,
		Size:        int64(len(payload)),
		Etag:        chunkHash,
		ContentType: "application/pdf",
		ChunkIds:    []string{chunkHash},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{
				ChunkHash:     chunkHash,
				NodeAddresses: []string{dataAddr},
				Size:          int64(len(payload)),
			},
		},
		OwnerId: ownerID,
	})
	if err != nil || !commitRes.GetCommitted() {
		t.Fatalf("CommitManifest failed: %v", err)
	}

	// verify manifest exists
	manifestRes, err := metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if manifestRes.GetSize() != int64(len(payload)) {
		t.Fatalf("size mismatch: expected %d, got %d", len(payload), manifestRes.GetSize())
	}
	if manifestRes.GetEtag() != chunkHash {
		t.Fatalf("ETag mismatch: expected %s, got %s", chunkHash, manifestRes.GetEtag())
	}
	if len(manifestRes.GetChunks()) != 1 {
		t.Fatalf("expected 1 chunk in manifest, got %d", len(manifestRes.GetChunks()))
	}
	if manifestRes.GetChunks()[0].GetNodeAddresses()[0] != dataAddr {
		t.Fatalf("expected node address %s, got %s", dataAddr, manifestRes.GetChunks()[0].GetNodeAddresses()[0])
	}

	// verify chunk readback matches payload
	getStream, err := dataClient.GetChunk(ctx, &castorv1.GetChunkRequest{
		ChunkHash: manifestRes.GetChunks()[0].GetChunkHash(),
	})
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}
	var readPayload bytes.Buffer
	for {
		frame, err := getStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("GetChunk stream error: %v", err)
		}
		readPayload.Write(frame.GetChunkBytes())
	}
	if !bytes.Equal(payload, readPayload.Bytes()) {
		t.Fatal("read bytes do not match committed object payload")
	}

	// verify bucket deletion fails with objects present
	_, err = metaClient.DeleteBucket(ctx, &castorv1.DeleteBucketMetadataRequest{
		Bucket: bucket,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition when deleting non-empty bucket, got: %v", err)
	}

	// delete manifest
	delRes, err := metaClient.DeleteManifest(ctx, &castorv1.DeleteManifestRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil || !delRes.GetDeleted() {
		t.Fatalf("DeleteManifest failed: %v", err)
	}

	// verify manifest does not exist after delete
	_, err = metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: bucket,
		Key:    key,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound after DeleteManifest, got: %v", err)
	}

	// verify delete non-existent manifest returns not found
	_, err = metaClient.DeleteManifest(ctx, &castorv1.DeleteManifestRequest{
		Bucket: bucket,
		Key:    key,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound on deleting non-existent manifest, got: %v", err)
	}
}
