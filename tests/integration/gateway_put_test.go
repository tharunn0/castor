package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	gwbackend "github.com/tharunn0/castor/internal/gateway/backend"
	gwconfig "github.com/tharunn0/castor/internal/gateway/config"
	gwstorage "github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestIntegration_GatewayPutObject(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-3")
	defer cleanup3()

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-lead")
	defer metaCleanup()

	metaConn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial metadata node: %v", err)
	}
	defer metaConn.Close()
	metaClient := castorv1.NewMetadataServiceClient(metaConn)

	const testChunkSize = 2 << 20 // 2MB chunk size for testing multi-chunking
	cfg := gwconfig.Config{
		ChunkSize:            testChunkSize,
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
		DataNodes:            []string{node1Addr, node2Addr, node3Addr},
	}

	engine := gwstorage.New(cfg, metaClient)
	defer engine.Close()

	be := gwbackend.New(engine, metaClient)
	ctx := context.Background()

	bucket := "gateway-put-bucket"
	key := "large-video.mp4"

	// 1. PutObject fails when bucket doesn't exist
	payload := make([]byte, 5*1024*1024) // 5MB payload
	_, _ = rand.Read(payload)
	payloadLen := int64(len(payload))

	_, err = be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(payload),
		ContentLength: &payloadLen,
	})
	if !errors.Is(err, s3err.GetBucketErr(s3err.ErrNoSuchBucket, bucket)) {
		t.Fatalf("expected ErrNoSuchBucket, got %v", err)
	}

	// 2. Create bucket
	_, err = metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucket,
		OwnerId: "admin",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 3. PutObject succeeds
	h := sha256.Sum256(payload)
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))

	out, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(payload),
		ContentLength: &payloadLen,
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if out.ETag != expectedETag {
		t.Fatalf("expected etag %s, got %s", expectedETag, out.ETag)
	}
	if out.Size == nil || *out.Size != payloadLen {
		t.Fatalf("expected size %d, got %v", payloadLen, out.Size)
	}

	// 4. Verify manifest in metadata-svc
	manifest, err := metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if manifest.GetSize() != payloadLen {
		t.Fatalf("manifest size mismatch: expected %d, got %d", payloadLen, manifest.GetSize())
	}
	if manifest.GetEtag() != hex.EncodeToString(h[:]) {
		t.Fatalf("manifest etag mismatch: expected %s, got %s", hex.EncodeToString(h[:]), manifest.GetEtag())
	}
	// 5MB / 2MB chunks = 3 chunks (2MB + 2MB + 1MB)
	if len(manifest.GetChunks()) != 3 {
		t.Fatalf("expected 3 chunks in manifest, got %d", len(manifest.GetChunks()))
	}

	// 5. Verify chunk placement and data integrity directly on storage nodes
	for chunkIdx, chunk := range manifest.GetChunks() {
		if len(chunk.GetNodeAddresses()) < 2 {
			t.Fatalf("chunk %d has fewer than quorum replicas: %v", chunkIdx, chunk.GetNodeAddresses())
		}

		offset := int64(chunkIdx) * testChunkSize
		end := min(offset+testChunkSize, payloadLen)
		expectedChunkBytes := payload[offset:end]

		for _, nodeAddr := range chunk.GetNodeAddresses() {
			dataConn, err := grpc.NewClient(nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("failed to connect to data node %s: %v", nodeAddr, err)
			}
			dataClient := castorv1.NewDataServiceClient(dataConn)

			stream, err := dataClient.GetChunk(ctx, &castorv1.GetChunkRequest{
				ChunkHash: chunk.GetChunkHash(),
			})
			if err != nil {
				_ = dataConn.Close()
				t.Fatalf("GetChunk on %s failed: %v", nodeAddr, err)
			}

			var chunkBuf []byte
			for {
				chunkResp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					_ = dataConn.Close()
					t.Fatalf("failed receiving chunk from %s: %v", nodeAddr, err)
				}
				chunkBuf = append(chunkBuf, chunkResp.GetChunkBytes()...)
			}
			_ = dataConn.Close()

			if !bytes.Equal(chunkBuf, expectedChunkBytes) {
				t.Fatalf("chunk %d byte mismatch on node %s", chunkIdx, nodeAddr)
			}
		}
	}

	// 6. Overwrite the same key with different content
	overwritePayload := []byte("overwritten-content")
	overwriteLen := int64(len(overwritePayload))
	overH := sha256.Sum256(overwritePayload)
	expectedOverETag := fmt.Sprintf("%q", hex.EncodeToString(overH[:]))

	outOver, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(overwritePayload),
		ContentLength: &overwriteLen,
	})
	if err != nil {
		t.Fatalf("overwrite PutObject failed: %v", err)
	}
	if outOver.ETag != expectedOverETag {
		t.Fatalf("overwrite etag mismatch: expected %s, got %s", expectedOverETag, outOver.ETag)
	}

	// 7. Zero-byte PutObject
	emptyKey := "empty.dat"
	emptyLen := int64(0)
	emptyH := sha256.Sum256(nil)
	expectedEmptyETag := fmt.Sprintf("%q", hex.EncodeToString(emptyH[:]))

	outEmpty, err := be.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &emptyKey,
		Body:          bytes.NewReader(nil),
		ContentLength: &emptyLen,
	})
	if err != nil {
		t.Fatalf("zero-byte PutObject failed: %v", err)
	}
	if outEmpty.ETag != expectedEmptyETag {
		t.Fatalf("zero-byte etag mismatch: expected %s, got %s", expectedEmptyETag, outEmpty.ETag)
	}
}
