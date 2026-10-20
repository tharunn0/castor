package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
)

func TestIntegration_GatewayInlineDeduplication(t *testing.T) {
	s3Client, metaClient, cleanup := setupMultipartGateway(t)
	defer cleanup()

	ctx := context.Background()
	bucket := "dedup-e2e-bucket"

	// 1. Create bucket
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 2. Generate 5MiB payload (strictly greater than 4MiB chunk size invariant)
	payloadSize := 5 * 1024 * 1024
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		t.Fatalf("failed generating random payload: %v", err)
	}
	h := sha256.Sum256(payload)
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))

	// 3. Upload Object 1
	key1 := "original.bin"
	putResp1, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key1),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(payloadSize)),
	})
	if err != nil {
		t.Fatalf("PutObject 1 failed: %v", err)
	}
	if *putResp1.ETag != expectedETag {
		t.Fatalf("putResp1 etag mismatch: expected %s, got %s", expectedETag, *putResp1.ETag)
	}

	// 4. Upload Object 2 with identical payload (should dedup against Object 1)
	key2 := "duplicate.bin"
	putResp2, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key2),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(payloadSize)),
	})
	if err != nil {
		t.Fatalf("PutObject 2 (dedup) failed: %v", err)
	}
	if *putResp2.ETag != expectedETag {
		t.Fatalf("putResp2 etag mismatch: expected %s, got %s", expectedETag, *putResp2.ETag)
	}

	// 5. Verify manifests in metadata service point to the same chunk hashes
	m1, err := metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: bucket,
		Key:    key1,
	})
	if err != nil {
		t.Fatalf("GetManifest key1 failed: %v", err)
	}
	m2, err := metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: bucket,
		Key:    key2,
	})
	if err != nil {
		t.Fatalf("GetManifest key2 failed: %v", err)
	}
	if len(m1.GetChunks()) != 2 || len(m2.GetChunks()) != 2 {
		t.Fatalf("expected 2 chunks per manifest, got %d and %d", len(m1.GetChunks()), len(m2.GetChunks()))
	}
	for i := range m1.GetChunks() {
		if m1.GetChunks()[i].GetChunkHash() != m2.GetChunks()[i].GetChunkHash() {
			t.Fatalf("chunk %d hash mismatch between objects", i)
		}
	}

	// 6. Verify CheckChunks confirms all hashes exist in the cluster
	checkResp, err := metaClient.CheckChunks(ctx, &castorv1.CheckChunksRequest{
		ChunkHashes: []string{m1.GetChunks()[0].GetChunkHash(), m1.GetChunks()[1].GetChunkHash()},
	})
	if err != nil {
		t.Fatalf("CheckChunks failed: %v", err)
	}
	for _, chunk := range m1.GetChunks() {
		if !checkResp.GetExistingChunks()[chunk.GetChunkHash()] {
			t.Fatalf("expected chunk hash %s to exist in cluster", chunk.GetChunkHash())
		}
	}

	// 7. Verify Object 2 is fully readable
	getResp2, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key2),
	})
	if err != nil {
		t.Fatalf("GetObject key2 failed: %v", err)
	}
	data2, err := io.ReadAll(getResp2.Body)
	_ = getResp2.Body.Close()
	if err != nil {
		t.Fatalf("reading key2 body failed: %v", err)
	}
	if !bytes.Equal(data2, payload) {
		t.Fatal("key2 body mismatch with original payload")
	}

	// 8. Delete Object 1; Object 2 must remain fully intact and readable
	_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key1),
	})
	if err != nil {
		t.Fatalf("DeleteObject key1 failed: %v", err)
	}

	getResp2After, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key2),
	})
	if err != nil {
		t.Fatalf("GetObject key2 after deleting key1 failed: %v", err)
	}
	data2After, err := io.ReadAll(getResp2After.Body)
	_ = getResp2After.Body.Close()
	if err != nil {
		t.Fatalf("reading key2 body after deleting key1 failed: %v", err)
	}
	if !bytes.Equal(data2After, payload) {
		t.Fatal("key2 body mismatch after deleting key1")
	}

	// 9. Multipart Upload Deduplication test
	mpKey := "multipart-dedup.bin"
	createMpResp, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(mpKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	uploadID := createMpResp.UploadId

	// Upload Part 1 (5MiB)
	partResp1, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(mpKey),
		UploadId:      uploadID,
		PartNumber:    aws.Int32(1),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(payloadSize)),
	})
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	// Upload Part 2 (identical 5MiB payload, deduplicated)
	partResp2, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(mpKey),
		UploadId:      uploadID,
		PartNumber:    aws.Int32(2),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(payloadSize)),
	})
	if err != nil {
		t.Fatalf("UploadPart 2 (dedup) failed: %v", err)
	}

	// Complete multipart upload
	_, err = s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(mpKey),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: partResp1.ETag},
				{PartNumber: aws.Int32(2), ETag: partResp2.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Download full 10MiB multipart object
	mpGetResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(mpKey),
	})
	if err != nil {
		t.Fatalf("GetObject for multipart object failed: %v", err)
	}
	mpData, err := io.ReadAll(mpGetResp.Body)
	_ = mpGetResp.Body.Close()
	if err != nil {
		t.Fatalf("reading multipart body failed: %v", err)
	}
	expectedMpData := append(payload, payload...)
	if !bytes.Equal(mpData, expectedMpData) {
		t.Fatal("downloaded multipart payload mismatch with expected payload")
	}
}
