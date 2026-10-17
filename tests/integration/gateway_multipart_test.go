package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gofiber/fiber/v3"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	gwbackend "github.com/tharunn0/castor/internal/gateway/backend"
	gwconfig "github.com/tharunn0/castor/internal/gateway/config"
	gwstorage "github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupMultipartGateway(t *testing.T) (s3Client *s3.Client, metaClient castorv1.MetadataServiceClient, cleanup func()) {
	t.Helper()

	node1Addr, cleanup1 := startTestDataNode(t, "data-mp-1")
	node2Addr, cleanup2 := startTestDataNode(t, "data-mp-2")
	node3Addr, cleanup3 := startTestDataNode(t, "data-mp-3")

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-mp-lead")

	metaConn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cleanup3()
		cleanup2()
		cleanup1()
		metaCleanup()
		t.Fatalf("failed to dial metadata node: %v", err)
	}
	metaClient = castorv1.NewMetadataServiceClient(metaConn)

	s3Addr := getFreePort(t)

	cfg := gwconfig.Config{
		S3Addr:               s3Addr,
		MetadataAddr:         metaAddr,
		DataNodes:            []string{node1Addr, node2Addr, node3Addr},
		RootAccessKey:        "admin",
		RootSecretKey:        "admin123",
		ChunkSize:            4 << 20, // 4MiB standard system chunk size
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
	}

	engine := gwstorage.New(cfg, metaClient)
	be := gwbackend.New(engine, metaClient, newTestLogger())

	gwCtx, cancel := context.WithCancel(context.Background())
	gwCfg := &embedgw.Config{
		Ports:             []string{s3Addr},
		RootUserAccess:    cfg.RootAccessKey,
		RootUserSecret:    cfg.RootSecretKey,
		Region:            "us-east-1",
		Quiet:             true,
		MaxConnections:    65536,
		MaxRequests:       65536,
		MultipartMaxParts: 10000,
		S3Options: []s3api.Option{
			s3api.WithRoute("GET", "/health", func(c fiber.Ctx) error {
				return c.SendStatus(fiber.StatusOK)
			}),
		},
	}

	gwErrCh := make(chan error, 1)
	go func() {
		gwErrCh <- embedgw.RunVersityGW(gwCtx, be, gwCfg)
	}()

	// Wait for S3 server to be ready
	httpClient := &http.Client{Timeout: 500 * time.Millisecond}
	var s3Ready bool
	for range 60 {
		select {
		case err := <-gwErrCh:
			cleanup3()
			cleanup2()
			cleanup1()
			_ = metaConn.Close()
			metaCleanup()
			cancel()
			t.Fatalf("embedgw failed immediately: %v", err)
		default:
		}

		conn, err := net.DialTimeout("tcp", s3Addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			resp, err := httpClient.Get(fmt.Sprintf("http://%s/health", s3Addr))
			if err == nil && resp.StatusCode == http.StatusOK {
				_ = resp.Body.Close()
				s3Ready = true
				break
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !s3Ready {
		cleanup3()
		cleanup2()
		cleanup1()
		_ = metaConn.Close()
		metaCleanup()
		cancel()
		t.Fatal("s3 server failed to become ready")
	}

	awsCfg, err := s3cfg.LoadDefaultConfig(context.Background(),
		s3cfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "admin123", "")),
		s3cfg.WithRegion("us-east-1"),
	)
	if err != nil {
		cleanup3()
		cleanup2()
		cleanup1()
		_ = metaConn.Close()
		metaCleanup()
		cancel()
		t.Fatalf("failed to load AWS config: %v", err)
	}

	s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", s3Addr))
		o.UsePathStyle = true
	})

	cleanup = func() {
		cancel()
		engine.Close()
		_ = metaConn.Close()
		metaCleanup()
		cleanup3()
		cleanup2()
		cleanup1()
	}

	return s3Client, metaClient, cleanup
}

func TestIntegration_GatewayMultipartUpload_Success(t *testing.T) {
	s3Client, metaClient, cleanup := setupMultipartGateway(t)
	defer cleanup()

	ctx := context.Background()
	bucketName := "live-multipart-bucket"
	objectKey := "multi-chunk-video.mp4"

	// 1. Create bucket
	_, err := metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucketName,
		OwnerId: "admin",
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 2. Initiate Multipart Upload via AWS SDK
	createOut, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	if createOut.UploadId == nil || *createOut.UploadId == "" {
		t.Fatal("expected non-empty UploadId")
	}
	uploadID := *createOut.UploadId

	// 3. Multi-chunk invariant: System chunk size is 4MiB (4 << 20).
	// Part 1: 6MiB (spans 2 chunks: 4MiB + 2MiB)
	part1Data := make([]byte, 6*1024*1024)
	_, _ = rand.Read(part1Data)

	part1Out, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectKey),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(1),
		Body:          bytes.NewReader(part1Data),
		ContentLength: aws.Int64(int64(len(part1Data))),
	})
	if err != nil {
		t.Fatalf("UploadPart 1 (6MiB) failed: %v", err)
	}
	if part1Out.ETag == nil || *part1Out.ETag == "" {
		t.Fatal("expected non-empty ETag for part 1")
	}

	// Part 2: 5MiB (spans 2 chunks: 4MiB + 1MiB)
	part2Data := make([]byte, 5*1024*1024)
	_, _ = rand.Read(part2Data)

	part2Out, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectKey),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(2),
		Body:          bytes.NewReader(part2Data),
		ContentLength: aws.Int64(int64(len(part2Data))),
	})
	if err != nil {
		t.Fatalf("UploadPart 2 (5MiB) failed: %v", err)
	}
	if part2Out.ETag == nil || *part2Out.ETag == "" {
		t.Fatal("expected non-empty ETag for part 2")
	}

	// 4. Complete Multipart Upload
	completeOut, err := s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: part1Out.ETag},
				{PartNumber: aws.Int32(2), ETag: part2Out.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}
	if completeOut.ETag == nil || !strings.Contains(*completeOut.ETag, "-2") {
		t.Fatalf("expected composite multipart ETag containing '-2', got %v", completeOut.ETag)
	}

	totalExpectedSize := int64(len(part1Data) + len(part2Data)) // 11MiB

	// 5. HeadObject verification
	headOut, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if headOut.ContentLength == nil || *headOut.ContentLength != totalExpectedSize {
		t.Fatalf("HeadObject ContentLength mismatch: expected %d, got %v", totalExpectedSize, headOut.ContentLength)
	}

	// 6. Full GetObject download and byte verification
	getOut, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer getOut.Body.Close()

	downloaded, err := io.ReadAll(getOut.Body)
	if err != nil {
		t.Fatalf("failed reading object body: %v", err)
	}
	if int64(len(downloaded)) != totalExpectedSize {
		t.Fatalf("downloaded size mismatch: expected %d, got %d", totalExpectedSize, len(downloaded))
	}

	expectedFullData := append(part1Data, part2Data...)
	if !bytes.Equal(downloaded, expectedFullData) {
		t.Fatal("downloaded 11MiB multipart object payload does not match uploaded parts byte-for-byte")
	}

	// 7. Range GetObject crossing chunk and part boundaries
	// Chunk size is 4MiB. Part boundary is at 6MiB.
	// Request bytes 4MiB-10 to 4MiB+10 (across chunk 1 and chunk 2 in part 1)
	rangeStr := "bytes=4194294-4194314"
	rangeOut, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		t.Fatalf("Range GetObject failed: %v", err)
	}
	defer rangeOut.Body.Close()

	rangeBytes, err := io.ReadAll(rangeOut.Body)
	if err != nil {
		t.Fatalf("reading range body failed: %v", err)
	}
	expectedRange := expectedFullData[4194294 : 4194314+1]
	if !bytes.Equal(rangeBytes, expectedRange) {
		t.Fatal("range query bytes do not match expected chunk-crossing content")
	}
}

func TestIntegration_GatewayMultipartUpload_Abort(t *testing.T) {
	s3Client, metaClient, cleanup := setupMultipartGateway(t)
	defer cleanup()

	ctx := context.Background()
	bucketName := "live-abort-bucket"
	objectKey := "abort-video.mp4"

	_, err := metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucketName,
		OwnerId: "admin",
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	createOut, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	uploadID := *createOut.UploadId

	partData := make([]byte, 5*1024*1024)
	_, _ = rand.Read(partData)

	partOut, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectKey),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(1),
		Body:          bytes.NewReader(partData),
		ContentLength: aws.Int64(int64(len(partData))),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Abort Multipart Upload
	_, err = s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Completing aborted upload must fail
	_, err = s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: partOut.ETag},
			},
		},
	})
	if err == nil {
		t.Fatal("expected CompleteMultipartUpload to fail on aborted upload")
	}

	// GetObject must return NoSuchKey / 404
	_, err = s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err == nil {
		t.Fatal("expected GetObject to fail for aborted upload")
	}
}
