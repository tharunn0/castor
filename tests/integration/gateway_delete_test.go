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
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	gwbackend "github.com/tharunn0/castor/internal/gateway/backend"
	gwconfig "github.com/tharunn0/castor/internal/gateway/config"
	gwhealth "github.com/tharunn0/castor/internal/gateway/health"
	gwstorage "github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestIntegration_GatewayDeleteObject(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-del-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-del-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-del-3")
	defer cleanup3()

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-lead-del")
	defer metaCleanup()

	metaConn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial metadata node: %v", err)
	}
	defer metaConn.Close()
	metaClient := castorv1.NewMetadataServiceClient(metaConn)

	s3Addr := getFreePort(t)
	consoleAddr := getFreePort(t)

	cfg := gwconfig.Config{
		S3Addr:               s3Addr,
		ConsoleAddr:          consoleAddr,
		MetadataAddr:         metaAddr,
		DataNodes:            []string{node1Addr, node2Addr, node3Addr},
		RootAccessKey:        "admin",
		RootSecretKey:        "admin123",
		ChunkSize:            int64(MaxChunkSize), // 4MiB chunk size
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
	}

	engine := gwstorage.New(cfg, metaClient)
	defer engine.Close()

	be := gwbackend.New(engine, metaClient)
	healthHandler := gwhealth.NewHandler(cfg)

	gwCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
			s3api.WithRoute("GET", "/health", healthHandler),
		},
	}

	gwErrCh := make(chan error, 1)
	go func() {
		gwErrCh <- embedgw.RunVersityGW(gwCtx, be, gwCfg)
	}()

	client := &http.Client{Timeout: 500 * time.Millisecond}
	var s3Ready bool
	for range 50 {
		select {
		case err := <-gwErrCh:
			t.Fatalf("embedgw failed immediately: %v", err)
		default:
		}

		resp, err := client.Get(fmt.Sprintf("http://%s/health", s3Addr))
		if err == nil {
			_ = resp.Body.Close()
			s3Ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !s3Ready {
		t.Fatal("timed out waiting for VersityGW S3 port to accept connections")
	}

	awsCfg, err := s3cfg.LoadDefaultConfig(context.Background(),
		s3cfg.WithRegion("us-east-1"),
		s3cfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.RootAccessKey, cfg.RootSecretKey, "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", s3Addr))
		o.UsePathStyle = true
	})

	ctx := context.Background()
	bucketName := "delete-e2e-bucket"

	// 1. Create bucket
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 2. Upload multi-chunk payload (5MiB > 4MiB chunk invariant)
	payloadSize := 5 * 1024 * 1024
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		t.Fatalf("failed to generate random payload: %v", err)
	}
	hash := sha256.Sum256(payload)
	expectedETag := hex.EncodeToString(hash[:])

	objKey := "multi-chunk-to-delete.bin"
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objKey),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(payloadSize)),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// 3. Verify object exists via GetObject
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	_ = getResp.Body.Close()
	if getResp.ETag != nil && *getResp.ETag != fmt.Sprintf("%q", expectedETag) {
		t.Fatalf("expected ETag %q, got %s", expectedETag, *getResp.ETag)
	}

	// 4. Delete object via S3 SDK
	delResp, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objKey),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if delResp == nil {
		t.Fatal("expected non-nil DeleteObject output")
	}

	// 5. Subsequent GetObject returns NoSuchKey
	_, err = s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objKey),
	})
	if err == nil {
		t.Fatal("expected GetObject to fail with NoSuchKey after deletion, got nil error")
	}
	var noSuchKeyErr *types.NoSuchKey
	var apiErr smithy.APIError
	if !errors.As(err, &noSuchKeyErr) && (!errors.As(err, &apiErr) || apiErr.ErrorCode() != "NoSuchKey") {
		t.Fatalf("expected NoSuchKey error, got: %v", err)
	}

	// 6. Delete already deleted object succeeds (idempotent)
	_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objKey),
	})
	if err != nil {
		t.Fatalf("expected idempotent DeleteObject on deleted key to succeed, got %v", err)
	}

	// 7. Delete object from non-existent bucket returns NoSuchBucket
	missingBucket := "completely-missing-bucket"
	_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(missingBucket),
		Key:    aws.String(objKey),
	})
	if err == nil {
		t.Fatal("expected DeleteObject on missing bucket to fail with NoSuchBucket")
	}
	var noSuchBucketErr *types.NoSuchBucket
	if !errors.As(err, &noSuchBucketErr) && (!errors.As(err, &apiErr) || apiErr.ErrorCode() != "NoSuchBucket") {
		t.Fatalf("expected NoSuchBucket error, got: %v", err)
	}

	// 8. Delete bucket now succeeds because all objects have been removed
	_, err = s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("DeleteBucket failed after object deletion: %v", err)
	}
}
