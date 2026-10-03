package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func TestIntegration_GatewayGetObject(t *testing.T) {
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
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			s3Ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !s3Ready {
		select {
		case err := <-gwErrCh:
			t.Fatalf("embedgw exited with error: %v", err)
		default:
			t.Fatal("s3 server failed to become ready")
		}
	}

	bucketName := "live-get-bucket"
	_, err = metaClient.CreateBucket(context.Background(), &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucketName,
		OwnerId: "admin",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	awsCfg, err := s3cfg.LoadDefaultConfig(context.Background(),
		s3cfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "admin123", "")),
		s3cfg.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", s3Addr))
		o.UsePathStyle = true
	})

	// 1. Upload multi-chunk payload (6MiB > 4MiB chunk size invariant)
	objectKey := "sample-get-object.bin"
	payload := make([]byte, 6*1024*1024)
	_, _ = rand.Read(payload)
	h := sha256.Sum256(payload)
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))

	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	if err != nil {
		t.Fatalf("S3 PutObject failed over HTTP: %v", err)
	}

	// 2. Perform S3 GetObject over HTTP
	getOut, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("S3 GetObject failed over HTTP: %v", err)
	}
	defer getOut.Body.Close()

	if getOut.ETag == nil || *getOut.ETag != expectedETag {
		t.Fatalf("etag mismatch: expected %s, got %v", expectedETag, getOut.ETag)
	}
	if getOut.ContentLength == nil || *getOut.ContentLength != int64(len(payload)) {
		t.Fatalf("content length mismatch: expected %d, got %v", len(payload), getOut.ContentLength)
	}

	downloaded, err := io.ReadAll(getOut.Body)
	if err != nil {
		t.Fatalf("failed reading downloaded body: %v", err)
	}
	if !bytes.Equal(downloaded, payload) {
		t.Fatal("downloaded bytes do not match uploaded payload")
	}

	// 3. Perform S3 GetObject with Range header across chunk boundary
	rangeStart := int64((4 << 20) - 50)
	rangeEnd := int64((4 << 20) + 49) // 100 bytes total
	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)
	expectedRangeBytes := payload[rangeStart : rangeEnd+1]

	rangeOut, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		t.Fatalf("S3 GetObject with Range failed over HTTP: %v", err)
	}
	defer rangeOut.Body.Close()

	if rangeOut.ContentLength == nil || *rangeOut.ContentLength != 100 {
		t.Fatalf("range content length mismatch: expected 100, got %v", rangeOut.ContentLength)
	}
	expectedContentRange := fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, len(payload))
	if rangeOut.ContentRange == nil || *rangeOut.ContentRange != expectedContentRange {
		t.Fatalf("range content-range mismatch: expected %s, got %v", expectedContentRange, rangeOut.ContentRange)
	}

	rangeDownloaded, err := io.ReadAll(rangeOut.Body)
	if err != nil {
		t.Fatalf("failed reading range body: %v", err)
	}
	if !bytes.Equal(rangeDownloaded, expectedRangeBytes) {
		t.Fatal("range downloaded bytes mismatch across chunk boundary")
	}

	// 4. Perform S3 GetObject on non-existent key -> expect 404
	_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("non-existent-file.bin"),
	})
	if err == nil {
		t.Fatal("expected error getting non-existent object, got nil")
	}
}
