package integration

import (
	"bytes"
	"context"
	"crypto/rand"
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

func TestIntegration_GatewayListObjectsV2(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-list-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-list-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-list-3")
	defer cleanup3()

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-lead-list")
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
		ChunkSize:            int64(MaxChunkSize), // 4MiB system chunk size
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
			if resp.StatusCode == http.StatusOK {
				s3Ready = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !s3Ready {
		t.Fatal("timed out waiting for embedded VersityGW S3 port to start serving")
	}

	ctx := context.Background()
	awsCfg, err := s3cfg.LoadDefaultConfig(ctx,
		s3cfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.RootAccessKey, cfg.RootSecretKey, "")),
		s3cfg.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("failed to load AWS SDK config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", s3Addr))
		o.UsePathStyle = true
	})

	bucketName := "test-list-v2-bucket"

	// 1. Missing bucket test
	_, err = s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("nonexistent-bucket-xyz"),
	})
	if err == nil {
		t.Fatal("expected error listing non-existent bucket")
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() != "NoSuchBucket" {
			t.Fatalf("expected NoSuchBucket error, got %s: %s", apiErr.ErrorCode(), apiErr.ErrorMessage())
		}
	}

	// 2. Create test bucket
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 3. Upload test files
	// photos/vacation.jpg payload is 5MiB (> 4MiB system chunk size) to satisfy chunk size invariant
	multiChunkPayload := make([]byte, 5*1024*1024)
	_, _ = io.ReadFull(rand.Reader, multiChunkPayload)

	files := map[string][]byte{
		"file_root.txt":        []byte("root level file content"),
		"photos/vacation.jpg":  multiChunkPayload,
		"photos/2026/img1.png": []byte("png 1 content"),
		"photos/2026/img2.png": []byte("png 2 content"),
		"docs/spec.pdf":        []byte("pdf content"),
	}

	for k, v := range files {
		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(k),
			Body:   bytes.NewReader(v),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", k, err)
		}
	}

	// 4. Flat listing (no delimiter, no prefix)
	flatOut, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("flat ListObjectsV2 failed: %v", err)
	}
	if len(flatOut.Contents) != 5 {
		t.Fatalf("expected 5 objects in flat listing, got %d", len(flatOut.Contents))
	}
	if len(flatOut.CommonPrefixes) != 0 {
		t.Fatalf("expected 0 common prefixes in flat listing, got %d", len(flatOut.CommonPrefixes))
	}

	for _, item := range flatOut.Contents {
		if *item.Key == "photos/vacation.jpg" {
			if *item.Size != int64(len(multiChunkPayload)) {
				t.Fatalf("expected multi-chunk size %d, got %d", len(multiChunkPayload), *item.Size)
			}
		}
	}

	// 5. Delimiter listing at root ("/")
	delimOut, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		t.Fatalf("delimiter ListObjectsV2 failed: %v", err)
	}
	if len(delimOut.Contents) != 1 || *delimOut.Contents[0].Key != "file_root.txt" {
		t.Fatalf("expected 1 root content file_root.txt, got %v", delimOut.Contents)
	}
	if len(delimOut.CommonPrefixes) != 2 {
		t.Fatalf("expected 2 common prefixes (docs/, photos/), got %d", len(delimOut.CommonPrefixes))
	}
	cpMap := make(map[string]bool)
	for _, cp := range delimOut.CommonPrefixes {
		cpMap[*cp.Prefix] = true
	}
	if !cpMap["docs/"] || !cpMap["photos/"] {
		t.Fatalf("expected docs/ and photos/ common prefixes, got %v", cpMap)
	}

	// 6. Subfolder prefix listing ("photos/" with delimiter "/")
	subOut, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String("photos/"),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		t.Fatalf("subfolder ListObjectsV2 failed: %v", err)
	}
	if len(subOut.Contents) != 1 || *subOut.Contents[0].Key != "photos/vacation.jpg" {
		t.Fatalf("expected 1 file photos/vacation.jpg, got %v", subOut.Contents)
	}
	if len(subOut.CommonPrefixes) != 1 || *subOut.CommonPrefixes[0].Prefix != "photos/2026/" {
		t.Fatalf("expected common prefix photos/2026/, got %v", subOut.CommonPrefixes)
	}

	// 7. Pagination across pages (MaxKeys: 2)
	p1, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("page 1 ListObjectsV2 failed: %v", err)
	}
	if !*p1.IsTruncated || p1.NextContinuationToken == nil {
		t.Fatalf("expected truncated page 1 with NextContinuationToken: %+v", p1)
	}
	if len(p1.Contents) != 2 {
		t.Fatalf("expected 2 objects on page 1, got %d", len(p1.Contents))
	}

	seenKeys := make(map[string]bool)
	for _, c := range p1.Contents {
		seenKeys[*c.Key] = true
	}

	p2, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucketName),
		ContinuationToken: p1.NextContinuationToken,
		MaxKeys:           aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("page 2 ListObjectsV2 failed: %v", err)
	}
	if len(p2.Contents) != 2 {
		t.Fatalf("expected 2 objects on page 2, got %d", len(p2.Contents))
	}
	for _, c := range p2.Contents {
		if seenKeys[*c.Key] {
			t.Fatalf("duplicate key found across pages: %s", *c.Key)
		}
		seenKeys[*c.Key] = true
	}

	p3, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucketName),
		ContinuationToken: p2.NextContinuationToken,
		MaxKeys:           aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("page 3 ListObjectsV2 failed: %v", err)
	}
	if len(p3.Contents) != 1 {
		t.Fatalf("expected 1 remaining object on page 3, got %d", len(p3.Contents))
	}
	for _, c := range p3.Contents {
		if seenKeys[*c.Key] {
			t.Fatalf("duplicate key found across pages: %s", *c.Key)
		}
		seenKeys[*c.Key] = true
	}

	if len(seenKeys) != 5 {
		t.Fatalf("expected 5 distinct keys across paginated queries, got %d", len(seenKeys))
	}
}
