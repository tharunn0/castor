package integration

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	gwbackend "github.com/tharunn0/castor/internal/gateway/backend"
	gwconfig "github.com/tharunn0/castor/internal/gateway/config"
	gwstorage "github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/versity/versitygw/embedgw"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestIntegration_GatewayCreateBucket(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-cb-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-cb-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-cb-3")
	defer cleanup3()

	log.Println("3 data nodes initialized")

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-lead-cb")
	defer metaCleanup()

	log.Println("metadata cluster initialized")

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
		ChunkSize:            int64(MaxChunkSize),
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
	}

	engine := gwstorage.New(cfg, metaClient)
	defer engine.Close()

	be := gwbackend.New(engine, metaClient)

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

		resp, err := client.Get(fmt.Sprintf("http://%s", s3Addr))
		if err == nil {
			_ = resp.Body.Close()
			s3Ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !s3Ready {
		t.Fatal("s3 server failed to become ready")
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

	bucketName := "integration-test-bucket"

	// 1. CreateBucket over S3 REST
	createOut, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed over S3 REST: %v", err)
	}
	if createOut.Location != nil && *createOut.Location != "/"+bucketName {
		t.Fatalf("unexpected Location header: %s", *createOut.Location)
	}

	// 2. HeadBucket over S3 REST confirms existence
	_, err = s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("HeadBucket failed after CreateBucket: %v", err)
	}

	// 3. Duplicate CreateBucket returns BucketAlreadyOwnedByYou
	_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		t.Fatal("expected duplicate CreateBucket to fail, but it succeeded")
	}
	if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		var bae *types.BucketAlreadyOwnedByYou
		if !strings.Contains(err.Error(), "409") && !strings.Contains(err.Error(), fmt.Sprintf("%T", bae)) {
			t.Fatalf("expected BucketAlreadyOwnedByYou or 409 Conflict, got: %v", err)
		}
	}

	// 4. ListBuckets over S3 REST confirms bucket appears in listing
	listOut, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets failed over S3 REST: %v", err)
	}
	var foundBucket bool
	for _, b := range listOut.Buckets {
		if b.Name != nil && *b.Name == bucketName {
			foundBucket = true
			break
		}
	}
	if !foundBucket {
		t.Fatalf("expected bucket %s in ListBuckets output, got: %v", bucketName, listOut.Buckets)
	}

	// 5. Attempt DeleteBucket on non-empty bucket fails with BucketNotEmpty
	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String("sample.txt"),
		Body:          bytes.NewReader([]byte("test-data")),
		ContentLength: aws.Int64(9),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		t.Fatal("expected DeleteBucket on non-empty bucket to fail, but it succeeded")
	}
	if !strings.Contains(err.Error(), "BucketNotEmpty") && !strings.Contains(err.Error(), "409") {
		t.Fatalf("expected BucketNotEmpty or 409 Conflict, got: %v", err)
	}

	// 6. DeleteBucket on empty bucket succeeds
	emptyBucketName := "empty-test-bucket"
	_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(emptyBucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket for empty bucket failed: %v", err)
	}

	_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(emptyBucketName),
	})
	if err != nil {
		t.Fatalf("DeleteBucket on empty bucket failed: %v", err)
	}

	// 7. HeadBucket confirms bucket is gone
	_, err = s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(emptyBucketName),
	})
	if err == nil {
		t.Fatal("expected HeadBucket on deleted bucket to fail, but it succeeded")
	}
}


