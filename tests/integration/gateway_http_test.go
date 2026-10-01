package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gofiber/fiber/v3"
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

var (
	MaxChunkSize = 4 << 20
)

func getFreePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func TestIntegration_GatewayHTTP(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-3")
	defer cleanup3()

	log.Println(" 3 nodes initialized")

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-lead")
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

	// 1. Start Fiber HTTP health endpoint
	healthApp := gwhealth.NewApp(cfg)
	healthHandler := gwhealth.NewHandler(cfg)

	go func() {
		_ = healthApp.Listen(consoleAddr, fiber.ListenConfig{
			DisableStartupMessage: true,
		})
	}()
	defer func() { _ = healthApp.Shutdown() }()

	// 2. Start VersityGW embedded S3 server
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
			s3api.WithRoute("GET", "/healthz", healthHandler),
		},
	}

	gwErrCh := make(chan error, 1)
	go func() {
		gwErrCh <- embedgw.RunVersityGW(gwCtx, be, gwCfg)
	}()

	// Wait for servers to be up
	client := &http.Client{Timeout: 500 * time.Millisecond}
	var healthReady bool
	for range 50 {
		resp, err := client.Get(fmt.Sprintf("http://%s/health", consoleAddr))
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			healthReady = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !healthReady {
		t.Fatal("health server failed to become ready")
	}

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

	// 3. Verify Fiber HTTP health endpoint response
	resp, err := client.Get(fmt.Sprintf("http://%s/health", consoleAddr))
	if err != nil {
		t.Fatalf("failed to query health endpoint: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed reading health body: %v", err)
	}

	var healthPayload map[string]any
	if err := json.Unmarshal(body, &healthPayload); err != nil {
		t.Fatalf("invalid health json: %v", err)
	}
	if healthPayload["status"] != "SERVING" {
		t.Errorf("expected status SERVING, got %v", healthPayload["status"])
	}

	// 4. Create bucket via metadata client
	bucketName := "live-s3-bucket"
	_, err = metaClient.CreateBucket(context.Background(), &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucketName,
		OwnerId: "admin",
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 5. Use AWS S3 SDK to perform S3 PutObject over HTTP
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
		// o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		// o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	objectKey := "sample-upload.bin"
	payload := make([]byte, 6*1024*1024) // 1.5MB (spans 2 chunks)
	_, _ = rand.Read(payload)
	h := sha256.Sum256(payload)
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))

	putOut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	if err != nil {
		t.Fatalf("S3 PutObject failed over HTTP: %v", err)
	}
	if putOut.ETag == nil || *putOut.ETag != expectedETag {
		t.Fatalf("etag mismatch: expected %s, got %v", expectedETag, putOut.ETag)
	}

	// 6. Verify manifest in metadata-svc
	manifest, err := metaClient.GetManifest(context.Background(), &castorv1.GetManifestRequest{
		Bucket: bucketName,
		Key:    objectKey,
	})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if manifest.GetSize() != int64(len(payload)) {
		t.Fatalf("manifest size mismatch: expected %d, got %d", len(payload), manifest.GetSize())
	}
	if len(manifest.GetChunks()) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(manifest.GetChunks()))
	}
}
