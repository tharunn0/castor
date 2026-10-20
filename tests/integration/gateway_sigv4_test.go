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
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	gwauth "github.com/tharunn0/castor/internal/gateway/auth"
	gwbackend "github.com/tharunn0/castor/internal/gateway/backend"
	gwconfig "github.com/tharunn0/castor/internal/gateway/config"
	gwstorage "github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/versity/versitygw/embedgw"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestIntegration_GatewayDynamicSigV4(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-sigv4-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-sigv4-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-sigv4-3")
	defer cleanup3()

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-sigv4")
	defer metaCleanup()

	metaConn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial metadata node: %v", err)
	}
	defer metaConn.Close()
	metaClient := castorv1.NewMetadataServiceClient(metaConn)

	activeAccessKey := "AKIAUSER1234567890AB"
	activeSecretKey := "userSecretKey123456789012345678901234"
	revokedAccessKey := "AKIAREVOKED123456789A"

	// Mock auth-svc HTTP endpoint for /internal/v1/validate-key
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("access_key_id")
		switch key {
		case activeAccessKey:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(gwauth.ValidateKeyResponse{
				AccessKeyID:     activeAccessKey,
				SecretAccessKey: activeSecretKey,
				UserID:          "user-uuid-1234",
				Status:          gwauth.StatusActive,
				CreatedAt:       time.Now(),
			})
		case revokedAccessKey:
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"credential is revoked"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"credential not found"}`))
		}
	}))
	defer authServer.Close()

	s3Addr := getFreePort(t)

	cfg := gwconfig.Config{
		S3Addr:               s3Addr,
		ConsoleAddr:          getFreePort(t),
		MetadataAddr:         metaAddr,
		DataNodes:            []string{node1Addr, node2Addr, node3Addr},
		AuthAddr:             authServer.URL,
		RootAccessKey:        "admin",
		RootSecretKey:        "admin123",
		ChunkSize:            int64(MaxChunkSize),
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
	}

	engine := gwstorage.New(cfg, metaClient)
	defer engine.Close()

	be := gwbackend.New(engine, metaClient, gwbackend.WithLogger(newTestLogger()))

	authClient := gwauth.NewHTTPClient(cfg.AuthAddr)
	iamBridge := gwauth.NewBridge(authClient, cfg.RootAccessKey, cfg.RootSecretKey, newTestLogger())
	iamCache := gwauth.NewCachedBridge(iamBridge)
	defer func() { _ = iamCache.Shutdown() }()

	gwCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwCfg := &embedgw.Config{
		Ports:             []string{s3Addr},
		RootUserAccess:    cfg.RootAccessKey,
		RootUserSecret:    cfg.RootSecretKey,
		Region:            "us-east-1",
		Quiet:             false,
		MaxConnections:    65536,
		MaxRequests:       65536,
		MultipartMaxParts: 10000,
		IAMService:        iamCache,
	}

	gwErrCh := make(chan error, 1)
	go func() {
		gwErrCh <- embedgw.RunVersityGW(gwCtx, be, gwCfg)
	}()

	// Wait for S3 server to become available
	waitForPort(t, s3Addr, gwErrCh)

	activeClient := newS3Client(t, s3Addr, activeAccessKey, activeSecretKey)

	bucketName := "dynamic-sigv4-bucket"
	_, err = activeClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("failed to create bucket with active SigV4 credentials: %v", err)
	}

	// 1. Dynamic active credentials perform multi-chunk PutObject and GetObject via SigV4

	payload := make([]byte, 5*1024*1024) // strictly > 4MiB chunk size invariant
	_, _ = rand.Read(payload)
	h := sha256.Sum256(payload)
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))
	objectKey := "dynamic-sigv4-file.bin"

	putOut, err := activeClient.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	if err != nil {
		t.Fatalf("PutObject with dynamic active SigV4 credentials failed: %v", err)
	}
	if putOut.ETag == nil || *putOut.ETag != expectedETag {
		t.Fatalf("ETag mismatch: expected %s, got %v", expectedETag, putOut.ETag)
	}

	getOut, err := activeClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("GetObject with dynamic active SigV4 credentials failed: %v", err)
	}
	defer getOut.Body.Close()

	downloaded, err := io.ReadAll(getOut.Body)
	if err != nil {
		t.Fatalf("reading downloaded body failed: %v", err)
	}
	if !bytes.Equal(downloaded, payload) {
		t.Fatalf("downloaded payload does not match uploaded payload")
	}

	// 2. Root credentials bypass validation and succeed
	rootClient := newS3Client(t, s3Addr, cfg.RootAccessKey, cfg.RootSecretKey)
	headOut, err := rootClient.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("HeadObject with root credentials failed: %v", err)
	}
	if *headOut.ContentLength != int64(len(payload)) {
		t.Fatalf("HeadObject size mismatch: expected %d, got %d", len(payload), *headOut.ContentLength)
	}

	// 3. Revoked credentials must be rejected
	revokedClient := newS3Client(t, s3Addr, revokedAccessKey, "anySecretKey12345678901234567890123")
	_, err = revokedClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err == nil {
		t.Fatalf("expected GetObject with revoked credentials to fail, but succeeded")
	}

	// 4. Unknown/non-existent credentials must be rejected
	unknownClient := newS3Client(t, s3Addr, "AKIAUNKNOWN1234567890", "unknownSecretKey123456789012345678")
	_, err = unknownClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err == nil {
		t.Fatalf("expected GetObject with unknown credentials to fail, but succeeded")
	}
}

func waitForPort(t *testing.T, addr string, errCh <-chan error) {
	t.Helper()
	client := &http.Client{Timeout: 300 * time.Millisecond}
	for range 60 {
		select {
		case err := <-errCh:
			t.Fatalf("gateway server failed to start: %v", err)
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			resp, err := client.Get(fmt.Sprintf("http://%s/", addr))
			if err == nil {
				_ = resp.Body.Close()
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server on %s failed to become ready in time", addr)
}

func newS3Client(t *testing.T, s3Addr, accessKey, secretKey string) *s3.Client {
	t.Helper()
	awsCfg, err := s3cfg.LoadDefaultConfig(context.Background(),
		s3cfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		s3cfg.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config for client: %v", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", s3Addr))
		o.UsePathStyle = true
	})
}
