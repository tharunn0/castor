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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

func TestIntegration_GatewayPresignedURLs(t *testing.T) {
	node1Addr, cleanup1 := startTestDataNode(t, "data-presign-1")
	defer cleanup1()
	node2Addr, cleanup2 := startTestDataNode(t, "data-presign-2")
	defer cleanup2()
	node3Addr, cleanup3 := startTestDataNode(t, "data-presign-3")
	defer cleanup3()

	metaAddr, _, metaCleanup := startTestMetadataNode(t, "meta-presign")
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
	revokedSecretKey := "revokedSecret12345678901234567890123"

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("access_key_id")
		switch key {
		case activeAccessKey:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(gwauth.ValidateKeyResponse{
				AccessKeyID:     activeAccessKey,
				SecretAccessKey: activeSecretKey,
				UserID:          "user-uuid-presign",
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

	be := gwbackend.New(engine, metaClient, newTestLogger())

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

	waitForPort(t, s3Addr, gwErrCh)

	activeClient := newS3Client(t, s3Addr, activeAccessKey, activeSecretKey)
	presignClient := s3.NewPresignClient(activeClient)

	bucketName := "presign-test-bucket"
	_, err = activeClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 1. Presigned PUT with multi-chunk payload (> 4MiB chunk size invariant)
	payloadSize := 5 * 1024 * 1024
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)
	h := sha256.Sum256(payload)
	expectedETag := fmt.Sprintf("%q", hex.EncodeToString(h[:]))
	objectKey := "multi-chunk-presigned.bin"

	presignedPut, err := presignClient.PresignPutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		t.Fatalf("failed to generate presigned PUT request: %v", err)
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}

	putReq, err := http.NewRequestWithContext(context.Background(), http.MethodPut, presignedPut.URL, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("failed to create HTTP PUT request: %v", err)
	}
	putReq.ContentLength = int64(len(payload))
	for k, vs := range presignedPut.SignedHeader {
		for _, v := range vs {
			putReq.Header.Add(k, v)
		}
	}

	putResp, err := httpClient.Do(putReq)
	if err != nil {
		t.Fatalf("presigned PUT request failed: %v", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(putResp.Body)
		t.Fatalf("presigned PUT expected 200 OK, got %d: %s", putResp.StatusCode, string(body))
	}

	putETag := putResp.Header.Get("ETag")
	if putETag != expectedETag {
		t.Fatalf("presigned PUT ETag mismatch: expected %s, got %s", expectedETag, putETag)
	}

	// 2. Presigned GET multi-chunk download
	presignedGet, err := presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		t.Fatalf("failed to generate presigned GET request: %v", err)
	}

	getReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, presignedGet.URL, nil)
	if err != nil {
		t.Fatalf("failed to create HTTP GET request: %v", err)
	}
	for k, vs := range presignedGet.SignedHeader {
		for _, v := range vs {
			getReq.Header.Add(k, v)
		}
	}

	getResp, err := httpClient.Do(getReq)
	if err != nil {
		t.Fatalf("presigned GET request failed: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("presigned GET expected 200 OK, got %d: %s", getResp.StatusCode, string(body))
	}

	if getResp.ContentLength != int64(payloadSize) {
		t.Fatalf("presigned GET Content-Length mismatch: expected %d, got %d", payloadSize, getResp.ContentLength)
	}

	downloaded, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("reading presigned GET response body failed: %v", err)
	}
	if !bytes.Equal(downloaded, payload) {
		t.Fatalf("downloaded payload does not match uploaded payload")
	}

	// 3. Presigned HEAD metadata request
	presignedHead, err := presignClient.PresignHeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		t.Fatalf("failed to generate presigned HEAD request: %v", err)
	}

	headReq, err := http.NewRequestWithContext(context.Background(), http.MethodHead, presignedHead.URL, nil)
	if err != nil {
		t.Fatalf("failed to create HTTP HEAD request: %v", err)
	}
	for k, vs := range presignedHead.SignedHeader {
		for _, v := range vs {
			headReq.Header.Add(k, v)
		}
	}

	headResp, err := httpClient.Do(headReq)
	if err != nil {
		t.Fatalf("presigned HEAD request failed: %v", err)
	}
	defer headResp.Body.Close()

	if headResp.StatusCode != http.StatusOK {
		t.Fatalf("presigned HEAD expected 200 OK, got %d", headResp.StatusCode)
	}
	if headResp.ContentLength != int64(payloadSize) {
		t.Fatalf("presigned HEAD Content-Length mismatch: expected %d, got %d", payloadSize, headResp.ContentLength)
	}
	if headResp.Header.Get("ETag") != expectedETag {
		t.Fatalf("presigned HEAD ETag mismatch: expected %s, got %s", expectedETag, headResp.Header.Get("ETag"))
	}

	// 4. Presigned DELETE request
	presignedDelete, err := presignClient.PresignDeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		t.Fatalf("failed to generate presigned DELETE request: %v", err)
	}

	deleteReq, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, presignedDelete.URL, nil)
	if err != nil {
		t.Fatalf("failed to create HTTP DELETE request: %v", err)
	}
	for k, vs := range presignedDelete.SignedHeader {
		for _, v := range vs {
			deleteReq.Header.Add(k, v)
		}
	}

	deleteResp, err := httpClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("presigned DELETE request failed: %v", err)
	}
	defer deleteResp.Body.Close()

	if deleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleteResp.Body)
		t.Fatalf("presigned DELETE expected 204 No Content, got %d: %s", deleteResp.StatusCode, string(body))
	}

	// Verify object is gone
	_, err = activeClient.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err == nil {
		t.Fatalf("expected HeadObject after deletion to fail, but succeeded")
	}

	// Re-upload object to test security edge cases
	_, err = activeClient.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	if err != nil {
		t.Fatalf("re-uploading object for security tests failed: %v", err)
	}

	// 5. Expired presigned URL rejection
	expiredPresignedGet, err := presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(1*time.Second))
	if err != nil {
		t.Fatalf("failed to generate short-lived presigned GET: %v", err)
	}

	time.Sleep(2 * time.Second)

	expiredReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, expiredPresignedGet.URL, nil)
	if err != nil {
		t.Fatalf("failed to create expired HTTP request: %v", err)
	}
	expiredResp, err := httpClient.Do(expiredReq)
	if err != nil {
		t.Fatalf("expired GET request execution failed: %v", err)
	}
	defer expiredResp.Body.Close()

	if expiredResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected expired presigned URL to return 403 Forbidden, got %d", expiredResp.StatusCode)
	}
	expiredBody, _ := io.ReadAll(expiredResp.Body)
	if !strings.Contains(string(expiredBody), "Request has expired") || !strings.Contains(string(expiredBody), "AccessDenied") {
		t.Fatalf("expected AccessDenied / Request has expired error, got: %s", string(expiredBody))
	}

	// 6. Tampered signature rejection
	tamperedURL := presignedGet.URL + "tampered"
	tamperedReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, tamperedURL, nil)
	if err != nil {
		t.Fatalf("failed to create tampered HTTP request: %v", err)
	}
	tamperedResp, err := httpClient.Do(tamperedReq)
	if err != nil {
		t.Fatalf("tampered GET request execution failed: %v", err)
	}
	defer tamperedResp.Body.Close()

	if tamperedResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected tampered URL to return 403 Forbidden, got %d", tamperedResp.StatusCode)
	}
	tamperedBody, _ := io.ReadAll(tamperedResp.Body)
	if !strings.Contains(string(tamperedBody), "SignatureDoesNotMatch") {
		t.Fatalf("expected SignatureDoesNotMatch error code, got: %s", string(tamperedBody))
	}

	// 7. Revoked credentials rejection
	revokedClient := newS3Client(t, s3Addr, revokedAccessKey, revokedSecretKey)
	revokedPresignClient := s3.NewPresignClient(revokedClient)

	revokedPresignedGet, err := revokedPresignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		t.Fatalf("failed to generate presigned GET with revoked credentials: %v", err)
	}

	revokedReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, revokedPresignedGet.URL, nil)
	if err != nil {
		t.Fatalf("failed to create revoked HTTP request: %v", err)
	}
	revokedResp, err := httpClient.Do(revokedReq)
	if err != nil {
		t.Fatalf("revoked GET request execution failed: %v", err)
	}
	defer revokedResp.Body.Close()

	if revokedResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected revoked presigned URL to return 403 Forbidden, got %d", revokedResp.StatusCode)
	}
	revokedBody, _ := io.ReadAll(revokedResp.Body)
	if !strings.Contains(string(revokedBody), "InvalidAccessKeyId") {
		t.Fatalf("expected InvalidAccessKeyId error code, got: %s", string(revokedBody))
	}
}
