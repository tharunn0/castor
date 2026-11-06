package integration

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

// writeSelfSignedCert writes a cert/key pair valid for 127.0.0.1 and returns
// their paths plus a pool that trusts the cert.
func writeSelfSignedCert(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "castor-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "tls.crt")
	keyFile = filepath.Join(dir, "tls.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	pool = x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return certFile, keyFile, pool
}

func TestIntegration_GatewayTLS(t *testing.T) {
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

	registerTestDataNode(t, metaClient, "data-1", node1Addr)
	registerTestDataNode(t, metaClient, "data-2", node2Addr)
	registerTestDataNode(t, metaClient, "data-3", node3Addr)

	certFile, keyFile, pool := writeSelfSignedCert(t)
	s3Addr := getFreePort(t)

	cfg := gwconfig.Config{
		S3Addr:               s3Addr,
		S3TLSCertFile:        certFile,
		S3TLSKeyFile:         keyFile,
		MetadataAddr:         metaAddr,
		RootAccessKey:        "admin",
		RootSecretKey:        "admin123",
		ChunkSize:            int64(MaxChunkSize),
		WriteQuorum:          2,
		MaxConcurrentUploads: 4,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation failed: %v", err)
	}

	engine := gwstorage.New(cfg, metaClient)
	defer engine.Close()
	be := gwbackend.New(engine, metaClient, gwbackend.WithLogger(newTestLogger()))
	healthHandler := gwhealth.NewHandler(cfg)

	gwCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwCfg := &embedgw.Config{
		Ports:             []string{cfg.S3Addr},
		CertFile:          cfg.S3TLSCertFile,
		KeyFile:           cfg.S3TLSKeyFile,
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

	httpsClient := &http.Client{
		Timeout:   time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
	}

	var ready bool
	for range 100 {
		select {
		case err := <-gwErrCh:
			t.Fatalf("embedgw exited: %v", err)
		default:
		}
		resp, err := httpsClient.Get(fmt.Sprintf("https://%s/health", s3Addr))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("TLS S3 server failed to become ready")
	}

	plainClient := &http.Client{Timeout: time.Second}
	if resp, err := plainClient.Get(fmt.Sprintf("http://%s/health", s3Addr)); err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("plain HTTP request succeeded against TLS listener")
		}
	}

	bucketName := "tls-bucket"
	if _, err := metaClient.CreateBucket(context.Background(), &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucketName,
		OwnerId: "admin",
	}); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	awsCfg, err := s3cfg.LoadDefaultConfig(context.Background(),
		s3cfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "admin123", "")),
		s3cfg.WithRegion("us-east-1"),
		s3cfg.WithHTTPClient(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("https://%s", s3Addr))
		o.UsePathStyle = true
	})

	payload := make([]byte, 6*1024*1024)
	_, _ = rand.Read(payload)

	if _, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String("over-tls.bin"),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	}); err != nil {
		t.Fatalf("PutObject over TLS failed: %v", err)
	}

	getOut, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("over-tls.bin"),
	})
	if err != nil {
		t.Fatalf("GetObject over TLS failed: %v", err)
	}
	defer getOut.Body.Close()
	got, err := io.ReadAll(getOut.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch over TLS: got %d bytes, want %d", len(got), len(payload))
	}
}
