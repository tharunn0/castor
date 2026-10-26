package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	dataconfig "github.com/tharunn0/castor/internal/data/config"
	dataserver "github.com/tharunn0/castor/internal/data/server"
	datastorage "github.com/tharunn0/castor/internal/data/storage"
	gwbackend "github.com/tharunn0/castor/internal/gateway/backend"
	gwconfig "github.com/tharunn0/castor/internal/gateway/config"
	gwhealth "github.com/tharunn0/castor/internal/gateway/health"
	gwstorage "github.com/tharunn0/castor/internal/gateway/storage"
	metaconfig "github.com/tharunn0/castor/internal/metadata/config"
	metaconsensus "github.com/tharunn0/castor/internal/metadata/consensus"
	metaregistry "github.com/tharunn0/castor/internal/metadata/registry"
	metaserver "github.com/tharunn0/castor/internal/metadata/server"
	metastore "github.com/tharunn0/castor/internal/metadata/store"
	metaworker "github.com/tharunn0/castor/internal/metadata/worker"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type healingDataNode struct {
	addr    string
	store   *datastorage.Storage
	cleanup func()
}

func startHealingDataNode(t *testing.T, nodeID string) *healingDataNode {
	t.Helper()

	tmpDir := t.TempDir()
	casStore, err := datastorage.New(tmpDir, 4<<20)
	if err != nil {
		t.Fatalf("[%s] failed to initialize CAS storage: %v", nodeID, err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("[%s] failed to listen on loopback port: %v", nodeID, err)
	}

	addr := lis.Addr().String()
	nodeCfg := dataconfig.NodeConfig{
		NodeId:   nodeID,
		GRPCAddr: addr,
		DataDir:  tmpDir,
	}

	grpcServer := grpc.NewServer()
	srv := dataserver.New(casStore, nodeCfg)
	castorv1.RegisterDataServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	}

	return &healingDataNode{
		addr:    addr,
		store:   casStore,
		cleanup: cleanup,
	}
}

type healingMetadataCluster struct {
	addr        string
	store       *metastore.Store
	raft        *metaconsensus.RaftNode
	registry    *metaregistry.Registry
	healer      *metaworker.ActiveReplicaHealer
	gc          *metaworker.QuarantineGCWorker
	coordinator *metaworker.LeaderCoordinator
	client      castorv1.MetadataServiceClient
	cleanup     func()
}

func startHealingMetadataCluster(t *testing.T, nodeID string, gcTTL time.Duration, healInterval time.Duration) *healingMetadataCluster {
	t.Helper()

	tmpDir := t.TempDir()
	badgerStore, err := metastore.Open(filepath.Join(tmpDir, "badger"), true)
	if err != nil {
		t.Fatalf("failed to open metadata store: %v", err)
	}

	fsm := metaconsensus.NewFSM(badgerStore, newTestLogger())
	raftCfg := metaconfig.Config{
		NodeID:        nodeID,
		RaftAddr:      "127.0.0.1:0",
		DataDir:       tmpDir,
		RaftBootstrap: true,
		RaftLogOutput: io.Discard,
	}

	raftNode, err := metaconsensus.NewRaftNode(raftCfg, fsm)
	if err != nil {
		_ = badgerStore.Close()
		t.Fatalf("failed to initialize raft node: %v", err)
	}

	for range 100 {
		if raftNode.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !raftNode.IsLeader() {
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
		t.Fatal("timed out waiting for metadata node to become Raft leader")
	}

	nodeRegistry := metaregistry.New()

	gcWorker := metaworker.NewQuarantineGCWorker(
		badgerStore,
		raftNode,
		metaworker.WithGCInterval(50*time.Millisecond),
		metaworker.WithQuarantineTTL(gcTTL),
		metaworker.WithGCLogger(newTestLogger()),
	)

	healerWorker := metaworker.NewActiveReplicaHealer(
		badgerStore,
		nodeRegistry,
		raftNode,
		metaworker.WithTargetReplicas(3),
		metaworker.WithHealInterval(healInterval),
		metaworker.WithHealerLogger(newTestLogger()),
	)

	coordinator := metaworker.NewLeaderCoordinator(
		raftNode,
		metaworker.WithWorkers(gcWorker, healerWorker),
		metaworker.WithLogger(newTestLogger()),
	)

	ctx := context.Background()
	if err := coordinator.Start(ctx); err != nil {
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
		t.Fatalf("failed to start leader coordinator: %v", err)
	}

	metaSrv := metaserver.New(
		badgerStore,
		raftNode,
		metaserver.WithRegistry(nodeRegistry),
		metaserver.WithRepairTrigger(healerWorker.TriggerRepair),
		metaserver.WithLogger(newTestLogger()),
	)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		coordinator.Stop()
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
		t.Fatalf("failed to listen on loopback: %v", err)
	}

	addr := lis.Addr().String()
	grpcServer := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(grpcServer, metaSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.GracefulStop()
		_ = lis.Close()
		coordinator.Stop()
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
		t.Fatalf("failed to connect to metadata gRPC: %v", err)
	}
	client := castorv1.NewMetadataServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		grpcServer.GracefulStop()
		coordinator.Stop()
		_ = lis.Close()
		_ = raftNode.Shutdown()
		_ = badgerStore.Close()
	}

	return &healingMetadataCluster{
		addr:        addr,
		store:       badgerStore,
		raft:        raftNode,
		registry:    nodeRegistry,
		healer:      healerWorker,
		gc:          gcWorker,
		coordinator: coordinator,
		client:      client,
		cleanup:     cleanup,
	}
}

func startHealingS3Gateway(t *testing.T, meta *healingMetadataCluster, dataNodes []string, writeQuorum int) (*s3.Client, *gwstorage.StorageEngine, func()) {
	t.Helper()

	s3Addr := getFreePort(t)
	consoleAddr := getFreePort(t)

	cfg := gwconfig.Config{
		S3Addr:               s3Addr,
		ConsoleAddr:          consoleAddr,
		MetadataAddr:         meta.addr,
		DataNodes:            dataNodes,
		RootAccessKey:        "admin",
		RootSecretKey:        "admin123",
		ChunkSize:            4 << 20, // 4MiB chunk size invariant
		WriteQuorum:          writeQuorum,
		MaxConcurrentUploads: 4,
	}

	engine := gwstorage.New(cfg, meta.client)
	be := gwbackend.New(engine, meta.client, gwbackend.WithLogger(newTestLogger()))
	healthHandler := gwhealth.NewHandler(cfg)

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
		cancel()
		_ = engine.Close()
		t.Fatal("s3 gateway failed to become healthy")
	}

	awsCfg, err := s3cfg.LoadDefaultConfig(context.Background(),
		s3cfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "admin123", "")),
		s3cfg.WithRegion("us-east-1"),
	)
	if err != nil {
		cancel()
		_ = engine.Close()
		t.Fatalf("failed to load AWS config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", s3Addr))
		o.UsePathStyle = true
	})

	cleanup := func() {
		cancel()
		_ = engine.Close()
	}

	return s3Client, engine, cleanup
}

func TestIntegration_SelfHealing_ActiveReplicaHealingAndFailover(t *testing.T) {
	// Start 4 data nodes to allow replication expansion from 2 to 3
	node1 := startHealingDataNode(t, "data-heal-1")
	defer node1.cleanup()
	node2 := startHealingDataNode(t, "data-heal-2")
	defer node2.cleanup()
	node3 := startHealingDataNode(t, "data-heal-3")
	defer node3.cleanup()
	node4 := startHealingDataNode(t, "data-heal-4")
	defer node4.cleanup()

	meta := startHealingMetadataCluster(t, "meta-heal-lead", 24*time.Hour, 50*time.Millisecond)
	defer meta.cleanup()

	// Register all 4 data nodes with storage capacities
	_ = meta.registry.Register("data-heal-1", node1.addr, 100<<30, 80<<30)
	_ = meta.registry.Register("data-heal-2", node2.addr, 100<<30, 75<<30)
	_ = meta.registry.Register("data-heal-3", node3.addr, 100<<30, 90<<30)
	_ = meta.registry.Register("data-heal-4", node4.addr, 100<<30, 85<<30)

	// Gateway with WriteQuorum=2 placing chunks onto 2 nodes initially
	s3Client, engine, gwCleanup := startHealingS3Gateway(t, meta, []string{node1.addr, node2.addr, node3.addr, node4.addr}, 2)
	defer gwCleanup()

	ctx := context.Background()
	bucket := "self-healing-bucket"
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 5MiB payload (strictly > 4MiB chunk size invariant)
	payloadSize := 5 * 1024 * 1024
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	expectedHash := sha256.Sum256(payload)
	expectedHex := hex.EncodeToString(expectedHash[:])

	objectKey := "multi-chunk-5mb.bin"
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	if err != nil {
		t.Fatalf("failed to upload 5MiB object: %v", err)
	}

	manifest, err := engine.GetManifest(ctx, bucket, objectKey)
	if err != nil {
		t.Fatalf("failed to get manifest: %v", err)
	}
	if len(manifest.GetChunks()) < 2 {
		t.Fatalf("expected at least 2 chunks for 5MiB payload, got: %d", len(manifest.GetChunks()))
	}

	chunk0Hash := manifest.GetChunks()[0].GetChunkHash()

	// Wait for ActiveReplicaHealer background cycle to restore R=3
	var fullyReplicated bool
	for range 50 {
		loc, err := meta.store.GetChunkLocation(ctx, chunk0Hash)
		if err == nil && len(loc.Nodes) >= 3 {
			fullyReplicated = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !fullyReplicated {
		loc, _ := meta.store.GetChunkLocation(ctx, chunk0Hash)
		var nodes []string
		if loc != nil {
			nodes = loc.Nodes
		}
		t.Fatalf("expected chunk to heal to >=3 replicas, got: %v", nodes)
	}

	// Verify that the newly replicated 3rd node physically has the chunk
	var physicalReplicas int
	for _, n := range []*healingDataNode{node1, node2, node3, node4} {
		if n.store.ChunkExists(chunk0Hash) {
			physicalReplicas++
		}
	}
	if physicalReplicas < 3 {
		t.Fatalf("expected at least 3 physical chunk replicas on data nodes, got: %d", physicalReplicas)
	}

	// Test Passive Failover: shut down node 1 and verify S3 GetObject succeeds
	node1.cleanup()

	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("failed to GetObject through replica failover: %v", err)
	}
	defer getResp.Body.Close()

	downloaded, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("failed to read downloaded body: %v", err)
	}
	if len(downloaded) != payloadSize {
		t.Fatalf("downloaded size mismatch: expected %d, got %d", payloadSize, len(downloaded))
	}
	actualHash := sha256.Sum256(downloaded)
	actualHex := hex.EncodeToString(actualHash[:])
	if actualHex != expectedHex {
		t.Fatalf("checksum mismatch on failover: expected %s, got %s", expectedHex, actualHex)
	}
}

func TestIntegration_SelfHealing_QuarantineGCPurge(t *testing.T) {
	node1 := startHealingDataNode(t, "data-gc-1")
	defer node1.cleanup()
	node2 := startHealingDataNode(t, "data-gc-2")
	defer node2.cleanup()

	// Short quarantine TTL for test execution (100ms)
	meta := startHealingMetadataCluster(t, "meta-gc-lead", 100*time.Millisecond, 1*time.Second)
	defer meta.cleanup()

	_ = meta.registry.Register("data-gc-1", node1.addr, 100<<30, 80<<30)
	_ = meta.registry.Register("data-gc-2", node2.addr, 100<<30, 75<<30)

	s3Client, engine, gwCleanup := startHealingS3Gateway(t, meta, []string{node1.addr, node2.addr}, 2)
	defer gwCleanup()

	ctx := context.Background()
	bucket := "gc-test-bucket"
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 5MiB payload (> 4MiB chunk size invariant)
	payload := make([]byte, 5*1024*1024)
	for i := range payload {
		payload[i] = byte(i % 199)
	}

	objectKey := "gc-file.bin"
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	manifest, err := engine.GetManifest(ctx, bucket, objectKey)
	if err != nil {
		t.Fatalf("failed to get manifest: %v", err)
	}
	chunk0Hash := manifest.GetChunks()[0].GetChunkHash()

	// Verify chunk physically exists on data node 1 or 2
	if !node1.store.ChunkExists(chunk0Hash) && !node2.store.ChunkExists(chunk0Hash) {
		t.Fatal("expected chunk to exist on data nodes prior to deletion")
	}

	// Delete object via S3 API
	_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("failed to delete object: %v", err)
	}

	// Wait for Quarantine TTL to elapse and GC worker cycle to purge chunk
	var purgedFromMeta bool
	for range 50 {
		_, err := meta.store.GetChunkLocation(ctx, chunk0Hash)
		if errors.Is(err, metastore.ErrNotFound) {
			purgedFromMeta = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !purgedFromMeta {
		t.Fatal("expected chunk location record to be purged from metadata by GC worker")
	}

	// Verify chunk was physically deleted from data nodes
	if node1.store.ChunkExists(chunk0Hash) {
		t.Fatalf("expected chunk %s to be deleted from node 1 storage", chunk0Hash)
	}
	if node2.store.ChunkExists(chunk0Hash) {
		t.Fatalf("expected chunk %s to be deleted from node 2 storage", chunk0Hash)
	}
}
