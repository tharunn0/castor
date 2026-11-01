package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type AuditConfig struct {
	MetaAddr     string
	Bucket       string
	Key          string
	Verify       bool
	VerifyHash   bool
	Timeout      time.Duration
	JSON         bool
	DockerRemap  bool
	Limit        int
	OnlyDegraded bool
	Workers      int
}

type NodeChunkStatus struct {
	NodeAddress string `json:"node_address"`
	Status      string `json:"status"`
	Detail      string `json:"detail,omitempty"`
}

type ChunkAudit struct {
	Index       int               `json:"index"`
	ChunkHash   string            `json:"chunk_hash"`
	Size        int64             `json:"size"`
	Replicas    []NodeChunkStatus `json:"replicas"`
	Healthy     bool              `json:"healthy"`
	ActiveNodes int               `json:"active_nodes"`
}

type ManifestAuditReport struct {
	Bucket       string       `json:"bucket"`
	Key          string       `json:"key"`
	Size         int64        `json:"size"`
	ETag         string       `json:"etag"`
	ContentType  string       `json:"content_type"`
	Status       string       `json:"status"`
	OwnerID      string       `json:"owner_id"`
	TotalChunks  int          `json:"total_chunks"`
	Chunks       []ChunkAudit `json:"chunks"`
	AllHealthy   bool         `json:"all_healthy"`
	AuditSummary string       `json:"audit_summary"`
}

type DataClientFactory func(ctx context.Context, addr string) (castorv1.DataServiceClient, func(), error)

func defaultDataClientFactory(_ context.Context, addr string) (castorv1.DataServiceClient, func(), error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return castorv1.NewDataServiceClient(conn), func() { _ = conn.Close() }, nil
}

func RemapNodeAddress(addr string, dockerRemap bool) string {
	if !dockerRemap {
		return addr
	}
	switch addr {
	case "data-1:9101":
		return "127.0.0.1:9101"
	case "data-2:9101":
		return "127.0.0.1:9102"
	case "data-3:9101":
		return "127.0.0.1:9103"
	default:
		var n int
		if _, err := fmt.Sscanf(addr, "data-%d:9101", &n); err == nil && n >= 1 && n <= 9 {
			return fmt.Sprintf("127.0.0.1:%d", 9100+n)
		}
		return addr
	}
}

func probeDataNode(ctx context.Context, client castorv1.DataServiceClient, chunkHash string, verifyHash bool) (string, string) {
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.GetChunk(probeCtx, &castorv1.GetChunkRequest{
		ChunkHash: chunkHash,
	})
	if err != nil {
		return "ERROR", err.Error()
	}

	firstFrame, err := stream.Recv()
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "NOT_FOUND", "chunk missing on data node"
		}
		return "ERROR", err.Error()
	}

	if !verifyHash {
		// Existence is verified; cancel early to avoid streaming remaining bytes
		return "OK", "present on disk"
	}

	hasher := sha256.New()
	hasher.Write(firstFrame.GetChunkBytes())
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "STREAM_ERROR", err.Error()
		}
		hasher.Write(frame.GetChunkBytes())
	}

	digest := hex.EncodeToString(hasher.Sum(nil))
	if digest != chunkHash {
		return "CORRUPTED", fmt.Sprintf("hash mismatch: expected %s, got %s", chunkHash, digest)
	}

	return "VERIFIED", "checksum matches SHA-256"
}

func AuditManifest(
	ctx context.Context,
	cfg AuditConfig,
	metaClient castorv1.MetadataServiceClient,
	dialDataNode DataClientFactory,
) (*ManifestAuditReport, error) {
	manifestResp, err := metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: cfg.Bucket,
		Key:    cfg.Key,
	})
	if err != nil {
		return nil, err
	}

	chunks := manifestResp.GetChunks()
	report := &ManifestAuditReport{
		Bucket:      manifestResp.GetBucket(),
		Key:         manifestResp.GetKey(),
		Size:        manifestResp.GetSize(),
		ETag:        manifestResp.GetEtag(),
		ContentType: manifestResp.GetContentType(),
		Status:      manifestResp.GetStatus(),
		OwnerID:     manifestResp.GetOwnerId(),
		TotalChunks: len(chunks),
		Chunks:      make([]ChunkAudit, len(chunks)),
		AllHealthy:  true,
	}

	if len(chunks) == 0 {
		report.AuditSummary = "Manifest has 0 chunks"
		return report, nil
	}

	workerCount := cfg.Workers
	if workerCount <= 0 {
		workerCount = 10
	}
	if workerCount > len(chunks) {
		workerCount = len(chunks)
	}

	type chunkJob struct {
		index int
		chunk *castorv1.ChunkWithLocations
	}

	jobs := make(chan chunkJob, len(chunks))
	for i, chunk := range chunks {
		jobs <- chunkJob{index: i, chunk: chunk}
	}
	close(jobs)

	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				chunk := job.chunk
				chunkAudit := ChunkAudit{
					Index:     job.index,
					ChunkHash: chunk.GetChunkHash(),
					Size:      chunk.GetSize(),
					Replicas:  make([]NodeChunkStatus, 0, len(chunk.GetNodeAddresses())),
				}

				activeCount := 0
				for _, nodeAddr := range chunk.GetNodeAddresses() {
					nodeStatus := NodeChunkStatus{
						NodeAddress: nodeAddr,
					}

					if cfg.Verify {
						dialAddr := RemapNodeAddress(nodeAddr, cfg.DockerRemap)
						nodeCtx, nodeCancel := context.WithTimeout(ctx, cfg.Timeout)
						dataClient, cleanup, dialErr := dialDataNode(nodeCtx, dialAddr)
						if dialErr != nil {
							nodeStatus.Status = "UNREACHABLE"
							nodeStatus.Detail = dialErr.Error()
						} else {
							st, detail := probeDataNode(nodeCtx, dataClient, chunk.GetChunkHash(), cfg.VerifyHash)
							nodeStatus.Status = st
							nodeStatus.Detail = detail
							cleanup()
						}
						nodeCancel()

						if nodeStatus.Status == "OK" || nodeStatus.Status == "VERIFIED" {
							activeCount++
						}
					} else {
						nodeStatus.Status = "MAPPED"
						nodeStatus.Detail = "unverified"
						activeCount++
					}

					chunkAudit.Replicas = append(chunkAudit.Replicas, nodeStatus)
				}

				chunkAudit.ActiveNodes = activeCount
				chunkAudit.Healthy = activeCount >= 2 || (!cfg.Verify && len(chunk.GetNodeAddresses()) >= 2)
				report.Chunks[job.index] = chunkAudit
			}
		}()
	}
	wg.Wait()

	for _, c := range report.Chunks {
		if !c.Healthy {
			report.AllHealthy = false
			break
		}
	}

	if report.AllHealthy {
		report.AuditSummary = fmt.Sprintf("All %d chunks meet replication quorum (W=2)", report.TotalChunks)
	} else {
		report.AuditSummary = "Degraded: one or more chunks lack required replication quorum"
	}

	return report, nil
}

func RenderReport(w io.Writer, report *ManifestAuditReport, cfg AuditConfig) {
	fmt.Fprintf(w, "Object: s3://%s/%s (%.2f MiB, %d bytes)\n", report.Bucket, report.Key, float64(report.Size)/(1<<20), report.Size)
	fmt.Fprintf(w, "ETag:   %s\n", report.ETag)
	fmt.Fprintf(w, "Status: %s | Chunks: %d\n\n", report.Status, report.TotalChunks)

	degradedCount := 0
	for _, c := range report.Chunks {
		if !c.Healthy {
			degradedCount++
		}
	}

	if cfg.OnlyDegraded && degradedCount == 0 {
		fmt.Fprintf(w, "All %d chunks meet replication quorum; 0 degraded chunks to display.\n\n", len(report.Chunks))
	} else {
		displayed := 0
		for _, c := range report.Chunks {
			if cfg.OnlyDegraded && c.Healthy {
				continue
			}

			if cfg.Limit > 0 && displayed >= cfg.Limit {
				omitted := len(report.Chunks) - displayed
				if cfg.OnlyDegraded {
					omitted = degradedCount - displayed
				}
				fmt.Fprintf(w, "... (%d more chunks omitted. Use -limit 0 to display all %d chunks)\n\n", omitted, len(report.Chunks))
				break
			}

			fmt.Fprintf(w, "[Chunk #%d] %s (%d bytes)\n", c.Index, c.ChunkHash, c.Size)
			if len(c.Replicas) == 0 {
				fmt.Fprintln(w, "  (No replica locations recorded in metadata)")
				displayed++
				continue
			}

			for _, replica := range c.Replicas {
				fmt.Fprintf(w, "  %-20s -> [%s] (%s)\n", replica.NodeAddress, replica.Status, replica.Detail)
			}

			if cfg.Verify {
				healthStatus := "Healthy"
				if !c.Healthy {
					healthStatus = "DEGRADED"
				}
				fmt.Fprintf(w, "  Replicas verified: %d/%d (%s)\n\n", c.ActiveNodes, len(c.Replicas), healthStatus)
			} else {
				fmt.Fprintf(w, "  Mapped nodes: %d\n\n", len(c.Replicas))
			}
			displayed++
		}
	}

	if report.AllHealthy {
		fmt.Fprintf(w, "RESULT: HEALTHY (%s)\n", report.AuditSummary)
	} else {
		fmt.Fprintf(w, "RESULT: WARNING (%s)\n", report.AuditSummary)
	}
}

func main() {
	cfg := AuditConfig{}
	flag.StringVar(&cfg.MetaAddr, "meta", "127.0.0.1:9090", "Metadata service gRPC address")
	flag.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket name (required)")
	flag.StringVar(&cfg.Key, "key", "", "S3 object key (required)")
	flag.BoolVar(&cfg.Verify, "verify", true, "Probe data-svc nodes to confirm physical chunk presence")
	flag.BoolVar(&cfg.VerifyHash, "verify-hash", false, "Stream full chunks and verify SHA-256 integrity (slower)")
	flag.BoolVar(&cfg.DockerRemap, "docker", true, "Remap internal Docker container addresses (data-N:9101 -> 127.0.0.1:910N)")
	flag.IntVar(&cfg.Limit, "limit", 10, "Max chunks to print in detail (0 to print all)")
	flag.BoolVar(&cfg.OnlyDegraded, "only-degraded", false, "Only print chunks that are degraded or failed verification")
	flag.IntVar(&cfg.Workers, "worker", 10, "Number of concurrent workers for probing chunks")
	flag.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "Timeout per gRPC operation")
	flag.BoolVar(&cfg.JSON, "json", false, "Output results in JSON format")
	flag.Parse()

	if cfg.Bucket == "" || cfg.Key == "" {
		fmt.Fprintf(os.Stderr, "Error: -bucket and -key flags are required.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if envAddr := os.Getenv("CASTOR_METADATA_ADDR"); envAddr != "" && cfg.MetaAddr == "127.0.0.1:9090" {
		cfg.MetaAddr = envAddr
	}

	dialCtx, dialCancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer dialCancel()

	metaConn, err := grpc.NewClient(cfg.MetaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to metadata service at %s: %v\n", cfg.MetaAddr, err)
		os.Exit(1)
	}
	defer metaConn.Close()

	metaClient := castorv1.NewMetadataServiceClient(metaConn)

	report, err := AuditManifest(dialCtx, cfg, metaClient, defaultDataClientFactory)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			fmt.Fprintf(os.Stderr, "Object not found: s3://%s/%s\n", cfg.Bucket, cfg.Key)
		} else {
			fmt.Fprintf(os.Stderr, "Audit failed: %v\n", err)
		}
		os.Exit(1)
	}

	if cfg.JSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fmt.Fprintf(os.Stderr, "JSON encoding error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	RenderReport(os.Stdout, report, cfg)
	if !report.AllHealthy {
		os.Exit(2)
	}
}
