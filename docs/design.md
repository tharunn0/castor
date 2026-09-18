# Castor — Minimal Design

## 1. Scope

**In:** Raft-replicated metadata, content-addressed chunk storage, majority quorum chunk replication (W=2/R=3), active background replica healing & passive read-repair, dynamic heartbeat node registry with capacity-aware placement, multipart upload, embedded leader GC worker, **S3 REST API compatibility (AWS CLI & SDK support, SigV4 auth, XML responses)**, HTTP health/admin endpoints.
**Out (cut for time-box):** Complex AWS IAM / STS / bucket policy engine (static credentials only), erasure coding, live compactor service, incremental snapshots, cross-datacenter multi-region replication.

## 2. Components (3 backend services)

```
                       ┌────────────────────────┐
                       │ AWS CLI / S3 SDKs /    │
                       │ Rclone (HTTP/REST S3)  │
                       └───────────┬────────────┘
                                   │ HTTP/REST (S3 :9000)
                                   ▼
                    ┌──────────────────────────────────────┐
                    │           gateway-svc [xN]           │ (stateless)
                    │  (Embedded S3 Frontend + Ingest Engine)
                    └───┬──────────────────────────────┬───┘
             gRPC (meta)│                              │ gRPC (chunks)
                        ▼                              ▼
            ┌───────────────────┐            ┌───────────────────────────┐
            │ metadata-svc [x3] │────────────► data-svc [x3, independent]│
            │ (Raft + Workers)  │            └───────────────────────────┘
            └───────────────────┘              (leader worker: GC & healing)
```

- **gateway-svc**: stateless entrypoint exposing an S3-compatible HTTP/REST API (port `:9000`) for standard clients (`aws-cli`, `boto3`, `rclone`). Handles stream chunking (4MB), SHA-256 hashing, coordinates chunk writes/reads to `data-svc`, performs passive read-repair, and coordinates metadata transactions with `metadata-svc`. Scalable horizontally.
- **metadata-svc**: Raft-replicated metadata store running embedded background workers on the active Raft leader (garbage collection, abandoned multipart cleanup, and under-replicated chunk healing). Maintains dynamic `data-svc` heartbeat registry and BadgerDB state machine. One leader serves writes and coordinates background tasks; followers redirect. Also exposes HTTP `/healthz`, `/metrics`, and `/admin/*` endpoints.
- **data-svc**: stateless-per-chunk store. Chunk = file on disk, keyed by SHA-256. Registers and heartbeats to `metadata-svc`. Executes peer-to-peer chunk transfers (`ReplicateChunk`) on demand.

## 3. Data Model

### Metadata (Raft FSM, backed by BadgerDB on metadata-svc)
```go
type Bucket struct {
    Name      string
    CreatedAt time.Time
}

type ObjectManifest struct {
    Bucket    string
    Key       string
    Size      int64
    ChunkIDs  []string   // SHA-256 hex, ordered
    CreatedAt time.Time
    Status    string     // "pending" | "committed" | "deleted"
}

type ChunkLocation struct {
    ChunkHash  string
    Nodes      []string   // data-svc addrs holding this chunk
    RefCount   int        // incremented per manifest referencing it
    OrphanedAt time.Time  // set when RefCount reaches 0 (used for 24h quarantine)
}
```
All Raft log entries and BadgerDB records are serialized using Protocol Buffers (Protobuf). Raft log entries are commands: `CreateBucket`, `DeleteBucket`, `PutManifest`, `CommitManifest`, `DeleteManifest`, `UpdateChunkLocation`, `RemoveChunkLocation`. FSM applies them to BadgerDB. Key schemas in BadgerDB:
- Buckets: `bucket:<bucket_name>`
- Manifests: `manifest:<bucket_name>:<object_key>`
- Chunk Locations: `chunk:<sha256>`

### Chunk (data-svc, local disk)
```
/data/chunks/<sha256>   # raw bytes, whole file, no custom format
```
- No offset indexing — one file per chunk. Directory sharded by first 2 hex chars to avoid huge flat directories (`/data/chunks/ab/abcdef...`).
- **Crash Consistency (Atomic Writes):** Streams are written to `/data/staging/<uuid>.tmp` on the same mount. Once fully written and SHA-256 verified, an atomic `os.Rename` moves the file into `/data/chunks/xx/<sha256>`, preventing torn chunks on crash.
- **Durability:** Configurable `storage.sync_writes: bool` (default `true` calls `file.Sync()` before returning ACK).

## 4. Chunking

- Fixed-size chunks (e.g. 4MB) on upload. Handled entirely by `gateway-svc`: splits incoming stream into chunks and computes SHA-256 per chunk.
- Chunk size fixed constant for v1 — no content-defined chunking (dedup gain not worth the complexity here).
- **Small Objects:** Objects smaller than 4MB (including tiny files $< 64$KB) follow the exact same uniform pipeline and are stored as a single chunk on `data-svc`. Keeps payload bytes completely out of `metadata-svc` Raft logs.
- **Hashing:** Standard SHA-256 wrapped behind a `Hasher` interface in `gateway-svc` to permit alternative algorithms (e.g. BLAKE3) later.
- **Ingress Buffering:** `gateway-svc` operates entirely diskless using reusable 4MB in-memory buffers (`sync.Pool`) with a bounded concurrency semaphore and HTTP/2 stream backpressure.

## 5. Write Path (PUT object)

1. Client streams object to `gateway-svc` (`Put(bucket, key, stream)`). Gateway checks if bucket exists via `metadata-svc`.
2. Gateway splits stream into chunks and computes SHA-256 hashes on the fly.
3. For each chunk: Gateway queries `metadata-svc` (`CheckChunks`) — if hash exists with `RefCount > 0`, skip write (dedup) and mark to increment `RefCount`.
4. Else: Gateway queries `metadata-svc` for active storage nodes (cached locally with 5-10s TTL), picks R=3 nodes weighted by available disk capacity, and writes chunk to all 3 nodes in parallel via `PutChunk` gRPC. **Quorum = Majority (W=2 of 3 nodes must ack)**. Gateway records successful nodes in `ChunkLocation.Nodes`.
5. Once all chunks placed, Gateway calls `metadata-svc` leader to propose `CommitManifest{bucket, key, chunk_ids, chunk_locations, size}` through Raft. The FSM atomically writes the manifest and registers/updates chunk locations in BadgerDB. Manifest is visible only after Raft-committed (majority ack).
6. Gateway returns success to client only after step 5 commits.

**Multipart:** client calls `InitiateMultipart(bucket, key) -> upload_id` on `gateway-svc`, uploads parts independently (`UploadPart(upload_id, part_num, stream)`). Each part is chunked/placed by Gateway to data-svc as above and recorded in `metadata-svc` as a `pending` sub-manifest. `CompleteMultipart(upload_id)` instructs `metadata-svc` to concatenate part chunk-lists in order into final `ObjectManifest`, proposing a single `CommitManifest` through Raft. Failure before `CompleteMultipart` → object never visible; orphaned chunks reclaimed by GC.

## 6. Read Path (GET object)

1. Client calls `gateway-svc` (`Get(bucket, key)`).
2. Gateway fetches `ObjectManifest` and `ChunkLocation` mappings for `(bucket, key)` from `metadata-svc` (leader-routed).
3. For each `ChunkID`, Gateway looks up `ChunkLocation.Nodes`, fetches from first reachable node (`GetChunk`), and verifies SHA-256 on read.
4. If a node fails or is missing the chunk, gateway fetches from the next replica and triggers an asynchronous read-repair to restore the missing replica.
5. Gateway streams concatenated chunks to client.

## 7. Delete Path

1. Client calls `gateway-svc` (`Delete(bucket, key)`). Gateway forwards `DeleteManifest{bucket, key}` to `metadata-svc`.
2. Proposed through Raft → FSM marks manifest `deleted`, decrements `RefCount` on each referenced chunk.
3. Chunk bytes on data-svc are **not** deleted synchronously — GC reclaims chunks with `RefCount == 0`.

## 8. Background Workers (Embedded on Metadata Leader)

The Raft leader in `metadata-svc` continuously runs embedded background worker tasks:

```
Leader Background Workers:
  1. Abort Incomplete Multiparts:
     - Scan pending multipart uploads where time.Since(CreatedAt) > 24h.
     - Propose AbortMultipart through Raft (decrements chunk RefCounts).
  2. Quarantine Garbage Collection:
     - Query local BadgerDB for ChunkLocation entries where RefCount == 0 AND time.Since(OrphanedAt) > 24h.
     - Stream rate-limited DeleteChunk RPCs to each node in ChunkLocation.Nodes (e.g. 50 chunks/sec).
     - Propose RemoveChunkLocations through Raft to drop metadata entries.
  3. Active Replica Healing:
     - Query local BadgerDB for under-replicated chunks where len(ChunkLocation.Nodes) < 3.
     - Pick an available healthy target node from the active heartbeat registry.
     - Instruct an existing replica node to stream the chunk to the target node via ReplicateChunk RPC.
     - Propose UpdateChunkLocation through Raft once replication completes.
```

- **Quarantine Safety:** The 24-hour quarantine window completely eliminates the race condition where concurrent uploads deduplicate against a chunk slated for deletion. If a write dedups against it within 24 hours, `RefCount` increments back to $\ge 1$ and `OrphanedAt` is cleared.
- **Zero External Schedulers:** Raft leader election ensures exactly one node coordinates GC and healing; stepping down immediately halts workers.

## 9. Replication & Consistency Guarantees

| Data | Mechanism | Consistency |
|---|---|---|
| Metadata (buckets, manifests, chunk locations) | Raft (majority quorum) | Leader-local read from BadgerDB (steps down on heartbeat loss) |
| Chunk bytes | Synchronous write with majority quorum (W=2 of 3) | Quorum ack before manifest commit; 3rd replica healed asynchronously |
| GC & Healing | Embedded leader workers | Safe & continuous (24h quarantine; rate-limited trickle healing) |

**Failure handling:**
- gateway-svc node down → client retries against another gateway instance (gateway is stateless; in-flight uncommitted chunks reclaimed by GC).
- data-svc node down during write → write succeeds if 2 nodes ack; missing replica is automatically healed in background by the leader worker.
- data-svc node down during read → gateway retries next node in `ChunkLocation.Nodes` and triggers passive read-repair.
- metadata-svc leader dies → Raft re-elects; gateway retries against new leader; in-flight uncommitted writes fail client-side, safe to retry (idempotent via chunk hash + manifest key).

## 10. API Surface

Castor exposes an S3-compatible REST interface on `gateway-svc` (port `:9000`) for clients, and uses strictly typed internal gRPC between backend services:

### 10.1. S3-Compatible REST Interface (`gateway-svc` HTTP `:9000`)

* **Addressing Style:** Path-style (`http://localhost:9000/<bucket>/<key>`).
* **Authentication:** AWS Signature Version 4 (SigV4) verification (`AWS4-HMAC-SHA256`) with static cluster credentials; also supports `--no-sign-request` / unsigned payloads.
* **Payload Streaming:** Transparent `aws-chunked` payload decoding streaming directly into 4MB memory buffers (`sync.Pool`).
* **Supported S3 Operations (Strict & Exhaustive):**
  Castor supports **only** the core storage and multipart operations below. Any unlisted S3 APIs return `501 Not Implemented` or `400 InvalidArgument`.

  | Category | High-Level AWS CLI Command | Low-Level S3 REST Wire Action | Description |
  |---|---|---|---|
  | **Buckets** | `aws s3 mb s3://<bucket>` | `PUT /<bucket>` (`CreateBucket`) | Creates a bucket namespace |
  | **Buckets** | `aws s3 rb s3://<bucket>` | `DELETE /<bucket>` (`DeleteBucket`) | Deletes an empty bucket |
  | **Buckets** | `aws s3 ls` | `GET /` (`ListBuckets`) | Lists all buckets |
  | **Buckets** | `aws s3api head-bucket` | `HEAD /<bucket>` (`HeadBucket`) | Checks bucket existence and access |
  | **Objects** | `aws s3 cp <file> s3://...` | `PUT /<bucket>/<key>` (`PutObject`) | Persists single/chunked object |
  | **Objects** | `aws s3 cp s3://... <file>` | `GET /<bucket>/<key>` (`GetObject`) | Streams object bytes (supports `Range`) |
  | **Objects** | `aws s3api head-object` | `HEAD /<bucket>/<key>` (`HeadObject`) | Returns metadata without payload |
  | **Objects** | `aws s3 rm s3://...` | `DELETE /<bucket>/<key>` (`DeleteObject`) | Marks deleted, decrements refcounts |
  | **Objects** | `aws s3 ls s3://<bucket>/` | `GET /<bucket>?list-type=2` (`ListObjectsV2`) | Lists keys with delimiter & prefix filtering |
  | **Objects** | `aws s3 sync ...` | Composite: `ListObjectsV2` + `PutObject`/`GetObject` | Syncs directories based on ETag & mtime |
  | **Multipart** | `aws s3 cp` (large files) | `POST /<bucket>/<key>?uploads` (`InitiateMultipart`) | Begins multi-part upload session |
  | **Multipart** | `aws s3 cp` (large files) | `PUT /...?uploadId=...&partNumber=...` (`UploadPart`) | Uploads an individual 5MB+ part |
  | **Multipart** | `aws s3 cp` (large files) | `POST /...?uploadId=...` (`CompleteMultipart`) | Commits and concatenates all parts |
  | **Multipart** | `aws s3api abort-...` | `DELETE /...?uploadId=...` (`AbortMultipart`) | Discards uncommitted parts |

* **Wire Protocol Compliance:** Full XML responses (`<ListBucketResult>`, `<Error>`), standard HTTP status codes, and quoted `ETag` integrity checksums.

### 10.2. Internal Service gRPC API

```protobuf
// Internal metadata API served by metadata-svc (Raft group :9091-:9093)
service MetadataService {
  // Bucket management (Raft replicated)
  rpc CreateBucket(CreateBucketMetadataRequest) returns (CreateBucketMetadataResponse);
  rpc DeleteBucket(DeleteBucketMetadataRequest) returns (DeleteBucketMetadataResponse);
  rpc ListBuckets(ListBucketsMetadataRequest) returns (ListBucketsMetadataResponse);
  rpc CheckBucketExists(CheckBucketExistsRequest) returns (CheckBucketExistsResponse);

  // Manifest & Chunk operations (Raft replicated)
  rpc CheckChunks(CheckChunksRequest) returns (CheckChunksResponse);
  rpc CommitManifest(CommitManifestRequest) returns (CommitManifestResponse);
  rpc GetManifest(GetManifestRequest) returns (GetManifestResponse);
  rpc DeleteManifest(DeleteManifestRequest) returns (DeleteManifestResponse);
  rpc ListManifests(ListManifestsRequest) returns (ListManifestsResponse);

  // Multipart uploads (Raft replicated)
  rpc InitiateMultipart(InitiateMultipartMetaRequest) returns (InitiateMultipartMetaResponse);
  rpc CommitPart(CommitPartMetaRequest) returns (CommitPartMetaResponse);
  rpc CompleteMultipart(CompleteMultipartMetaRequest) returns (CommitManifestResponse);
  rpc AbortMultipart(AbortMultipartMetaRequest) returns (AbortMultipartMetaResponse);

  // Dynamic Heartbeat Registry (Ephemeral, in-memory, bypasses Raft)
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc GetActiveNodes(GetActiveNodesRequest) returns (GetActiveNodesResponse);

  // Leader-only background maintenance & repair
  rpc UpdateChunkLocation(UpdateChunkLocationRequest) returns (UpdateChunkLocationResponse);
  rpc RemoveChunkLocations(RemoveChunkLocationsRequest) returns (RemoveChunkLocationsResponse);
  rpc TriggerReadRepair(TriggerReadRepairRequest) returns (TriggerReadRepairResponse);
}

// Internal chunk storage API served by data-svc (:9101-:9103)
service DataService {
  // Streaming chunk ingest (staging -> rename)
  rpc PutChunk(stream PutChunkRequest) returns (PutChunkResponse);

  // Streaming chunk retrieval
  rpc GetChunk(GetChunkRequest) returns (stream GetChunkResponse);

  // Chunk deletion (called by GC worker)
  rpc DeleteChunk(DeleteChunkRequest) returns (DeleteChunkResponse);

  // Peer-to-peer chunk copy (called by healing worker)
  rpc ReplicateChunk(ReplicateChunkRequest) returns (ReplicateChunkResponse);

  // Local node health & disk status
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

## 11. Build Order (maps to 4-week plan)

1. `metadata-svc`: integrate `hashicorp/raft` consensus with BadgerDB-backed FSM for metadata (manifests, chunk locations, heartbeat registry).
2. `data-svc`: PutChunk/GetChunk/DeleteChunk/ReplicateChunk against local disk, node registration & heartbeat loop.
3. `gateway-svc`: core storage engine (stream chunking, SHA-256 verification, W=2 placement coordination, metadata calls), integrated S3 REST frontend (`versitygw` on `:9000`).
4. Multipart uploads, embedded leader workers (multipart cleanup + quarantine GC + replica healing), HTTP admin/health endpoints (`/healthz`, `/metrics`), integration tests (kill/restart nodes, partition).

## 12. Explicit Non-Goals (document in README)

- **No AWS IAM, STS, ACLs, or dynamic bucket policy engine** — single-tenant/static SigV4 root credentials and path-style addressing only.
- **No Object Versioning** (`put-bucket-versioning`, `list-object-versions`) — all object overwrites strictly follow Last-Write-Wins (LWW).
- **No Object Tagging** (`put-object-tagging`, `get-object-tagging`).
- **No Server-Side Encryption / KMS** (`SSE-KMS`, SSE-C).
- **No Bucket Lifecycle & Retention Rules** (`put-bucket-lifecycle-configuration`) — disk reclamation is handled exclusively by the internal 24-hour quarantine GC.
- **No Static Website Hosting, CORS, or Event Notifications**.
- **No erasure coding / bit-rot scrubbing** — SHA-256 verify-on-read only ($R=3$ replication).
- **No cross-datacenter multi-region replication** — single-cluster focus.
- **No dynamic cluster membership changes in Raft** — static 3-node metadata cluster.
- **No follower reads / ReadIndex optimization** — all reads leader-routed.

## 13. Performance & Benchmark Targets (Milestones)

These target metrics serve as practical milestones for development and validation on standard developer hardware / cloud instances (e.g. 8 vCPUs, 16GB RAM, NVMe SSD, local loopback or 10GbE networking, `storage.sync_writes: true`). These represent **solid, production-grade benchmarks** achievable with clean Go code and proper concurrency, rather than theoretical micro-benchmarks.

### 13.1. Large Object Streaming (100MB – 1GB Objects, 4MB Chunks)

| Operation | Concurrency | Target Throughput | p95 Latency (TTFB) | Primary Bottleneck / Limiter |
|---|---|---|---|---|
| **Sequential PUT** (R=3, W=2) | 1 stream | **150 – 250 MB/s** | < 25 ms | SHA-256 calculation + disk `fsync` on 2 data nodes |
| **Concurrent PUT** (R=3, W=2) | 8 – 16 streams | **400 – 650 MB/s** | < 45 ms | NVMe write bandwidth & CPU hashing concurrency |
| **Sequential GET** (Verify on read) | 1 stream | **250 – 400 MB/s** | < 15 ms | Disk sequential read + SHA-256 verify + gRPC stream |
| **Concurrent GET** (Verify on read) | 8 – 16 streams | **600 – 1,000 MB/s** | < 25 ms | NVMe read bandwidth & gRPC HTTP/2 framing |
| **Deduplicated PUT** (Chunk exists) | 8 streams | **1.0 – 1.8 GB/s** | < 10 ms | CPU SHA-256 throughput (disk writes bypassed) |

---

### 13.2. Small Object High-Throughput (64KB – 1MB Objects)

| Operation | Concurrency | Target Request Rate (RPS) | p95 Latency | Primary Bottleneck / Limiter |
|---|---|---|---|---|
| **Small PUT** (Single chunk, W=2) | 32 clients | **1,000 – 2,500 ops/s** | 12 – 25 ms | Raft BadgerDB WAL append batching + disk sync |
| **Small GET** (Single chunk) | 32 clients | **3,000 – 6,000 ops/s** | 4 – 10 ms | Leader BadgerDB lookup + single disk chunk read |
| **Object Deletion** (`DeleteManifest`) | 16 clients | **2,000 – 4,000 ops/s** | 5 – 15 ms | Raft log consensus (async chunk GC) |

---

### 13.3. Metadata Consensus & State Machine (`metadata-svc`)

| Operation | Target Performance | Notes & Boundaries |
|---|---|---|
| **Raft Commit Rate** (`CommitManifest`) | **1,500 – 3,500 commits/s** | Sustained Raft proposals with BadgerDB Raft WAL fsync batching. |
| **Metadata Read Rate** (`GetManifest`) | **12,000 – 25,000 queries/s** | Served directly from leader's in-memory / BadgerDB cache (< 1.5ms p95). |
| **Bucket Range Scan** (`ListObjects`, 1k keys) | **500 – 1,200 scans/s** | BadgerDB LSM sequential key prefix iteration. |

---

### 13.4. Degraded Mode & Failure Recovery Milestones

| Scenario | Target Metric / Tolerance | Expected System Behavior |
|---|---|---|
| **1 Data Node Down during Write** | $\le$ 15% throughput degradation | Writes continue uninterrupted since $W=2$ of 3 quorum is satisfied. |
| **1 Data Node Down during Read** | $\le$ 20ms failover penalty | Gateway catches failure on node 1, fetches from node 2, triggers read-repair. |
| **Raft Leader Crash & Failover** | **< 500 – 800 ms** election window | In-flight writes retry; new leader serves reads within < 1 second. |
| **Active Replica Healing Worker** | **20 – 50 chunks/s (80 – 200 MB/s)** | Trickle peer-to-peer copies without starving foreground client uploads. |
| **Quarantine GC Worker** | **50 – 100 chunks/s reclaimed** | Rate-limited deletion RPCs preventing storage I/O starvation. |

