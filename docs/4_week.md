# Castor — 4-Week Implementation Plan

---

## Topology
- **`gateway-svc`**: Stateless S3 REST frontend (`:9000` via embedded `versitygw`).
- **`metadata-svc`**: 3-node Raft consensus cluster backed by BadgerDB (`/raft/` WAL, `/state/` FSM).
- **`data-svc`**: Raw 4MB chunk store on local disk (`/data/chunks/xx/<sha256>`).

---

## Week 1: Core Storage (`data-svc`) & Consensus (`metadata-svc`)
- [ ] **Tooling & Logging:** Compile `proto/castor/v1/*.proto`; set up structured JSON logging (`log/slog`) and env-var configs.
- [ ] **`data-svc` Engine:**
  - Two-phase disk writes: `/data/staging/<uuid>.tmp` $\rightarrow$ `file.Sync()` $\rightarrow$ atomic `os.Rename` to `/data/chunks/xx/<sha256>`.
  - gRPC `DataService`: `PutChunk` (stream + verify SHA-256), `GetChunk`, `DeleteChunk`, `ReplicateChunk` (P2P copy), `HealthCheck`.
  - 3-second heartbeat loop reporting capacity telemetry to `metadata-svc`.
- [ ] **`metadata-svc` Consensus:**
  - Dual BadgerDB instances: `/data/badger/raft/` (`SyncWrites: true` for WAL) and `/data/badger/state/` (FSM).
  - 3-node `hashicorp/raft` bootstrap with custom FSM (`Apply`, `Snapshot`, `Restore`).
  - In-memory heartbeat registry (Raft bypass).
  - gRPC `MetadataService`: `CheckBucketExists`, bucket CRUD, `CheckChunks` (dedup), atomic `CommitManifest`, `GetManifest`, `DeleteManifest`.
- [ ] **Tests:** Unit tests for staging crash recovery & checksum mismatch; integration tests for 3-node Raft election and log replication.

**Milestone 1:** Storage nodes write chunks with atomic fsync; 3-node Raft cluster commits and replicates metadata.

---

## Week 2: Stateless Gateway & S3 REST Engine (`gateway-svc`)
- [ ] **Ingress Engine:** Memory-bounded 4MB buffer pool (`sync.Pool`) with concurrency semaphore.
- [ ] **Placement & Quorum:**
  - Active node cache (5s TTL) from `metadata-svc.GetActiveNodes`.
  - Capacity-weighted node selection; parallel fan-out write with majority quorum ($W=2$ of 3).
  - Inline SHA-256 deduplication (bypass existing chunks).
- [ ] **Read Pipeline:** Retrieve manifest, stream chunks sequentially from first responsive replica, verify SHA-256 on ingress.
- [ ] **S3 REST via `versitygw` (`:9000`):**
  - Implement `versitygw.Backend`: `CreateBucket`, `DeleteBucket`, `ListBuckets`, `HeadBucket`, `PutObject`, `GetObject` (with `Range`), `HeadObject`, `DeleteObject`, `ListObjectsV2`.
  - Path-style routing, SigV4 auth, unsigned payload support (`--no-sign-request`).
- [ ] **Observability & Lifecycle:** Prometheus `/metrics`, `/healthz` endpoints, graceful shutdown on `SIGTERM`.
- [ ] **Tests:** Automated AWS CLI integration suite (`aws s3 mb`, `rb`, `cp`, `ls`, `rm`, `sync`).

**Milestone 2:** Fully functional local S3 store. Passes all core AWS CLI operations with live metrics.

---

## Week 3: Multipart Uploads & Self-Healing Lifecycle Workers
- [ ] **S3 Multipart Pipeline:**
  - `InitiateMultipartUpload` (issue `UploadId`).
  - `UploadPart` (stream 5MB+ parts, slice into 4MB chunks, persist part records).
  - `CompleteMultipartUpload` (atomic concatenation into final manifest via single Raft commit).
  - `AbortMultipartUpload` (discard uncommitted parts, decrement chunk refcounts).
- [ ] **Leader-Only Background Workers (`metadata-svc`):**
  - **Quarantine GC:** Scan `RefCount == 0` AND `time > 24h` $\rightarrow$ rate-limited `DeleteChunk` (50/s) $\rightarrow$ propose `RemoveChunkLocations`.
  - **Active Replica Healer:** Scan `len(Nodes) < 3` $\rightarrow$ issue `ReplicateChunk` for P2P copy $\rightarrow$ update metadata via Raft.
  - **Multipart Cleanup:** Auto-abort pending multipart uploads older than 24 hours.
- [ ] **Passive Read-Repair:** Gateway detects bad checksum or unreachable node during download $\rightarrow$ fails over to replica $\rightarrow$ triggers async repair.
- [ ] **Admin API:** Expose `/admin/nodes`, `/admin/raft/status`, and `POST /admin/gc?dry_run=true`.

**Milestone 3:** Resilient, self-healing cluster supporting multi-gigabyte multipart uploads and automated 24h quarantine GC.

---

## Week 4: Cloud Kubernetes (GKE) Deployment & Chaos Testing
- [ ] **Containerization:** Multi-stage distroless `Dockerfile`s (`CGO_ENABLED=0`, non-root user) for all 3 services. Turnkey `docker-compose.yml` for local testing.
- [ ] **Kubernetes Manifests (GKE / Cloud K8s):**
  - **`gateway-svc` (Deployment):** Stateless, HPA autoscaling, Cloud L4 Network Load Balancer (NLB) on port `:9000`.
  - **`metadata-svc` (StatefulSet):** 3 replicas, Headless Service for Raft DNS (`meta-0.meta-svc`), NVMe/SSD PVCs (`/data/badger`), `topologySpreadConstraints` across 3 Availability Zones.
  - **`data-svc` (StatefulSet):** 3+ replicas, dedicated PVCs (`/data/chunks`).
  - ConfigMaps, Secrets, `livenessProbe` & `readinessProbe` on `/healthz`.
- [ ] **Chaos & Failure Testing (on GKE):**
  - **Node Loss:** Kill a `data-svc` pod during active writes; verify $W=2$ quorum holds and healer restores $R=3$.
  - **Leader Crash:** Kill `metadata-svc` leader pod; verify new leader election in $<500$ms with zero data loss.
  - **Bit-Rot:** Corrupt on-disk chunk byte; verify `GetObject` detects hash mismatch, serves from replica, and triggers repair.
- [ ] **Public Validation:** Benchmark and validate public S3 ingress via AWS CLI from external networks.

**Milestone 4:** Live, production-grade distributed object store running on Cloud Kubernetes (GKE), accessible over public S3, fully monitored and chaos-tested.

---

## Weekly Workload Summary

| Week | Focus | Core Deliverable |
|---|---|---|
| **Week 1** | **Storage & Consensus** | `data-svc` atomic engine + `metadata-svc` 3-node Raft cluster on BadgerDB. |
| **Week 2** | **Gateway & S3 API** | 4MB streaming chunker + `versitygw` S3 REST frontend + AWS CLI test suite. |
| **Week 3** | **Multipart & Self-Healing** | S3 multipart upload + leader GC worker (24h quarantine) + active replica healer. |
| **Week 4** | **Cloud K8s & Chaos** | Distroless containers + GKE StatefulSets/NLB deployment + live chaos testing. |
