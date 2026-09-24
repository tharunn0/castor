# Castor — 4-Week Implementation Plan

---

## Topology
- **`gateway-svc`**: Stateless dual-port front door: S3 REST frontend (`:9000` via embedded `versitygw`) + Embedded Web Console & BFF (`:9001` via Go `embed.FS`).
- **`auth-svc`**: Identity and credential provider (`:9095`) backed by PostgreSQL or SQLite (`users` and `s3_credentials`).
- **`metadata-svc`**: 3-node Raft consensus cluster backed by `raft-boltdb` (`/data/raft/raft.db` WAL/stable store) and BadgerDB (`/data/badger/state/` FSM with user attribution `owner_id`).
- **`data-svc`**: Raw 4MB chunk store on local disk (`/data/chunks/xx/<sha256>`).

---

## Week 1: Core Storage (`data-svc`) & Consensus (`metadata-svc`)
- [ ] **Tooling & Logging:** Compile `proto/castor/v1/*.proto`; set up structured JSON logging (`log/slog`) and env-var configs.
- [ ] **`data-svc` Engine:**
  - Two-phase disk writes: `/data/staging/<uuid>.tmp` $\rightarrow$ `file.Sync()` $\rightarrow$ atomic `os.Rename` to `/data/chunks/xx/<sha256>`.
  - gRPC `DataService`: `PutChunk` (stream + verify SHA-256), `GetChunk`, `DeleteChunk`, `ReplicateChunk` (P2P copy), `HealthCheck`.
  - 3-second heartbeat loop reporting capacity telemetry to `metadata-svc`.
- [ ] **`metadata-svc` Consensus:**
  - Storage engines: `raft-boltdb` for consensus WAL & stable store (`/data/raft/raft.db`) and BadgerDB for replicated FSM (`/data/badger/state/`).
  - 3-node `hashicorp/raft` bootstrap with custom FSM (`Apply`, `Snapshot`, `Restore`).
  - In-memory heartbeat registry (Raft bypass).
  - gRPC `MetadataService`: `CheckBucketExists`, bucket CRUD (with `owner_id`), `CheckChunks` (dedup), atomic `CommitManifest`, `GetManifest`, `DeleteManifest`.
- [ ] **Tests:** Unit tests for staging crash recovery & checksum mismatch; integration tests for 3-node Raft election and log replication.

**Milestone 1:** Storage nodes write chunks with atomic fsync; 3-node Raft cluster commits and replicates metadata.

---

## Week 2: Stateless Gateway, Auth Service & S3 REST Engine
- [ ] **Ingress Engine:** Memory-bounded 4MB buffer pool (`sync.Pool`) with concurrency semaphore.
- [ ] **Placement & Quorum:**
  - Active node cache (5s TTL) from `metadata-svc.GetActiveNodes`.
  - Capacity-weighted node selection; parallel fan-out write with majority quorum ($W=2$ of 3).
  - Inline SHA-256 deduplication (bypass existing chunks).
- [ ] **Identity & Auth Service (`auth-svc`):**
  - PostgreSQL / SQLite schema: `users` (with `bcrypt` passwords) and `s3_credentials` (`access_key_id` / `secret_access_key`).
  - REST endpoints: `POST /auth/register`, `POST /auth/login` (JWT), `POST /auth/keys`, `GET /internal/validate-key`.
- [ ] **S3 REST via `versitygw` (`:9000`):**
  - Implement `versitygw.Backend`: `CreateBucket`, `DeleteBucket`, `ListBuckets`, `HeadBucket`, `PutObject`, `GetObject` (with `Range`), `HeadObject`, `DeleteObject`, `ListObjectsV2`.
  - Dynamic SigV4 credential validation bridging into `auth-svc` with in-memory LRU cache (30s TTL).
  - Path-style routing, unsigned payload support (`--no-sign-request`).
- [ ] **Observability & Lifecycle:** Prometheus `/metrics`, `/healthz` endpoints, graceful shutdown on `SIGTERM`.
- [ ] **Tests:** Automated AWS CLI integration suite (`aws s3 mb`, `rb`, `cp`, `ls`, `rm`, `sync`).

**Milestone 2:** Fully functional local S3 store. Multi-user S3 credentials validated from PostgreSQL/SQLite with live metrics and AWS CLI compatibility.

---

## Week 3: Multipart Uploads, Self-Healing & Embedded Web Console
- [ ] **S3 Multipart Pipeline:**
  - `InitiateMultipartUpload` (issue `UploadId`).
  - `UploadPart` (stream 5MB+ parts, slice into 4MB chunks, persist part records).
  - `CompleteMultipartUpload` (atomic concatenation into final manifest via single Raft commit).
  - `AbortMultipartUpload` (discard uncommitted parts, decrement chunk refcounts).
- [ ] **Leader-Only Background Workers (`metadata-svc`):**
  - **Quarantine GC:** Scan `RefCount == 0` AND `time > 24h` $\rightarrow$ rate-limited `DeleteChunk` (50/s) $\rightarrow$ propose `RemoveChunkLocations`.
  - **Active Replica Healer:** Scan `len(Nodes) < 3` $\rightarrow$ issue `ReplicateChunk` for P2P copy $\rightarrow$ update metadata via Raft.
  - **Multipart Cleanup:** Auto-abort pending multipart uploads older than 24 hours.
- [ ] **Embedded Web Console & BFF (`gateway-svc :9001`):**
  - AI-generated modern SPA embedded via Go `//go:embed dist/*`.
  - User login & JWT session handling (`/api/auth/*`).
  - S3 Access Key management view (one-click generate & copy snippet for AWS CLI).
  - Bucket & file explorer with in-process chunked upload bridge (`POST /api/files/upload`).
  - Live cluster health dashboard querying `/admin/raft/status` and `/admin/nodes`.
- [ ] **Passive Read-Repair:** Gateway detects bad checksum or unreachable node during download $\rightarrow$ fails over to replica $\rightarrow$ triggers async repair.
- [ ] **Admin API:** Expose `/admin/nodes`, `/admin/raft/status`, and `POST /admin/gc?dry_run=true`.

**Milestone 3:** Resilient, self-healing cluster with an interactive embedded Web Console, multi-gigabyte multipart uploads, and automated 24h quarantine GC.

---

## Week 4: Cloud Kubernetes (GKE) Deployment & Chaos Testing
- [ ] **Containerization:** Multi-stage distroless `Dockerfile`s (`CGO_ENABLED=0`, non-root user) for all services. Turnkey `docker-compose.yml` including PostgreSQL Auth DB.
- [ ] **Kubernetes Manifests (GKE / Cloud K8s):**
  - **`gateway-svc` (Deployment):** Stateless, HPA autoscaling, Cloud L4 Network Load Balancer (NLB) on ports `:9000` (S3) and `:9001` (Console).
  - **`auth-svc` (Deployment):** Connected to Cloud SQL PostgreSQL or stateful PVC.
  - **`metadata-svc` (StatefulSet):** 3 replicas, Headless Service for Raft DNS (`meta-0.meta-svc`), NVMe/SSD PVCs (`/data/badger`), `topologySpreadConstraints` across 3 Availability Zones.
  - **`data-svc` (StatefulSet):** 3+ replicas, dedicated PVCs (`/data/chunks`).
  - ConfigMaps, Secrets, `livenessProbe` & `readinessProbe` on `/healthz`.
- [ ] **Chaos & Failure Testing (on GKE):**
  - **Node Loss:** Kill a `data-svc` pod during active writes; verify $W=2$ quorum holds and healer restores $R=3$.
  - **Leader Crash:** Kill `metadata-svc` leader pod; verify new leader election in $<500$ms with zero data loss.
  - **Bit-Rot:** Corrupt on-disk chunk byte; verify `GetObject` detects hash mismatch, serves from replica, and triggers repair.
- [ ] **Public Validation:** Benchmark and validate public S3 ingress via AWS CLI and Web Console from external networks.

**Milestone 4:** Live, production-grade distributed object store running on Cloud Kubernetes (GKE), accessible over public S3 and Web Console, fully monitored and chaos-tested.

---

## Weekly Workload Summary

| Week | Focus | Core Deliverable |
|---|---|---|
| **Week 1** | **Storage & Consensus** | `data-svc` atomic engine + `metadata-svc` 3-node Raft cluster on BadgerDB. |
| **Week 2** | **Gateway, Auth & S3 API** | 4MB streaming chunker + `auth-svc` (PostgreSQL/SQLite) + `versitygw` S3 REST frontend with dynamic SigV4 validation. |
| **Week 3** | **Multipart, Self-Healing & Console** | S3 multipart upload + leader GC worker + embedded Web Console UI on `:9001`. |
| **Week 4** | **Cloud K8s & Chaos** | Distroless containers + GKE StatefulSets/NLB deployment + live chaos testing. |
