# Castor — 4-Week Implementation Plan

---

## Topology
- **`gateway-svc`**: Stateless dual-port front door: S3 REST frontend (`:9000` via embedded `versitygw`) + Embedded Web Console & BFF (`:9001` via Go `embed.FS`).
- **`auth-svc`**: Identity and credential provider (`:9095`) backed by PostgreSQL or SQLite (`users` and `s3_credentials`).
- **`metadata-svc`**: 3-node Raft consensus cluster backed by `raft-boltdb` (`/data/raft/raft.db` WAL/stable store) and BadgerDB (`/data/badger/state/` FSM with user attribution `owner_id`).
- **`data-svc`**: Raw 4MB chunk store on local disk (`/data/chunks/xx/<sha256>`).

---

## Week 1: Core Storage, Consensus & Gateway MVP (CLI Vertical Slice)
- [ ] **Tooling & Logging:** Compile `proto/castor/v1/*.proto`; set up structured JSON logging (`log/slog`) and env-var configs.
- [ ] **`data-svc` Engine:**
  - Two-phase disk writes: `/data/staging/<uuid>.tmp` $\rightarrow$ `file.Sync()` $\rightarrow$ atomic `os.Rename` to `/data/chunks/xx/<sha256>`.
  - gRPC `DataService`: `PutChunk` (stream + verify SHA-256), `GetChunk`, `DeleteChunk`, `HealthCheck`.
  - 3-second heartbeat loop reporting capacity telemetry to `metadata-svc`.
- [ ] **`metadata-svc` Consensus:**
  - Storage engines: `raft-boltdb` for consensus WAL & stable store (`/data/raft/raft.db`) and BadgerDB for replicated FSM (`/data/badger/state/`).
  - 3-node `hashicorp/raft` bootstrap with custom FSM (`Apply`, `Snapshot`, `Restore`).
  - In-memory heartbeat registry (Raft bypass).
  - gRPC `MetadataService`: `CreateBucket`, `DeleteBucket`, `ListBuckets`, `CheckBucketExists`, atomic `CommitManifest`, `GetManifest`, `DeleteManifest`.
- [ ] **`gateway-svc` S3 REST MVP (`:9000` via `versitygw`):**
  - Memory-bounded 4MB streaming chunker (`sync.Pool`) with concurrency semaphore.
  - Placement & Quorum: Fixed/active node dispatch with majority write quorum ($W=2$ of 3).
  - Implement basic `versitygw.Backend`:
    - `CreateBucket` (`aws s3 mb`)
    - `DeleteBucket` (`aws s3 rb`)
    - `ListBuckets` (`aws s3 ls`)
    - `PutObject` / Overwrite (`aws s3 cp <local> s3://<bucket>/<key>`)
    - `GetObject` (`aws s3 cp s3://<bucket>/<key> <local>`)
    - `DeleteObject` (`aws s3 rm s3://<bucket>/<key>`)
  - Static root credentials / unsigned request support (`--no-sign-request` or fixed bootstrap keypair e.g., `admin`/`admin123`).
- [ ] **CLI Validation & Tests:**
  - End-to-end AWS CLI integration tests: bucket creation, file upload, file download, key overwrite, file deletion, and bucket removal.
  - Integration tests for 3-node Raft election and log replication during active writes.

**Milestone 1:** Full end-to-end distributed S3 pipeline operational via CLI! All three core services (`gateway-svc`, `metadata-svc`, `data-svc`) are up. Basic bucket & object CRUD operations function via `aws s3` CLI commands.

---

## Week 2: Auth Service, Dynamic SigV4, Deduplication & Advanced Ingress
- [ ] **Identity & Auth Service (`auth-svc :9095`):**
  - PostgreSQL / SQLite schema: `users` (with `bcrypt` passwords) and `s3_credentials` (`access_key_id` / `secret_access_key`).
  - REST endpoints: `POST /auth/register`, `POST /auth/login` (JWT), `POST /auth/keys`, `DELETE /auth/keys/{id}`, `GET /internal/validate-key`.
- [ ] **Dynamic SigV4 Credential Validation:**
  - Bridge `gateway-svc` into `auth-svc` with in-memory LRU credential cache (30s TTL).
  - Multi-user isolation: Link buckets and manifests to `owner_id` (User UUID).
- [ ] **Ingress & Storage Optimizations:**
  - Inline SHA-256 deduplication: Query `metadata-svc.CheckChunks` to bypass writing duplicate chunks across uploads.
  - Dynamic capacity-weighted node selection from active heartbeat registry cache.
  - `ListObjectsV2` prefix and delimiter (`/`) folder hierarchy support.
  - `Range` GET request support (partial content / streaming).
  - P2P chunk replication (`ReplicateChunk`) on `data-svc`.
- [ ] **Observability & Lifecycle:** Prometheus `/metrics`, `/healthz` endpoints, graceful shutdown on `SIGTERM`.
- [ ] **Tests:** Multi-user isolation test suite and automated AWS CLI test with rotated credentials.

**Milestone 2:** Production-ready multi-user object store with dynamic SigV4 validation, PostgreSQL/SQLite auth backend, inline chunk deduplication, HTTP Range requests, and Prometheus metrics.

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
| **Week 1** | **Storage, Consensus & Gateway MVP (CLI Slice)** | 3 core services up (`gateway-svc`, `metadata-svc`, `data-svc`). Working `aws s3` CLI for bucket & object CRUD with static/bootstrap auth. |
| **Week 2** | **Dynamic Auth, Dedup & S3 Enhancements** | `auth-svc` (PostgreSQL/SQLite) + dynamic SigV4 validation + inline SHA-256 dedup + `ListObjectsV2` + `Range` GET + metrics. |
| **Week 3** | **Multipart, Self-Healing & Console** | S3 multipart upload + leader GC worker + embedded Web Console UI on `:9001`. |
| **Week 4** | **Cloud K8s & Chaos** | Distroless containers + GKE StatefulSets/NLB deployment + live chaos testing. |
