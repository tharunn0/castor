# Castor — 4-Week Implementation Plan

---

### Topology
- **`gateway-svc`**: Stateless dual-port front door: S3 REST frontend (`:9000` via embedded `versitygw`) + Browser JSON BFF API (`:9001` with CORS support for standalone UI).
- **`castor-ui`**: Standalone, lightweight operator SPA (React/Vite/Tailwind) deployed independently, communicating via BFF API on `:9001` or unified via Ingress.
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

## Week 2: Auth Service, Docker Containerization, Advanced Ingress & Console MVP
- [ ] **Identity & Auth Service (`auth-svc :9095`):**
  - PostgreSQL / SQLite schema: `users` (with `bcrypt` passwords) and `s3_credentials` (`access_key_id` / `secret_access_key`).
  - REST endpoints: `POST /auth/register`, `POST /auth/login` (JWT), `POST /auth/keys`, `DELETE /auth/keys/{id}`, `GET /internal/validate-key`.
- [ ] **Dynamic SigV4 Credential Validation:**
  - Bridge `gateway-svc` into `auth-svc` with in-memory LRU credential cache (30s TTL).
  - Multi-user isolation: Link buckets and manifests to `owner_id` (User UUID).
- [ ] **Ingress Enhancements & Presigned URLs:**
  - `ListObjectsV2` prefix and delimiter (`/`) folder hierarchy support.
  - `Range` GET request support (partial content / streaming).
  - Stateless AWS SigV4 presigned URLs (`POST /api/files/presign` on `:9001`).
- [ ] **Docker Containerization & Local Cluster (`docker-compose.yml`):**
  - Multi-stage distroless `Dockerfile`s (`CGO_ENABLED=0`, non-root) for `gateway-svc`, `auth-svc`, `metadata-svc`, and `data-svc`.
  - Turnkey local `docker-compose.yml` orchestrating PostgreSQL, `auth-svc`, `gateway-svc`, 3 `metadata-svc` nodes, and 3 `data-svc` nodes.
  - Verifies multi-container networking and disk volumes locally before touching the cloud.
- [ ] **Console MVP (`castor-ui` v1 & Headless BFF):**
  - Headless BFF setup on `gateway-svc :9001` with CORS and JWT cookie/token support.
  - Setup minimal `castor-ui` SPA (React + Vite + Tailwind):
    - **Login Screen**: Clean email/password authentication.
    - **Access Key Manager**: One-click S3 keypair generation with copy-to-clipboard `aws configure` snippet.
- [ ] **Observability & Lifecycle:** Prometheus `/metrics`, `/healthz` endpoints, graceful shutdown on `SIGTERM`.
- [ ] **Tests:** Multi-user isolation test suite and automated AWS CLI test with rotated credentials.

**Milestone 2:** Multi-user object store with dynamic SigV4 validation, PostgreSQL auth backend, complete local Docker Compose cluster, HTTP Range/ListObjectsV2 support, and a working Web Console MVP for login and S3 key management.

---

## Week 3: Multipart Uploads, Deduplication, Self-Healing & Console Explorer
- [ ] **S3 Multipart Pipeline:**
  - `InitiateMultipartUpload` (issue `UploadId`).
  - `UploadPart` (stream 5MB+ parts, slice into 4MB chunks, persist part records).
  - `CompleteMultipartUpload` (atomic concatenation into final manifest via single Raft commit).
  - `AbortMultipartUpload` (discard uncommitted parts, decrement chunk refcounts).
- [ ] **Storage Engine Optimizations:**
  - Inline SHA-256 deduplication: Query `metadata-svc.CheckChunks` to bypass writing duplicate chunks across uploads.
  - Dynamic capacity-weighted node selection from active heartbeat registry cache.
  - P2P chunk replication (`ReplicateChunk`) on `data-svc`.
- [ ] **Leader-Only Background Workers (`metadata-svc`):**
  - **Quarantine GC:** Scan `RefCount == 0` AND `time > 24h` $\rightarrow$ rate-limited `DeleteChunk` (50/s) $\rightarrow$ propose `RemoveChunkLocations`.
  - **Active Replica Healer:** Scan `len(Nodes) < 3` $\rightarrow$ issue `ReplicateChunk` for P2P copy $\rightarrow$ update metadata via Raft.
  - **Bit-Rot Scrubber:** Full-cluster periodic integrity scan (7-day cycle, 10 chunks/sec rate limit). Issues `ScrubChunk` RPC to each holding `data-svc` node (local verify, no byte streaming). On SHA-256 mismatch: propose `CmdMarkChunkCorrupted` through Raft → trigger `TriggerReadRepair`. Progress tracked via `scrub_cursor` key in BadgerDB to survive leader failover.
  - **Multipart Cleanup:** Auto-abort pending multipart uploads older than 24 hours.
- [ ] **Web Console v2 (`castor-ui` Full Feature):**
  - **Bucket & File Explorer**: Browse folders, stream downloads, view file metadata, generate presigned sharing links, and drag-and-drop uploads via direct presigned URLs or in-process upload bridge (`POST /api/files/upload`).
  - **Live Cluster Health Dashboard**: Real-time visualization of Raft leader status, active/degraded storage nodes, disk capacity utilization, and scrubber progress.
- [ ] **Passive Read-Repair & Admin API:**
  - Gateway detects bad checksum or unreachable node during download $\rightarrow$ fails over to replica $\rightarrow$ triggers async repair.
  - Admin endpoints: `/admin/nodes`, `/admin/raft/status`, `POST /admin/gc?dry_run=true`.

**Milestone 3:** Resilient, self-healing cluster with multi-gigabyte multipart uploads, inline deduplication, automated 24h quarantine GC, bit-rot scrubbing with auto-repair, and a full-featured operator Web Console.

---

## Week 4: Cloud Kubernetes (GKE) Deployment & Chaos Testing (Lightweight Ops Sprint)
*(Zero new feature code written in Week 4. Dedicated exclusively to learning GCP, deploying to GKE, and running verification tests.)*
- [ ] **GCP & Cloud Onboarding:**
  - Set up GCP Project, configure `gcloud` CLI, VPC networking, and cloud IAM credentials.
  - Provision a 3-node Google Kubernetes Engine (GKE) cluster across 3 Availability Zones.
- [ ] **Kubernetes Manifests (GKE Deployment):**
  - **`gateway-svc` (Deployment):** Stateless, HPA autoscaling, Cloud L4 Network Load Balancer (NLB) on ports `:9000` (S3) and `:9001` (BFF API).
  - **`castor-ui` (Deployment):** Lightweight Nginx container (~15MB) with Ingress path routing (`/*` to UI, `/api/*` to `gateway-svc:9001`).
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
| **Week 2** | **Auth, Docker, Ingress & Console MVP** | `auth-svc` (PostgreSQL/SQLite) + dynamic SigV4 validation + `Range`/`ListObjectsV2` + local `docker-compose` cluster + `castor-ui` MVP (Login & S3 Keys). |
| **Week 3** | **Multipart, Dedup, Resilience & Full UI** | S3 multipart uploads + inline dedup + leader GC & bit-rot scrubber + `castor-ui` v2 (Bucket/Object Explorer & Cluster Health Dashboard). |
| **Week 4** | **Cloud K8s & Chaos (Lightweight Ops Sprint)** | GCP/GKE onboarding + translate Compose to GKE StatefulSets/Deployments/Ingress + live chaos tests (zero new application code). |
