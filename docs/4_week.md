# Castor: 4-Week Plan

## Topology

| Service | Role |
|---|---|
| `gateway-svc` | Stateless. S3 REST `:9000` (embedded `versitygw`), admin API `:9001` (CORS) |
| `auth-svc` `:9095` | Users and S3 credentials (Postgres/SQLite) |
| `metadata-svc` | 3-node Raft. `raft-boltdb` (log/stable), BadgerDB (FSM, `owner_id`) |
| `data-svc` | 4MB chunk store: `/data/chunks/xx/<sha256>` |
| `castor-admin` | Admin dashboard (React/Vite/Tailwind) |

---

## Week 1: Core Storage, Consensus, Gateway

- [ ] **Foundations:** compile protos, `slog` JSON logging, env config
- [ ] **`data-svc`:**
  - Write to `/data/staging/<uuid>.tmp`, `fsync`, atomic `rename`
  - gRPC: `PutChunk` (stream, SHA-256 verify), `GetChunk`, `DeleteChunk`, `HealthCheck`
  - 3s heartbeat with capacity telemetry
- [ ] **`metadata-svc`:**
  - 3-node `hashicorp/raft`, custom FSM (`Apply`, `Snapshot`, `Restore`)
  - In-memory heartbeat registry (bypasses Raft)
  - gRPC: bucket CRUD, `CheckBucketExists`, `CommitManifest`, `GetManifest`, `DeleteManifest`
- [ ] **`gateway-svc` (`:9000`):**
  - Bounded 4MB streaming chunker (`sync.Pool` + semaphore)
  - Write quorum W=2 of 3
  - `versitygw.Backend`: bucket CRUD, Put/Get/DeleteObject
  - Static bootstrap credentials
- [ ] **Tests:** AWS CLI e2e (mb, cp, overwrite, rm, rb); Raft election and replication under writes

**M1:** Bucket/object CRUD via `aws s3` across all three core services.

---

## Week 2: Auth, Docker, Admin API

- [ ] **`auth-svc`:**
  - Tables: `users` (bcrypt), `s3_credentials`
  - `POST /auth/register`, `POST /auth/login` (JWT), `POST /auth/keys`, `DELETE /auth/keys/{id}`, `GET /internal/validate-key`
- [ ] **SigV4 auth:** gateway validates via `auth-svc` (LRU cache, 30s TTL); buckets/manifests scoped by `owner_id`
- [ ] **S3 features:** `ListObjectsV2` (prefix, `/` delimiter), `Range` GET, SigV4 presigned URLs
- [ ] **Admin API (`:9001`):** JWT + admin role; `/admin/nodes`, `/admin/raft/status`
- [ ] **Docker:** distroless multi-stage images (`CGO_ENABLED=0`, non-root); Compose with Postgres, auth, gateway, 3× metadata, 3× data
- [ ] **Ops:** `/metrics`, `/healthz`, graceful `SIGTERM`
- [ ] **Tests:** multi-user isolation; CLI with rotated credentials

**M2:** Multi-user store with dynamic SigV4, local Compose cluster, admin API.

---

## Week 3: Multipart, Dedup, Self-Healing, Dashboard

- [ ] **Multipart:** `Initiate`, `UploadPart` (5MB+, sliced to 4MB chunks), `Complete` (single Raft commit), `Abort` (decrement refcounts)
- [ ] **Storage:**
  - Inline SHA-256 dedup via `CheckChunks`
  - Capacity-weighted node selection
  - P2P `ReplicateChunk`
- [ ] **Leader-only workers:**
  - **GC:** `RefCount == 0` and >24h, `DeleteChunk` at 50/s, then `RemoveChunkLocations`
  - **Healer:** replicas <3, `ReplicateChunk`, update via Raft
  - **Multipart cleanup:** abort uploads >24h
- [ ] **Read-repair:** on checksum failure or node down, fail over to a replica and repair async
- [ ] **Admin API:** `POST /admin/gc?dry_run=true`, user and key management
- [ ] **`castor-admin`:**
  - Admin login
  - Cluster health: Raft leader/term, node status, disk usage
  - GC controls, worker status
  - User and key management

**M3:** Self-healing cluster with multipart, dedup, GC, read-repair, admin dashboard.

---

## Week 4: Kubernetes and Chaos Testing

*No new features; deploy and verify only.*

- [ ] **Cluster:** Kubernetes across 3 zones
- [ ] **Manifests:**
  - `gateway-svc`: Deployment + HPA, LoadBalancer on `:9000`/`:9001`
  - `castor-admin`: Nginx Deployment, Ingress (`/*` to UI, `/api/*` to `:9001`)
  - `auth-svc`: Deployment (managed Postgres or PVC)
  - `metadata-svc`: StatefulSet ×3, headless Service, SSD PVCs, zone `topologySpreadConstraints`
  - `data-svc`: StatefulSet ×3+, PVCs
  - ConfigMaps, Secrets, `/healthz` probes
- [ ] **Chaos:**
  - **Node loss:** kill `data-svc` mid-write; W=2 holds, healer restores R=3
  - **Leader crash:** kill metadata leader; re-election <500ms, no data loss
  - **Bit-rot:** corrupt a chunk; mismatch detected, replica serves, repair runs
- [ ] **External check:** benchmark public S3 via AWS CLI; verify dashboard access

**M4:** Castor on Kubernetes, reachable via S3 and admin dashboard, chaos-tested.

---

## Summary

| Week | Focus | Deliverable |
|---|---|---|
| 1 | Storage, consensus, gateway | 3 core services, CLI CRUD |
| 2 | Auth, Docker, admin API | SigV4, Range/List, Compose |
| 3 | Multipart, dedup, healing, UI | GC, healer, read-repair, dashboard |
| 4 | Kubernetes, chaos | Manifests, 3 chaos tests |
