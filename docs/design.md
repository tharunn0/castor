# Castor — System Design & Overview

Castor is an S3-compatible, distributed object storage system designed for high write throughput, linearizable metadata consistency, and content-addressed chunk deduplication.

---

## 1. System Philosophy & Architecture

Castor decouples **stateless protocol & console ingress**, **identity management**, **replicated consensus metadata**, and **raw chunk storage**:

```
                       ┌──────────────────────────────────────────────┐
                       │               CLIENT INTERFACES              │
                       ├──────────────────────┬───────────────────────┤
                       │  AWS CLI / SDKs /    │  Web Browser Console  │
                       │  Rclone (HTTP S3)    │  (HTML/JS Dashboard)  │
                       └──────────┬───────────┴───────────┬───────────┘
                                  │ HTTP/S3 (:9000)       │ HTTP/BFF (:9001)
                                  ▼                       ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ gateway-svc [xN] (Stateless Front Door & Management)                                   │
│ • Port :9000: S3 REST Engine (versitygw, SigV4 Auth, 4MB Chunking, Dedup, W=2 Quorum)  │
│ • Port :9001: Embedded Web Console & BFF (embed.FS, JWT Sessions, In-Process Bridge)    │
│ • Local Credential LRU Cache (30s TTL, avoids DB bottleneck)                          │
└───────────────────┬─────────────────────────────────┬───────────────────┬──────────────┘
                    │ gRPC (metadata)                 │ gRPC (chunks)     │ HTTP/REST
                    ▼                                 ▼                   ▼
        ┌───────────────────┐             ┌────────────────────────┐  ┌───────────────────┐
        │ metadata-svc [x3] │─────────────► data-svc [x3, indep.]  │  │   auth-svc [x1]   │
        │ (Raft + BadgerDB) │             │ (Stateless Chunks)     │  │ (PostgreSQL /     │
        └───────────────────┘             └────────────────────────┘  │  SQLite Auth DB)  │
          (leader: GC & heal)               (raw 4MB files sharded)   └───────────────────┘
```

1. **`gateway-svc` (Stateless Front Door, Ports `:9000` & `:9001`)**:
   - **Port `:9000` (S3 REST Engine)**: Embeds `versitygw` to provide Amazon S3 REST compatibility (Path-style, dynamic SigV4 authentication via `auth-svc`, XML marshaling). Ingress chunking engine streams payloads disklessly through 4MB memory buffers (`sync.Pool`), computes SHA-256 hashes, queries deduplication, and coordinates $W=2$ quorum writes.
   - **Port `:9001` (Embedded Web Console & BFF)**: Serves a precompiled, single-page web console embedded via Go `embed.FS`. Exposes browser-tailored JSON endpoints for user authentication, S3 key generation, file browsing/uploads, and live cluster health telemetry.
   - **In-Memory Credential Cache**: Maintains a short-lived (30s TTL) LRU cache of active S3 credentials to prevent database connection saturation during high-throughput S3 streams.
   - Horizontally scalable behind a layer 4 or layer 7 load balancer.

2. **`auth-svc` (Identity & S3 Key Provider, Port `:9095`)**:
   - Manages user identity, password hashing (`bcrypt`), and S3 credential lifecycles backed by a relational database (PostgreSQL or SQLite).
   - Issues JWTs for Web Console sessions and generates standard S3 API keypairs (`access_key_id` / `secret_access_key`).
   - Exposes internal validation endpoints for `gateway-svc` SigV4 signature verification.

3. **`metadata-svc` (Consensus & Metadata, Ports `:9091`-`:9093`, Admin `:9071`-`:9073`)**:
   - 3-node Raft consensus cluster backed by `raft-boltdb` B+Tree storage (`/data/raft/raft.db` WAL/stable store) and pure-Go BadgerDB LSM storage (`/data/badger/state/` replicated FSM).
   - Manages bucket catalogs, object manifests (with user attribution `owner_id`), chunk locations, and an ephemeral in-memory storage node heartbeat registry.
   - Raft leader runs embedded background workers: active replica healing, 24-hour quarantine GC, and multipart expiration.

4. **`data-svc` (Raw Chunk Storage, Ports `:9101`-`:9103`)**:
   - Content-addressed chunk store on local POSIX filesystems. Completely agnostic to users, buckets, or auth.
   - Chunks are stored as raw immutable files sharded by 2-character hex prefixes (`/data/chunks/xx/<sha256>`).
   - Crash-consistent writes via staging and atomic `os.Rename`.
   - Executes peer-to-peer chunk transfers (`ReplicateChunk`) on demand.

---

## 2. Core Architectural Invariants

- **Fixed 4MB Content-Addressed Chunks**: Standard uniform chunk size. Small objects ($< 4$MB) are stored as a single chunk. Raw bytes never touch the Raft log or BadgerDB.
- **Majority Quorum ($W=2, R=3$)**: Chunks are streamed to 3 storage nodes in parallel. Writes succeed and advance as soon as 2 nodes persist to disk (`fsync`); the 3rd replica is healed asynchronously.
- **Inline Deduplication**: Chunks matching an existing SHA-256 hash bypass disk writes; only reference counts are incremented on commit.
- **24-Hour Quarantine Window**: When objects are overwritten or deleted, chunk reference counts decrement. Chunks with `ref_count == 0` enter a 24-hour grace period before physical disk purging, eliminating race conditions with concurrent uploads.
- **Leader-Only Embedded Workers**: All garbage collection and healing workers execute exclusively on the active Raft leader, preventing split-brain operations without requiring external schedulers.
- **Strict Separation of Identity and Storage State**: Relational Auth DB owns user identity and credentials; Raft consensus owns bucket and object manifest state. Zero cross-system distributed 2-phase transactions.

---

## 3. Explicit Non-Goals

To maintain a lean, robust implementation, the following features are intentionally out of scope:
- **No Complex AWS IAM JSON Policy Engine or STS**: Authentication uses user accounts and S3 API keypairs with bucket ownership checks (`owner_id`); dynamic multi-statement AWS IAM policy evaluation is excluded.
- **No Object Versioning**: All overwrites strictly follow Last-Write-Wins (LWW).
- **No Erasure Coding**: Fixed $R=3$ replication with SHA-256 verify-on-read.
- **No Server-Side Encryption (KMS)**.
- **No Object Tagging, Cross-Region Replication, or Static Website Hosting**.
- **No Dynamic Raft Membership**: Fixed 3-node metadata cluster topology.

---

## 4. Performance & Target Benchmarks

Target baselines for standard hardware (8 vCPUs, 16GB RAM, NVMe SSD, loopback/10GbE networking):

| Metric | Workload | Target | Bottleneck / Limiter |
|---|---|---|---|
| **PUT Throughput** | 100MB–1GB streaming ($W=2, R=3$) | **400 – 650 MB/s** | NVMe write bandwidth & CPU SHA-256 hashing |
| **GET Throughput** | 100MB–1GB streaming (verified) | **600 – 1,000 MB/s** | NVMe read bandwidth & gRPC framing |
| **Dedup PUT** | Re-uploading existing chunks | **1.0 – 1.8 GB/s** | CPU SHA-256 calculation (disk I/O bypassed) |
| **Small Object RPS** | 64KB–1MB objects ($W=2$) | **1,000 – 2,500 ops/s** | Raft BadgerDB WAL append batching + fsync |
| **Metadata Read RPS** | `GetManifest` lookups | **12,000 – 25,000 ops/s** | Leader BadgerDB cache / memory read |
| **Failover Window** | Raft Leader crash | **< 500 – 800 ms** | Raft heartbeat timeout & new leader election |

---

## 5. Specification Document Index

For low-level contracts and workflows, refer to the dedicated specification documents:

- **[architecture.md](architecture.md)**: System topology, end-to-end Mermaid sequence diagrams, and failure boundary tables.
- **[database.md](database.md)**: Storage directories, key prefix encodings, Protobuf schemas, and transaction invariants.
- **[flow.md](flow.md)**: Step-by-step execution flows and failure recovery scenarios for Writes, Reads, Multipart, and Deletes.
- **[api.md](api.md)**: Full S3 REST endpoint catalog, operator admin HTTP APIs, and internal gRPC service definitions.
- **[4_week.md](4_week.md)**: 4-week implementation milestones, weekly deliverables, and testing tasks.
