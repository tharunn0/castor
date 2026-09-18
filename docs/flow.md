# Castor — Operational Lifecycle & Request Flows

This document details the step-by-step execution flows across Castor for Writes, Reads, Multipart Uploads, and Deletes. For each operation, it details the flow first under **optimal conditions**, followed by **common failure scenarios and recovery paths**.

---

## 1. Write Lifecycle (`PUT Object`)

### 1.1. Optimal Flow (Happy Path)

1. **Client Ingress & Authentication**:
   - The S3 client (AWS CLI, SDK, or HTTP client) sends `PUT /<bucket>/<key>` with payload headers (`Content-Length`, `Content-Type`, SigV4 authentication).
   - `gateway-svc` receives the HTTP request on port `:9000`. The embedded `versitygw` engine verifies AWS SigV4 signature credentials against configured static secrets and extracts the unencoded byte stream.

2. **Bucket Verification**:
   - `gateway-svc` makes a gRPC call to `metadata-svc`: `CheckBucketExists(bucket)`.
   - The Raft leader checks its BadgerDB state store (`bucket:<bucket>`). It confirms the bucket exists and is not marked deleted, returning `OK`.

3. **In-Memory Streaming & Chunking**:
   - `gateway-svc` pulls 4MB buffers from its in-memory pool (`sync.Pool`), bounded by a concurrency semaphore to prevent memory exhaustion.
   - The gateway reads incoming bytes up to 4MB per buffer (objects smaller than 4MB are handled as a single smaller chunk).
   - On the fly, the gateway computes the SHA-256 digest of the chunk bytes.

4. **Inline Deduplication Check**:
   - Before streaming any bytes to disk, `gateway-svc` queries `metadata-svc`: `CheckChunks([sha256_1, sha256_2, ...])`.
   - The leader checks BadgerDB for existing `chunk:<sha256>` records where `ref_count > 0`.
   - If a chunk already exists in the cluster, `gateway-svc` skips writing that chunk to disk entirely and flags it for reference incrementing during commit.

5. **Storage Node Selection**:
   - For newly discovered chunks, `gateway-svc` looks up active storage nodes from its local cache (which refreshes every 5–10 seconds from `metadata-svc.GetActiveNodes`).
   - The gateway selects 3 healthy `data-svc` nodes ($R=3$) weighted by available disk capacity.

6. **Parallel Streaming to Storage Nodes**:
   - `gateway-svc` opens 3 concurrent gRPC client streams to `DataService.PutChunk` on the selected nodes.
   - Frame 1 sends `PutChunkMetadata` with the expected SHA-256 hash and size.
   - Frames 2 through $N$ stream the chunk byte payload.

7. **Crash-Consistent Disk Writes on Storage Nodes**:
   - Each `data-svc` node streams incoming bytes into `/data/staging/<uuid>.tmp`.
   - The node computes SHA-256 incrementally as bytes arrive.
   - Once all bytes are written, the node verifies the calculated checksum against the metadata header.
   - The node calls `file.Sync()` to flush dirty OS page cache pages to physical non-volatile media.
   - The node executes an atomic `os.Rename` moving `/data/staging/<uuid>.tmp` to `/data/chunks/xx/<sha256>` (where `xx` is the first 2 hex digits of the hash).
   - The node issues `dir.Sync()` on the parent directory to persist the directory entry.
   - The node returns a success ACK to `gateway-svc`.

8. **Majority Quorum Evaluation**:
   - `gateway-svc` waits for responses. As soon as **2 of the 3 nodes** acknowledge success ($W=2$ majority quorum), the chunk is officially accepted.
   - If the 3rd node is slow, the gateway does not wait; it records whichever 2 or 3 nodes succeeded in the placement map.

9. **Atomic Commit via Raft**:
   - After all chunks of the object satisfy write quorum, `gateway-svc` sends `CommitManifest` to the `metadata-svc` Raft leader.
   - The leader serializes the command and proposes it to the 3-node Raft consensus cluster over TCP.
   - Once the Raft majority replicates the log entry, the leader applies it to the BadgerDB state machine inside a single atomic transaction (`db.Update`):
     - It creates/updates `ChunkLocationRecord` entries: increments `ref_count`, registers the successful node addresses, and clears any pending `orphaned_at` quarantine timestamps.
     - It creates the `ManifestRecord` at `manifest:<bucket>:<key>` with `status = "committed"`, file size, creation timestamp, and ordered `chunk_ids`.
   - The FSM returns success to the Raft leader.

10. **Client Acknowledgment**:
    - `metadata-svc` returns `CommitManifestResponse` to `gateway-svc`.
    - `gateway-svc` releases the 4MB memory buffers back to `sync.Pool`.
    - `gateway-svc` returns `HTTP/1.1 200 OK` to the client along with standard headers, including `ETag` (hex-encoded SHA-256) and `Content-Length: 0`.

---

### 1.2. Failure Scenarios & Recovery Paths

#### Scenario A: 1 Storage Node Fails or Times Out During Ingest
- **Event**: During a chunk write, Node 1 and Node 2 return success ACKs, but Node 3 crashes, drops connection, or times out.
- **Handling**: Because $W=2$ out of 3 nodes succeeded, write quorum is satisfied. `gateway-svc` marks Node 1 and Node 2 in `ChunkLocation.nodes` and proceeds immediately. The client upload succeeds without penalty.
- **Recovery**: Later, the leader-only **Active Replica Healer** scans BadgerDB, notices `len(ChunkLocation.nodes) < 3`, picks a healthy third node, and issues a background `ReplicateChunk` RPC from Node 1 to restore $R=3$.

#### Scenario B: 2 Storage Nodes Fail (Quorum Loss)
- **Event**: 2 of the 3 target nodes fail, run out of disk space, or time out.
- **Handling**: `gateway-svc` cannot reach write quorum ($W=2$).
- **Client Impact**: `gateway-svc` aborts the pipeline, drops the connection, and returns `HTTP 503 Service Unavailable` or `HTTP 507 Insufficient Storage`.
- **Cleanup**: `gateway-svc` does NOT call `CommitManifest`. Any chunk files written to the single surviving storage node remain on disk without a manifest entry. When the 24-hour quarantine GC sweeps, unreferenced chunks are safely deleted.

#### Scenario C: Checksum Mismatch During Chunk Ingest
- **Event**: Network packet corruption causes the SHA-256 computed on `data-svc` during streaming to differ from the gateway's expected hash.
- **Handling**: `data-svc` immediately halts the stream, deletes `/data/staging/<uuid>.tmp`, and returns gRPC error `INVALID_ARGUMENT`.
- **Client Impact**: `gateway-svc` detects the error. It either retries the chunk against an alternate active node or aborts the upload, returning `HTTP 400 Bad Request` (`ClientChecksumMismatch`).

#### Scenario D: Raft Leader Crashes During `CommitManifest`
- **Event**: All chunk bytes are persisted on storage nodes, but the `metadata-svc` leader crashes while or immediately after `gateway-svc` sends `CommitManifest`.
- **Handling**:
  - The remaining 2 metadata nodes detect missing Raft heartbeats and elect a new leader in $< 500$ms.
  - If the previous leader committed the Raft entry before crashing, the new leader has the record; the gateway’s gRPC retry succeeds immediately.
  - If the previous leader crashed before committing the Raft log, the gateway’s retried `CommitManifest` RPC is routed to the new leader and committed cleanly.
  - If `gateway-svc` completely times out and fails the request to the client, the client safely retries `PUT`. Because chunks are content-addressed and deduplicated, the retry completes almost instantaneously without re-writing bytes to disk.

#### Scenario E: Client Aborts Connection Mid-Stream
- **Event**: The client disconnects halfway through uploading a 500MB file.
- **Handling**: `gateway-svc` detects the broken HTTP pipe. It immediately cancels its outgoing gRPC streams to the storage nodes and releases all allocated memory buffers back to `sync.Pool`.
- **Cleanup**: Storage nodes close and delete their open `/data/staging/<uuid>.tmp` files. Because `CommitManifest` was never called, no manifest is created. Any complete chunks that were already promoted to `/data/chunks/` have a `ref_count` of 0 in metadata and are swept by the 24-hour quarantine GC worker.

---

## 2. Read Lifecycle (`GET Object`)

### 2.1. Optimal Flow (Happy Path)

1. **Client Ingress & S3 Parsing**:
   - Client issues `GET /<bucket>/<key>` (optionally specifying an HTTP `Range: bytes=start-end` header).
   - `gateway-svc` (`versitygw`) authenticates the request and parses bucket, key, and byte-range parameters.

2. **Manifest Lookup**:
   - `gateway-svc` invokes gRPC `MetadataService.GetManifest(bucket, key)` on `metadata-svc`.
   - The Raft leader executes a read transaction (`db.View`) on BadgerDB:
     - Fetches `manifest:<bucket>:<key>`.
     - Validates that `status == "committed"`.
     - For each chunk hash in `chunk_ids`, looks up `chunk:<sha256>` to retrieve the list of holding storage node addresses.
   - `metadata-svc` returns the ordered list of chunks with their sizes and holding node addresses.

3. **Range Calculation**:
   - If no `Range` header is present, the gateway streams the full list of chunks from chunk 0 to end.
   - If a `Range` header is provided (e.g. `bytes=5000000-10000000`), the gateway calculates the exact starting chunk index, byte offset within that chunk, and terminating chunk index.

4. **Sequential Chunk Streaming & Checksum Verification**:
   - For each required chunk in order:
     - `gateway-svc` picks the first responsive storage node from the chunk’s `nodes` list.
     - It invokes gRPC `DataService.GetChunk(chunk_hash, offset, length)`.
     - The storage node opens `/data/chunks/xx/<sha256>`, seeks to the requested offset, and streams raw chunk bytes back to the gateway.
     - If reading the complete chunk, `gateway-svc` computes the SHA-256 digest on the fly and verifies it matches the chunk hash.

5. **Client Response**:
   - `gateway-svc` streams the verified chunk bytes directly into the client’s HTTP response body (`HTTP 200 OK` for full reads, or `HTTP 206 Partial Content` for range requests).

---

### 2.2. Failure Scenarios & Recovery Paths

#### Scenario A: Target Storage Node Unreachable
- **Event**: During streaming of chunk 3, Node 1 fails to respond or drops the gRPC connection.
- **Handling**: `gateway-svc` catches the connection error before writing bad data to the client. It immediately fails over to the next address in `ChunkLocation.nodes` (e.g. Node 2) and issues `GetChunk`.
- **Client Impact**: None. The download continues with negligible latency penalty ($\le 20$ms).
- **Self-Healing**: `gateway-svc` asynchronously sends `TriggerReadRepair(chunk_hash, failed_node)` to the metadata leader, which schedules background re-replication to replace the failed node.

#### Scenario B: Bit-Rot / On-Disk Data Corruption
- **Event**: A byte flips on the storage node's physical media.
- **Handling**:
  - `gateway-svc` receives the stream and finishes hashing the chunk. The computed SHA-256 does not match the manifest's immutable chunk hash.
  - `gateway-svc` discards the corrupt buffer, marks that replica node as untrusted for this chunk, and fetches the chunk from an alternate replica node.
- **Self-Healing**: The gateway issues an asynchronous repair alert to the metadata leader. The leader directs a healthy replica node to overwrite the corrupted file on the failing node or place a fresh copy on a new node via `ReplicateChunk`.

#### Scenario C: Object Does Not Exist or Was Deleted
- **Event**: Client requests a non-existent object or one whose manifest has `status == "deleted"`.
- **Handling**: `metadata-svc` returns gRPC status `NOT_FOUND`.
- **Client Impact**: `gateway-svc` responds with `HTTP/1.1 404 Not Found` and standard XML error code `NoSuchKey`.

#### Scenario D: All Holding Replicas Down (Total Chunk Loss)
- **Event**: Catastrophic simultaneous hardware failure of all 3 replica nodes holding a specific chunk.
- **Handling**: `gateway-svc` attempts each holding node sequentially; all fail or time out.
- **Client Impact**: `gateway-svc` terminates the HTTP stream and returns `HTTP 500 Internal Server Error` or drops the connection if streaming had already commenced.

---

## 3. Multipart Upload Lifecycle

### 3.1. Optimal Flow (Happy Path)

1. **Initiate Session**:
   - Client sends `POST /<bucket>/<key>?uploads`.
   - `gateway-svc` forwards `InitiateMultipart(bucket, key)` to `metadata-svc`.
   - The Raft leader generates a unique `UploadId` (UUIDv4) and proposes an entry to Raft.
   - BadgerDB inserts `multipart:<upload_id>` with `status = "pending"` and `created_at = time.Now()`.
   - `gateway-svc` returns XML `<InitiateMultipartUploadResult>` containing the `UploadId` to the client.

2. **Upload Parts (`UploadPart`)**:
   - The client uploads individual parts concurrently or sequentially:
     `PUT /<bucket>/<key>?uploadId=<upload_id>&partNumber=<N>`.
   - Each part must be $\ge 5$MB (except the final part, per S3 specification).
   - `gateway-svc` receives the part stream, slices it into 4MB chunks, hashes them, performs dedup checks, and writes them with $W=2$ quorum to storage nodes, identical to the standard write path.
   - Once all chunks of the part are stored, `gateway-svc` calls `metadata-svc.CommitPart(upload_id, part_number, chunk_ids, size, etag)`.
   - BadgerDB stores `multipart_part:<upload_id>:<part_number>` (formatted with 5-digit padding, e.g. `00001`) with the part's ordered chunk list.
   - `gateway-svc` returns `HTTP 200 OK` with the part's `ETag` to the client.

3. **Complete Upload (`CompleteMultipartUpload`)**:
   - When all parts are uploaded, the client sends:
     `POST /<bucket>/<key>?uploadId=<upload_id>` with an XML body listing all part numbers and their respective ETags.
   - `gateway-svc` forwards `CompleteMultipart(upload_id, parts)` to `metadata-svc`.
   - The Raft leader processes the completion in a **single atomic transaction**:
     - Scans BadgerDB for all `multipart_part:<upload_id>:*` records in lexicographical order.
     - Verifies that every part declared in the client request matches the stored ETags and part numbers.
     - Concatenates the ordered chunk IDs from all parts into a single consolidated `ManifestRecord`.
     - Inserts `manifest:<bucket>:<key>` with `status = "committed"`.
     - Updates all referenced chunk locations, incrementing reference counts.
     - Marks `multipart:<upload_id>` status as `"completed"`.
     - Deletes intermediate `multipart_part` staging records.
   - `gateway-svc` receives the commit ACK and returns XML `<CompleteMultipartUploadResult>` with the composite object ETag to the client.

---

### 3.2. Failure Scenarios & Recovery Paths

#### Scenario A: Client Explicitly Aborts (`AbortMultipartUpload`)
- **Event**: Client sends `DELETE /<bucket>/<key>?uploadId=<upload_id>`.
- **Handling**: `gateway-svc` calls `metadata-svc.AbortMultipart(upload_id)`.
- **Raft & DB Cleanup**:
  - Leader proposes `AbortMultipart`.
  - BadgerDB transaction marks `multipart:<upload_id>` as `"aborted"`.
  - For all uploaded parts belonging to this session, the FSM decrements `ref_count` on their chunks.
  - Any chunks whose `ref_count` hits 0 have `orphaned_at` set to `time.Now()`, entering the 24-hour quarantine period.
  - The intermediate `multipart_part` records are deleted.
- **Response**: `gateway-svc` returns `HTTP/1.1 204 No Content`.

#### Scenario B: Abandoned / Incomplete Multipart Session
- **Event**: A client initiates an upload, uploads several gigabytes across multiple parts, but network crashes or the client process dies without completing or aborting.
- **Handling**:
  - The Raft leader runs an embedded background worker: **Abandoned Multipart Cleaner**.
  - Every hour, the cleaner scans BadgerDB for `multipart:*` records where `status == "pending"` and `time.Since(created_at) > 24 hours`.
  - The worker automatically triggers the `AbortMultipart` procedure through Raft.
  - Uncommitted chunk refcounts decrement to 0 and enter the 24-hour quarantine window, automatically reclaiming disk space without administrator intervention.

#### Scenario C: Upload Part Fails Mid-Stream
- **Event**: Network drops while uploading part 4 of 10.
- **Handling**: `gateway-svc` fails the request and does not call `CommitPart`. The client receives an error on part 4.
- **Recovery**: The client simply retries uploading part 4 using the same `UploadId` and `partNumber`. Chunks that succeeded during the failed attempt are safely deduplicated or overwritten. Parts 1, 2, and 3 remain intact and valid.

#### Scenario D: Mismatched Part List on Complete
- **Event**: The client sends `CompleteMultipartUpload`, but part 3 is missing, or the ETag for part 2 does not match what was recorded.
- **Handling**: `metadata-svc` validates the part sequence against BadgerDB before applying changes. It detects the inconsistency and rejects the transaction with `INVALID_ARGUMENT`.
- **Client Impact**: `gateway-svc` returns `HTTP 400 Bad Request` (`InvalidPart` or `InvalidPartOrder`). The multipart session remains open in `"pending"` state, allowing the client to re-upload the offending part and retry completion.

---

## 4. Delete Lifecycle (`DELETE Object`)

### 4.1. Optimal Flow (Happy Path)

1. **Client Ingress**:
   - Client sends `DELETE /<bucket>/<key>`.
   - `gateway-svc` validates authentication and issues gRPC `MetadataService.DeleteManifest(bucket, key)` to the Raft leader.

2. **Atomic Soft-Delete & Refcount Decrement**:
   - The Raft leader proposes `DeleteManifestCommand` through Raft consensus.
   - Upon consensus, the BadgerDB FSM executes an atomic transaction (`db.Update`):
     - Loads `manifest:<bucket>:<key>`.
     - Marks the manifest record status as `"deleted"` (or removes it).
     - Reads the manifest’s `chunk_ids` array.
     - For each referenced chunk:
       - Loads `chunk:<sha256>`.
       - Decrements `ref_count` by 1.
       - **Quarantine Trigger**: If `ref_count` drops to 0, the FSM sets `orphaned_at = time.Now()`.
   - The manifest is immediately invisible to subsequent `GET`, `HEAD`, and `LIST` requests.

3. **Client Acknowledgment**:
   - `metadata-svc` returns success to `gateway-svc`.
   - `gateway-svc` returns `HTTP/1.1 204 No Content` to the client.
   - **Crucial Rule**: Raw chunk files on `data-svc` are **not** deleted synchronously during the client request.

4. **The 24-Hour Quarantine Grace Period**:
   - The orphaned chunk remains on disk for 24 hours.
   - If another client uploads an identical chunk within this 24-hour window, the deduplication engine increments `ref_count` back to $\ge 1$ and clears `orphaned_at`, saving the chunk from deletion.

5. **Asynchronous Leader Garbage Collection (Quarantine GC)**:
   - The Raft leader runs an embedded background **Quarantine GC Worker** every hour.
   - The worker scans `chunk:*` records in BadgerDB where:
     `ref_count == 0` AND `time.Since(orphaned_at) > 24 hours`.
   - For qualifying chunks:
     - The worker dispatches rate-limited `DataService.DeleteChunk(chunk_hash)` gRPC calls (capped at 50/second) to every storage node listed in `ChunkLocation.nodes`.
     - Each storage node deletes `/data/chunks/xx/<sha256>` from its local filesystem.
     - Once storage node deletions succeed, the leader proposes `RemoveChunkLocations([chunk_hash])` through Raft to permanently purge the metadata entry from BadgerDB.

---

### 4.2. Failure Scenarios & Recovery Paths

#### Scenario A: Storage Node Down During Quarantine GC Purge
- **Event**: The GC worker tries to delete an orphaned chunk from Node 1, Node 2, and Node 3, but Node 3 is offline.
- **Handling**: Node 1 and Node 2 delete their local chunk files. The call to Node 3 times out or fails.
- **Recovery**: The GC worker does NOT purge the chunk's metadata entry completely. Instead, it updates `ChunkLocation.nodes` to remove Node 1 and Node 2, leaving Node 3. On the next GC cycle (or when Node 3 rejoins), the worker retries `DeleteChunk` against Node 3. Once all nodes confirm deletion, the chunk location record is removed via Raft.

#### Scenario B: Deleting a Non-Existent Object
- **Event**: Client issues `DELETE /<bucket>/non-existent-key`.
- **Handling**: Per Amazon S3 specification, deleting a non-existent key is an idempotent operation.
- **Response**: `metadata-svc` finds no active manifest record and takes no action. `gateway-svc` returns `HTTP/1.1 204 No Content`.

#### Scenario C: Concurrent Upload Re-Deduplicates Quarantined Chunk
- **Event**: Chunk `7f83...` was orphaned 23 hours ago (`ref_count == 0`, `orphaned_at` set). A client uploads a new object containing the exact same chunk 30 minutes before the 24-hour GC deadline.
- **Handling**:
  - `gateway-svc` checks `CheckChunks` and identifies that `7f83...` exists in metadata.
  - When `CommitManifest` executes its atomic transaction in BadgerDB, it detects that `7f83...` is already present.
  - It increments `ref_count` from 0 to 1 and sets `orphaned_at = null`.
  - The chunk is instantly salvaged. When the GC worker runs 30 minutes later, it skips this chunk because `ref_count > 0`. Zero disk writes were needed, and zero race conditions occurred.

#### Scenario D: Raft Leadership Changes During Garbage Collection
- **Event**: The metadata leader is in the middle of executing a GC sweep and loses leadership (network partition or restart).
- **Handling**:
  - Background workers run **strictly on the active Raft leader**.
  - Upon losing leadership, the worker immediately aborts its current sweep.
  - The newly elected leader starts its own worker instance, scans BadgerDB afresh, and resumes safely from where the previous leader left off. No duplicate deletions cause harm because filesystem chunk removal is idempotent (`os.Remove` ignores non-existent files).
