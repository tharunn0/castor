# Castor — Database & Persistence Specification

Minimal reference for Castor's storage layouts, key-value schemas, and transactional rules.

---

## 1. Storage Directories

| Path | Engine | Purpose | Durability |
|---|---|---|---|
| `/data/badger/raft/` | BadgerDB | Raft WAL & stable store (term, vote) | `SyncWrites: true` |
| `/data/badger/state/` | BadgerDB | Replicated FSM metadata store | Managed by Raft FSM commits |
| `/data/staging/` | Local POSIX | In-flight upload chunks (`<uuid>.tmp`) | Swept on node startup |
| `/data/chunks/xx/<sha256>` | Local POSIX | Committed 4MB chunks (2-hex prefix sharded) | Atomic `os.Rename` + parent `dir.Sync()` |

---

## 2. Key Encodings

| Prefix Pattern | Example Key | Notes |
|---|---|---|
| `bucket:<bucket>` | `bucket:media` | Bucket existence and metadata |
| `manifest:<bucket>:<key>` | `manifest:media:videos/clip.mp4` | Object metadata and ordered chunk list |
| `chunk:<sha256>` | `chunk:7f83b165...` | Content-addressed chunk refcount and nodes |
| `multipart:<upload_id>` | `multipart:c83b40d2...` | Active multipart session |
| `multipart_part:<upload_id>:<part_num>` | `multipart_part:c83b40d2...:00001` | 5-digit zero-padded part for lexicographical sort |

---

## 3. Metadata Protobuf Schemas

Package: `castor.v1.db`

```protobuf
syntax = "proto3";
package castor.v1.db;

import "google/protobuf/timestamp.proto";

message BucketRecord {
  string name = 1;
  google.protobuf.Timestamp created_at = 2;
  map<string, string> tags = 3;
  bool is_deleted = 4;
}

message ManifestRecord {
  string bucket = 1;
  string key = 2;
  int64 size = 3;
  string etag = 4;                 // Hex-encoded SHA-256
  string content_type = 5;
  repeated string chunk_ids = 6;   // Ordered SHA-256 chunk digests
  string status = 7;               // "pending" | "committed" | "deleted"
  google.protobuf.Timestamp created_at = 8;
  google.protobuf.Timestamp updated_at = 9;
  map<string, string> user_metadata = 10;
}

message ChunkLocationRecord {
  string chunk_hash = 1;           // SHA-256 digest
  int64 size = 2;                  // Chunk size in bytes (up to 4MB)
  repeated string nodes = 3;       // data-svc gRPC addresses holding this chunk
  int32 ref_count = 4;             // Active manifest references
  google.protobuf.Timestamp orphaned_at = 5; // Set when ref_count == 0 (24h quarantine)
}

message MultipartRecord {
  string upload_id = 1;
  string bucket = 2;
  string key = 3;
  string content_type = 4;
  string status = 5;               // "pending" | "completed" | "aborted"
  google.protobuf.Timestamp created_at = 6;
}

message PartRecord {
  string upload_id = 1;
  int32 part_number = 2;
  int64 size = 3;
  string etag = 4;
  repeated string chunk_ids = 5;
  google.protobuf.Timestamp uploaded_at = 6;
}
```

---

## 4. Transaction Invariants (`db.Update`)

1. **`CommitManifest` (Atomic Write / Overwrite)**:
   - On overwrite, decrement `ref_count` on displaced chunks (set `orphaned_at = now` if `ref_count == 0`).
   - For all incoming chunks: if chunk exists, increment `ref_count`, clear `orphaned_at`, union node addresses; if new, set `ref_count = 1`.
   - Persist `ManifestRecord` with `status = "committed"`.

2. **`DeleteManifest` (Atomic Delete)**:
   - Mark `ManifestRecord` status as `"deleted"`.
   - Decrement `ref_count` for all associated chunks. If `ref_count == 0`, set `orphaned_at = now`.
   - **Quarantine rule**: Chunks are physically purged via `DeleteChunk` only after remaining at `ref_count == 0` for $\ge$ 24 hours.
