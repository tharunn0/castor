# Castor — Database & Persistence Specification

Minimal reference for Castor's storage layouts, key-value schemas, and transactional rules.

---

## 1. Storage Directories

| Path | Engine | Purpose | Durability |
|---|---|---|---|
| `/data/postgres/` (or `castor_auth.db`) | PostgreSQL / SQLite | User accounts, password hashes, and S3 API credentials | Managed by RDB WAL / ACID |
| `/data/raft/raft.db` | `raft-boltdb` (B+Tree) | Raft WAL & stable store (term, vote) | POSIX `fsync` on commit |
| `/data/badger/state/` | BadgerDB (LSM) | Replicated FSM metadata store | Managed by Raft FSM commits |
| `/data/staging/` | Local POSIX | In-flight upload chunks (`<uuid>.tmp`) | Swept on node startup |
| `/data/chunks/xx/<sha256>` | Local POSIX | Committed 4MB chunks (2-hex prefix sharded) | Atomic `os.Rename` + parent `dir.Sync()` |

---

## 2. Auth Database Relational Schema (PostgreSQL / SQLite)

Managed by `auth-svc`. Owns identity and credential lifecycles:

```sql
-- 1. Users Table
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(64) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(16) NOT NULL DEFAULT 'USER', -- 'ADMIN' | 'USER'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX idx_users_username ON users(username);

-- 2. S3 Credentials Table (SigV4 Keypairs)
CREATE TABLE s3_credentials (
    access_key_id VARCHAR(32) PRIMARY KEY,
    secret_access_key VARCHAR(64) NOT NULL,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    label VARCHAR(64),
    status VARCHAR(16) DEFAULT 'ACTIVE', -- 'ACTIVE' | 'REVOKED'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX idx_s3_credentials_user_id ON s3_credentials(user_id);
```

---

## 3. Key Encodings (BadgerDB Metadata)

| Prefix Pattern | Example Key | Notes |
|---|---|---|
| `bucket:<bucket>` | `bucket:media` | Bucket existence, metadata, and `owner_id` |
| `manifest:<bucket>:<key>` | `manifest:media:videos/clip.mp4` | Object metadata, `owner_id`, and ordered chunk list |
| `chunk:<sha256>` | `chunk:7f83b165...` | Content-addressed chunk refcount and nodes |
| `multipart:<upload_id>` | `multipart:c83b40d2...` | Active multipart session |
| `multipart_part:<upload_id>:<part_num>` | `multipart_part:c83b40d2...:00001` | 5-digit zero-padded part for lexicographical sort |

---

## 4. Metadata Protobuf Schemas

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
  string owner_id = 5;             // User UUID from Auth DB
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
  string owner_id = 11;            // User UUID from Auth DB
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

## 5. Transaction Invariants (`db.Update`)

1. **`CommitManifest` (Atomic Write / Overwrite)**:
   - On overwrite, decrement `ref_count` on displaced chunks (set `orphaned_at = now` if `ref_count == 0`).
   - For all incoming chunks: if chunk exists, increment `ref_count`, clear `orphaned_at`, union node addresses; if new, set `ref_count = 1`.
   - Persist `ManifestRecord` with `status = "committed"` and authenticated `owner_id`.

2. **`DeleteManifest` (Atomic Delete)**:
   - Mark `ManifestRecord` status as `"deleted"`.
   - Decrement `ref_count` for all associated chunks. If `ref_count == 0`, set `orphaned_at = now`.
   - **Quarantine rule**: Chunks are physically purged via `DeleteChunk` only after remaining at `ref_count == 0` for $\ge$ 24 hours.
