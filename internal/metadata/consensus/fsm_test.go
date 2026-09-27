package consensus

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/store"
	"google.golang.org/protobuf/proto"
)

func newTestFSM(t *testing.T) (*FSM, *store.Store) {
	t.Helper()
	s, err := store.Open("", true)
	if err != nil {
		t.Fatalf("failed to open in-memory badger store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	fsm := NewFSM(s)
	return fsm, s
}

func applyCommand(t *testing.T, fsm *FSM, cmd *Command) any {
	t.Helper()
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("failed to encode command %s: %v", cmd.Type, err)
	}
	return fsm.Apply(&raft.Log{Data: data})
}

func TestFSM_ApplyCreateBucket(t *testing.T) {
	fsm, s := newTestFSM(t)
	ctx := context.Background()

	req := &castorv1.CreateBucketMetadataRequest{
		Bucket:  "photos",
		OwnerId: "user-uuid-1",
		Tags:    map[string]string{"env": "prod"},
	}

	cmd, err := NewCreateBucketCommand(req)
	if err != nil {
		t.Fatalf("failed to create bucket command: %v", err)
	}

	res := applyCommand(t, fsm, cmd)
	if err, ok := res.(error); ok && err != nil {
		t.Fatalf("fsm.Apply failed: %v", err)
	}

	bucket, err := s.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("expected bucket to be created, got error: %v", err)
	}
	if bucket == nil {
		t.Fatal("expected non-nil bucket record")
	}
	if bucket.Name != "photos" {
		t.Fatalf("expected bucket name 'photos', got %q", bucket.Name)
	}
	if bucket.OwnerId != "user-uuid-1" {
		t.Fatalf("expected owner_id 'user-uuid-1', got %q", bucket.OwnerId)
	}
	if bucket.IsDeleted {
		t.Fatal("expected is_deleted to be false")
	}
	if bucket.Tags["env"] != "prod" {
		t.Fatalf("expected tag env=prod, got %v", bucket.Tags)
	}
}

func TestFSM_ApplyDeleteBucket(t *testing.T) {
	fsm, s := newTestFSM(t)
	ctx := context.Background()

	// 1. Create bucket first
	createReq := &castorv1.CreateBucketMetadataRequest{
		Bucket:  "documents",
		OwnerId: "user-uuid-2",
	}
	createCmd, err := NewCreateBucketCommand(createReq)
	if err != nil {
		t.Fatalf("failed to create bucket command: %v", err)
	}
	if res := applyCommand(t, fsm, createCmd); res != nil {
		if err, ok := res.(error); ok && err != nil {
			t.Fatalf("create bucket apply failed: %v", err)
		}
	}

	// Verify bucket created
	b, err := s.GetBucket(ctx, "documents")
	if err != nil || b == nil {
		t.Fatalf("expected bucket 'documents' to exist before deletion: %v", err)
	}

	// 2. Delete bucket
	deleteReq := &castorv1.DeleteBucketMetadataRequest{
		Bucket: "documents",
	}
	deleteCmd, err := NewDeleteBucketCommand(deleteReq)
	if err != nil {
		t.Fatalf("failed to create delete bucket command: %v", err)
	}
	if res := applyCommand(t, fsm, deleteCmd); res != nil {
		if err, ok := res.(error); ok && err != nil {
			t.Fatalf("delete bucket apply failed: %v", err)
		}
	}

	// 3. Verify bucket is marked deleted in public store read
	_, err = s.GetBucket(ctx, "documents")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted bucket, got: %v", err)
	}

	// 4. Verify record in BadgerDB has IsDeleted == true
	err = s.DB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(store.BucketKey("documents"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var record castorv1.BucketRecord
			if err := proto.Unmarshal(val, &record); err != nil {
				return err
			}
			if !record.IsDeleted {
				t.Fatal("expected record.IsDeleted to be true in raw badger store")
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("failed reading raw badger bucket record: %v", err)
	}
}

func TestFSM_ApplyCommitManifest_NewAndOverwrite(t *testing.T) {
	fsm, s := newTestFSM(t)
	ctx := context.Background()

	chunk1 := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	chunk2 := "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
	chunk3 := "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"

	// 1. Commit initial manifest with chunk1 and chunk2
	req1 := &castorv1.CommitManifestRequest{
		Bucket:      "media",
		Key:         "video.mp4",
		Size:        8388608,
		Etag:        "etag-initial",
		ContentType: "video/mp4",
		ChunkIds:    []string{chunk1, chunk2},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: chunk1, NodeAddresses: []string{"node1:9101"}, Size: 4194304},
			{ChunkHash: chunk2, NodeAddresses: []string{"node1:9101"}, Size: 4194304},
		},
	}
	cmd1, err := NewCommitManifestCommand(req1)
	if err != nil {
		t.Fatalf("failed to create commit manifest command: %v", err)
	}

	if res := applyCommand(t, fsm, cmd1); res != nil {
		if err, ok := res.(error); ok && err != nil {
			t.Fatalf("commit manifest apply failed: %v", err)
		}
	}

	// Verify manifest committed
	m, err := s.GetManifest(ctx, "media", "video.mp4")
	if err != nil {
		t.Fatalf("expected manifest to exist, got: %v", err)
	}
	if m == nil || m.Size != 8388608 || m.Etag != "etag-initial" || len(m.ChunkIds) != 2 {
		t.Fatalf("unexpected manifest record: %+v", m)
	}

	// Verify chunk reference counts
	loc1, err := s.GetChunkLocation(ctx, chunk1)
	if err != nil {
		t.Fatalf("expected chunk1 to exist: %v", err)
	}
	if loc1.RefCount != 1 || loc1.OrphanedAt != nil {
		t.Fatalf("chunk1 expected RefCount 1 with nil OrphanedAt, got %+v", loc1)
	}

	loc2, err := s.GetChunkLocation(ctx, chunk2)
	if err != nil {
		t.Fatalf("expected chunk2 to exist: %v", err)
	}
	if loc2.RefCount != 1 || loc2.OrphanedAt != nil {
		t.Fatalf("chunk2 expected RefCount 1 with nil OrphanedAt, got %+v", loc2)
	}

	// 2. Overwrite manifest: replaces chunk1 with chunk3 (chunk2 is retained)
	req2 := &castorv1.CommitManifestRequest{
		Bucket:      "media",
		Key:         "video.mp4",
		Size:        10000000,
		Etag:        "etag-overwritten",
		ContentType: "video/mp4",
		ChunkIds:    []string{chunk2, chunk3},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: chunk2, NodeAddresses: []string{"node1:9101"}, Size: 4194304},
			{ChunkHash: chunk3, NodeAddresses: []string{"node1:9101"}, Size: 5805696},
		},
	}
	cmd2, err := NewCommitManifestCommand(req2)
	if err != nil {
		t.Fatalf("failed to create overwrite manifest command: %v", err)
	}

	if res := applyCommand(t, fsm, cmd2); res != nil {
		if err, ok := res.(error); ok && err != nil {
			t.Fatalf("overwrite manifest apply failed: %v", err)
		}
	}

	// Displaced chunk1 must have RefCount == 0 and OrphanedAt != nil
	loc1After, err := s.GetChunkLocation(ctx, chunk1)
	if err != nil {
		t.Fatalf("expected chunk1 to still exist in store: %v", err)
	}
	if loc1After.RefCount != 0 || loc1After.OrphanedAt == nil {
		t.Fatalf("displaced chunk1 expected RefCount 0 and non-nil OrphanedAt, got %+v", loc1After)
	}

	// Retained chunk2 must maintain its reference
	loc2After, err := s.GetChunkLocation(ctx, chunk2)
	if err != nil {
		t.Fatalf("expected chunk2 to exist: %v", err)
	}
	if loc2After.RefCount < 1 {
		t.Fatalf("retained chunk2 expected RefCount >= 1, got %+v", loc2After)
	}

	// New chunk3 must have RefCount == 1 and OrphanedAt == nil
	loc3After, err := s.GetChunkLocation(ctx, chunk3)
	if err != nil {
		t.Fatalf("expected chunk3 to exist: %v", err)
	}
	if loc3After.RefCount != 1 || loc3After.OrphanedAt != nil {
		t.Fatalf("new chunk3 expected RefCount 1 with nil OrphanedAt, got %+v", loc3After)
	}
}

func TestFSM_ApplyDeleteManifest(t *testing.T) {
	fsm, s := newTestFSM(t)
	ctx := context.Background()

	chunk := "2b92e73886f4a8b79b634863c1b017b2b71fa14798e2cae544521dd987e9ec8d"

	// 1. Commit manifest
	commitReq := &castorv1.CommitManifestRequest{
		Bucket:      "backups",
		Key:         "archive.tar",
		Size:        4096,
		Etag:        "etag-archive",
		ContentType: "application/x-tar",
		ChunkIds:    []string{chunk},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: chunk, NodeAddresses: []string{"node1:9101"}, Size: 4096},
		},
	}
	commitCmd, err := NewCommitManifestCommand(commitReq)
	if err != nil {
		t.Fatalf("failed to create commit command: %v", err)
	}
	applyCommand(t, fsm, commitCmd)

	// Verify manifest exists
	m, err := s.GetManifest(ctx, "backups", "archive.tar")
	if err != nil || m == nil {
		t.Fatalf("expected manifest before delete, got: %v", err)
	}

	// 2. Delete manifest
	deleteReq := &castorv1.DeleteManifestRequest{
		Bucket: "backups",
		Key:    "archive.tar",
	}
	deleteCmd, err := NewDeleteManifestCommand(deleteReq)
	if err != nil {
		t.Fatalf("failed to create delete command: %v", err)
	}
	if res := applyCommand(t, fsm, deleteCmd); res != nil {
		if err, ok := res.(error); ok && err != nil {
			t.Fatalf("delete manifest apply failed: %v", err)
		}
	}

	// 3. Manifest should return ErrNotFound
	_, err = s.GetManifest(ctx, "backups", "archive.tar")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted manifest, got: %v", err)
	}

	// 4. Associated chunk must have RefCount == 0 and OrphanedAt populated
	loc, err := s.GetChunkLocation(ctx, chunk)
	if err != nil {
		t.Fatalf("failed to get chunk location: %v", err)
	}
	if loc.RefCount != 0 || loc.OrphanedAt == nil {
		t.Fatalf("expected RefCount 0 and non-nil OrphanedAt, got %+v", loc)
	}
}


func TestFSM_ApplyChunkLocationOperations(t *testing.T) {
	fsm, s := newTestFSM(t)
	ctx := context.Background()

	hash := "e4b9e73886f4a8b79b634863c1b017b2b71fa14798e2cae544521dd987e9ec8e"

	// 1. Update Chunk Location
	updateReq := &castorv1.UpdateChunkLocationRequest{
		ChunkHash:     hash,
		NodeAddresses: []string{"10.0.0.1:9101", "10.0.0.2:9102"},
	}
	updateCmd, err := NewUpdateChunkLocationCommand(updateReq)
	if err != nil {
		t.Fatalf("failed to create update chunk location command: %v", err)
	}
	applyCommand(t, fsm, updateCmd)

	loc, err := s.GetChunkLocation(ctx, hash)
	if err != nil {
		t.Fatalf("expected chunk location to exist, got: %v", err)
	}
	if len(loc.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(loc.Nodes))
	}

	// 2. Remove Chunk Locations
	removeReq := &castorv1.RemoveChunkLocationsRequest{
		ChunkHash:     hash,
		NodeAddresses: []string{"10.0.0.1:9101"},
	}
	removeCmd, err := NewRemoveChunkLocationsCommand(removeReq)
	if err != nil {
		t.Fatalf("failed to create remove chunk locations command: %v", err)
	}
	applyCommand(t, fsm, removeCmd)

	_, err = s.GetChunkLocation(ctx, hash)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for removed chunk, got %v", err)
	}
}

type bufferSnapshotSink struct {
	buf    bytes.Buffer
	closed bool
}

func (s *bufferSnapshotSink) Write(p []byte) (n int, err error) {
	return s.buf.Write(p)
}

func (s *bufferSnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *bufferSnapshotSink) ID() string {
	return "test-snapshot-1"
}

func (s *bufferSnapshotSink) Cancel() error {
	return nil
}

func TestFSM_SnapshotAndRestore(t *testing.T) {
	fsm1, s1 := newTestFSM(t)
	ctx := context.Background()

	// Seed source FSM1 with bucket and manifest
	bucketCmd, _ := NewCreateBucketCommand(&castorv1.CreateBucketMetadataRequest{
		Bucket:  "snapshot-bucket",
		OwnerId: "user-snap",
	})
	applyCommand(t, fsm1, bucketCmd)

	manifestCmd, _ := NewCommitManifestCommand(&castorv1.CommitManifestRequest{
		Bucket:      "snapshot-bucket",
		Key:         "data.txt",
		Size:        1024,
		Etag:        "etag-snap",
		ContentType: "text/plain",
		ChunkIds:    []string{"chunk-snap-1"},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: "chunk-snap-1", NodeAddresses: []string{"node1:9101"}, Size: 1024},
		},
	})
	applyCommand(t, fsm1, manifestCmd)

	// Verify s1 has bucket before snapshot
	if _, err := s1.GetBucket(ctx, "snapshot-bucket"); err != nil {
		t.Fatalf("expected bucket in s1 before snapshot: %v", err)
	}

	// Take snapshot from FSM1
	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("failed to take snapshot: %v", err)
	}

	sink := &bufferSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot to sink: %v", err)
	}
	snap.Release()

	if !sink.closed {
		t.Fatal("expected snapshot sink to be closed")
	}

	// Create clean FSM2 and restore snapshot
	fsm2, s2 := newTestFSM(t)
	readCloser := io.NopCloser(bytes.NewReader(sink.buf.Bytes()))
	if err := fsm2.Restore(readCloser); err != nil {
		t.Fatalf("failed to restore snapshot into FSM2: %v", err)
	}

	// Verify restored data in FSM2
	restoredBucket, err := s2.GetBucket(ctx, "snapshot-bucket")
	if err != nil {
		t.Fatalf("expected restored bucket to exist in s2: %v", err)
	}
	if restoredBucket.Name != "snapshot-bucket" || restoredBucket.OwnerId != "user-snap" {
		t.Fatalf("unexpected restored bucket state: %+v", restoredBucket)
	}

	restoredManifest, err := s2.GetManifest(ctx, "snapshot-bucket", "data.txt")
	if err != nil {
		t.Fatalf("expected restored manifest to exist in s2: %v", err)
	}
	if restoredManifest.Key != "data.txt" || restoredManifest.Etag != "etag-snap" {
		t.Fatalf("unexpected restored manifest state: %+v", restoredManifest)
	}
}
