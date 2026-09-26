package store

import (
	"context"
	"errors"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
)

func TestStore_CreateBucket(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// 1. Successful bucket creation
	b, err := s.CreateBucket(ctx, "media", "user-uuid-1", map[string]string{"env": "prod"})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	if b.Name != "media" || b.OwnerId != "user-uuid-1" || b.Tags["env"] != "prod" || b.IsDeleted {
		t.Fatalf("unexpected bucket record: %+v", b)
	}

	// 2. Duplicate bucket returns ErrBucketExists
	_, err = s.CreateBucket(ctx, "media", "user-uuid-2", nil)
	if !errors.Is(err, ErrBucketExists) {
		t.Fatalf("expected ErrBucketExists on duplicate bucket, got: %v", err)
	}

	// 3. Verify bucket retrievable via GetBucket
	got, err := s.GetBucket(ctx, "media")
	if err != nil {
		t.Fatalf("failed to get created bucket: %v", err)
	}
	if got.Name != "media" || got.OwnerId != "user-uuid-1" {
		t.Fatalf("mismatched bucket data: %+v", got)
	}
}

func TestStore_DeleteBucket(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// 1. Delete non-existent bucket returns ErrNotFound
	err := s.DeleteBucket(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for nonexistent bucket, got: %v", err)
	}

	// 2. Create empty bucket and delete it
	_, err = s.CreateBucket(ctx, "empty-bucket", "user-1", nil)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	err = s.DeleteBucket(ctx, "empty-bucket")
	if err != nil {
		t.Fatalf("failed to delete empty bucket: %v", err)
	}

	// After deletion, GetBucket returns ErrNotFound
	_, err = s.GetBucket(ctx, "empty-bucket")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after deletion, got: %v", err)
	}

	// 3. Bucket with active manifests cannot be deleted
	_, err = s.CreateBucket(ctx, "active-bucket", "user-1", nil)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	_, err = s.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:      "active-bucket",
		Key:         "file.txt",
		Size:        100,
		Etag:        "etag-1",
		ContentType: "text/plain",
		ChunkIds:    []string{"chunk-1"},
	})
	if err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}

	err = s.DeleteBucket(ctx, "active-bucket")
	if !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("expected ErrBucketNotEmpty, got: %v", err)
	}

	// Delete manifest then delete bucket succeeds
	err = s.DeleteManifest(ctx, "active-bucket", "file.txt")
	if err != nil {
		t.Fatalf("failed to delete manifest: %v", err)
	}

	err = s.DeleteBucket(ctx, "active-bucket")
	if err != nil {
		t.Fatalf("expected delete bucket to succeed after manifest deleted, got: %v", err)
	}
}

func TestStore_CommitManifest_NewAndOverwrite(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	chunk1 := "hash-1"
	chunk2 := "hash-2"
	chunk3 := "hash-3"

	// Must fail if bucket doesn't exist
	_, err := s.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:   "nonexistent",
		Key:      "data.bin",
		ChunkIds: []string{chunk1},
	})
	if !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got: %v", err)
	}

	// Create bucket
	_, err = s.CreateBucket(ctx, "my-bucket", "user-owner", nil)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 1. Commit new manifest with chunk1 and chunk2
	m1, err := s.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:      "my-bucket",
		Key:         "data.bin",
		Size:        8192,
		Etag:        "etag-1",
		ContentType: "application/octet-stream",
		ChunkIds:    []string{chunk1, chunk2},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: chunk1, NodeAddresses: []string{"node1:9101"}, Size: 4096},
			{ChunkHash: chunk2, NodeAddresses: []string{"node2:9101"}, Size: 4096},
		},
	})
	if err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}
	if m1.Status != "committed" || m1.OwnerId != "user-owner" || m1.Size != 8192 {
		t.Fatalf("unexpected manifest record: %+v", m1)
	}

	// Verify chunk locations initialized with RefCount 1
	c1, err := s.GetChunkLocation(ctx, chunk1)
	if err != nil || c1.RefCount != 1 || c1.OrphanedAt != nil {
		t.Fatalf("chunk1 expected RefCount 1, nil OrphanedAt, got: %+v, err: %v", c1, err)
	}
	c2, err := s.GetChunkLocation(ctx, chunk2)
	if err != nil || c2.RefCount != 1 || c2.OrphanedAt != nil {
		t.Fatalf("chunk2 expected RefCount 1, nil OrphanedAt, got: %+v, err: %v", c2, err)
	}

	// 2. Overwrite manifest: retains chunk2, replaces chunk1 with chunk3
	m2, err := s.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:      "my-bucket",
		Key:         "data.bin",
		Size:        12000,
		Etag:        "etag-2",
		ContentType: "application/octet-stream",
		ChunkIds:    []string{chunk2, chunk3},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: chunk2, NodeAddresses: []string{"node3:9101"}, Size: 4096},
			{ChunkHash: chunk3, NodeAddresses: []string{"node1:9101"}, Size: 7904},
		},
	})
	if err != nil {
		t.Fatalf("failed to overwrite manifest: %v", err)
	}
	if m2.Size != 12000 || m2.Etag != "etag-2" {
		t.Fatalf("unexpected overwritten manifest: %+v", m2)
	}

	// Displaced chunk1: RefCount == 0, OrphanedAt != nil
	c1After, err := s.GetChunkLocation(ctx, chunk1)
	if err != nil {
		t.Fatalf("failed to get chunk1 location: %v", err)
	}
	if c1After.RefCount != 0 || c1After.OrphanedAt == nil {
		t.Fatalf("displaced chunk1 expected RefCount 0, OrphanedAt != nil, got: %+v", c1After)
	}

	// Retained chunk2: RefCount >= 1, unioned node addresses
	c2After, err := s.GetChunkLocation(ctx, chunk2)
	if err != nil || c2After.RefCount < 1 {
		t.Fatalf("retained chunk2 expected RefCount >= 1, got: %+v, err: %v", c2After, err)
	}
	if len(c2After.Nodes) != 2 {
		t.Fatalf("expected chunk2 to have 2 unioned nodes, got %v", c2After.Nodes)
	}

	// New chunk3: RefCount == 1, OrphanedAt == nil
	c3After, err := s.GetChunkLocation(ctx, chunk3)
	if err != nil || c3After.RefCount != 1 || c3After.OrphanedAt != nil {
		t.Fatalf("new chunk3 expected RefCount 1, nil OrphanedAt, got: %+v, err: %v", c3After, err)
	}
}

func TestStore_DeleteManifest(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	chunk := "del-chunk-1"
	_, err := s.CreateBucket(ctx, "media", "user-1", nil)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	_, err = s.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:   "media",
		Key:      "photo.jpg",
		Size:     1024,
		ChunkIds: []string{chunk},
	})
	if err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}

	// Delete manifest
	err = s.DeleteManifest(ctx, "media", "photo.jpg")
	if err != nil {
		t.Fatalf("failed to delete manifest: %v", err)
	}

	// Subsequent GetManifest returns ErrNotFound
	_, err = s.GetManifest(ctx, "media", "photo.jpg")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted manifest, got: %v", err)
	}

	// Associated chunk refcount dropped to 0, OrphanedAt set
	c, err := s.GetChunkLocation(ctx, chunk)
	if err != nil {
		t.Fatalf("failed to get chunk location: %v", err)
	}
	if c.RefCount != 0 || c.OrphanedAt == nil {
		t.Fatalf("expected RefCount 0 and OrphanedAt set, got %+v", c)
	}

	// Deleting again returns ErrNotFound
	err = s.DeleteManifest(ctx, "media", "photo.jpg")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on duplicate delete, got: %v", err)
	}
}

func TestStore_MultipartLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	uploadID := "upload-session-123"
	_, err := s.CreateBucket(ctx, "large-bucket", "user-mp", nil)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// 1. Initiate Multipart
	mp, err := s.InitiateMultipart(ctx, uploadID, &castorv1.InitiateMultipartMetaRequest{
		Bucket:      "large-bucket",
		Key:         "archive.zip",
		ContentType: "application/zip",
	})
	if err != nil {
		t.Fatalf("failed to initiate multipart: %v", err)
	}
	if mp.Status != "pending" || mp.UploadId != uploadID {
		t.Fatalf("unexpected multipart record: %+v", mp)
	}

	// 2. Commit Part 1 and Part 2
	p1, err := s.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   uploadID,
		PartNumber: 1,
		Size:       5000,
		Etag:       "etag-p1",
		ChunkIds:   []string{"p1-chunk"},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: "p1-chunk", NodeAddresses: []string{"node1:9101"}, Size: 5000},
		},
	})
	if err != nil {
		t.Fatalf("failed to commit part 1: %v", err)
	}
	if p1.PartNumber != 1 {
		t.Fatalf("unexpected part 1: %+v", p1)
	}

	p2, err := s.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   uploadID,
		PartNumber: 2,
		Size:       6000,
		Etag:       "etag-p2",
		ChunkIds:   []string{"p2-chunk"},
		ChunkPlacements: []*castorv1.ChunkPlacement{
			{ChunkHash: "p2-chunk", NodeAddresses: []string{"node2:9101"}, Size: 6000},
		},
	})
	if err != nil {
		t.Fatalf("failed to commit part 2: %v", err)
	}
	if p2.PartNumber != 2 {
		t.Fatalf("unexpected part 2: %+v", p2)
	}

	// Verify parts listed
	parts, err := s.ListParts(ctx, uploadID)
	if err != nil || len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d, err: %v", len(parts), err)
	}

	// 3. Complete Multipart
	m, err := s.CompleteMultipart(ctx, &castorv1.CompleteMultipartMetaRequest{
		UploadId: uploadID,
		Parts: []*castorv1.PartSummary{
			{PartNumber: 1, Etag: "etag-p1"},
			{PartNumber: 2, Etag: "etag-p2"},
		},
	})
	if err != nil {
		t.Fatalf("failed to complete multipart: %v", err)
	}
	if m.Status != "committed" || m.Size != 11000 || len(m.ChunkIds) != 2 {
		t.Fatalf("unexpected final manifest: %+v", m)
	}

	// Parts should be cleared
	partsAfter, err := s.ListParts(ctx, uploadID)
	if err != nil || len(partsAfter) != 0 {
		t.Fatalf("expected parts to be deleted after completion, got %d", len(partsAfter))
	}

	// Multipart record status should be "completed"
	mpAfter, err := s.GetMultipart(ctx, uploadID)
	if err != nil || mpAfter.Status != "completed" {
		t.Fatalf("expected status completed, got %+v, err: %v", mpAfter, err)
	}
}

func TestStore_AbortMultipart(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	uploadID := "upload-abort-456"
	chunk := "abort-chunk-1"

	_, err := s.CreateBucket(ctx, "files", "user-1", nil)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	_, err = s.InitiateMultipart(ctx, uploadID, &castorv1.InitiateMultipartMetaRequest{
		Bucket: "files",
		Key:    "discard.bin",
	})
	if err != nil {
		t.Fatalf("failed to initiate multipart: %v", err)
	}

	_, err = s.CommitPart(ctx, &castorv1.CommitPartMetaRequest{
		UploadId:   uploadID,
		PartNumber: 1,
		Size:       4096,
		Etag:       "etag-ab",
		ChunkIds:   []string{chunk},
	})
	if err != nil {
		t.Fatalf("failed to commit part: %v", err)
	}

	// Verify chunk location refcount is 1
	c, err := s.GetChunkLocation(ctx, chunk)
	if err != nil || c.RefCount != 1 {
		t.Fatalf("expected chunk refcount 1, got %+v, err: %v", c, err)
	}

	// Abort multipart
	err = s.AbortMultipart(ctx, uploadID)
	if err != nil {
		t.Fatalf("failed to abort multipart: %v", err)
	}

	// Status should be aborted
	mp, err := s.GetMultipart(ctx, uploadID)
	if err != nil || mp.Status != "aborted" {
		t.Fatalf("expected status aborted, got %+v, err: %v", mp, err)
	}

	// Chunks should have refcount decremented to 0 and orphaned_at populated
	cAfter, err := s.GetChunkLocation(ctx, chunk)
	if err != nil || cAfter.RefCount != 0 || cAfter.OrphanedAt == nil {
		t.Fatalf("expected chunk refcount 0 and non-nil OrphanedAt, got %+v, err: %v", cAfter, err)
	}

	// Parts should be cleaned up
	parts, err := s.ListParts(ctx, uploadID)
	if err != nil || len(parts) != 0 {
		t.Fatalf("expected 0 parts remaining after abort, got %d", len(parts))
	}
}

func TestStore_ChunkLocationTransactions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	hash := "loc-chunk-xyz"

	// 1. Update Chunk Location creates record if not existing
	err := s.UpdateChunkLocation(ctx, hash, []string{"node1:9101"})
	if err != nil {
		t.Fatalf("failed to update chunk location: %v", err)
	}

	loc, err := s.GetChunkLocation(ctx, hash)
	if err != nil || len(loc.Nodes) != 1 || loc.Nodes[0] != "node1:9101" {
		t.Fatalf("unexpected chunk location: %+v, err: %v", loc, err)
	}

	// 2. Update with second node unions addresses
	err = s.UpdateChunkLocation(ctx, hash, []string{"node2:9101", "node1:9101"})
	if err != nil {
		t.Fatalf("failed to union chunk locations: %v", err)
	}

	loc, err = s.GetChunkLocation(ctx, hash)
	if err != nil || len(loc.Nodes) != 2 {
		t.Fatalf("expected 2 unioned nodes, got %+v", loc)
	}

	// 3. Remove single node
	err = s.RemoveChunkLocations(ctx, hash, []string{"node1:9101"})
	if err != nil {
		t.Fatalf("failed to remove node: %v", err)
	}
	loc, err = s.GetChunkLocation(ctx, hash)
	if err != nil || len(loc.Nodes) != 1 || loc.Nodes[0] != "node2:9101" {
		t.Fatalf("expected only node2 remaining, got %+v", loc)
	}

	// 4. Remove all nodes: removes record
	err = s.RemoveChunkLocations(ctx, hash, nil)
	if err != nil {
		t.Fatalf("failed to remove all locations: %v", err)
	}
	_, err = s.GetChunkLocation(ctx, hash)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after removing all locations, got: %v", err)
	}
}

var _ = time.Now
