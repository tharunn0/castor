package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open("", true)
	if err != nil {
		t.Fatalf("failed to open in-memory store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

func TestStore_BucketQueries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// 1. Initial empty checks
	exists, err := s.CheckBucketExists(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("expected bucket to not exist")
	}

	_, err = s.GetBucket(ctx, "my-bucket")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// 2. Insert test buckets directly via Badger transaction
	b1 := &castorv1.BucketRecord{Name: "alpha", CreatedAt: timestamppb.Now()}
	b2 := &castorv1.BucketRecord{Name: "beta", CreatedAt: timestamppb.Now()}
	bDeleted := &castorv1.BucketRecord{Name: "gamma", IsDeleted: true}

	err = s.DB().Update(func(txn *badger.Txn) error {
		for _, b := range []*castorv1.BucketRecord{b1, b2, bDeleted} {
			val, _ := proto.Marshal(b)
			if err := txn.Set(BucketKey(b.Name), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed buckets: %v", err)
	}

	// 3. Test GetBucket & CheckBucketExists
	gotB1, err := s.GetBucket(ctx, "alpha")
	if err != nil {
		t.Fatalf("failed to get bucket alpha: %v", err)
	}
	if gotB1.Name != "alpha" {
		t.Fatalf("expected alpha, got %s", gotB1.Name)
	}

	exists, err = s.CheckBucketExists(ctx, "alpha")
	if err != nil || !exists {
		t.Fatalf("expected bucket alpha to exist, err: %v", err)
	}

	// Deleted bucket should return ErrNotFound
	_, err = s.GetBucket(ctx, "gamma")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted bucket gamma, got %v", err)
	}

	// 4. Test ListBuckets (should exclude deleted bucket gamma)
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("failed to list buckets: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("expected 2 active buckets, got %d", len(buckets))
	}
}

func TestStore_ManifestQueries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Seed objects
	m1 := &castorv1.ManifestRecord{
		Bucket:      "my-bucket",
		Key:         "photos/2026/img1.png",
		Size:        100,
		Status:      "committed",
		ContentType: "image/png",
	}
	m2 := &castorv1.ManifestRecord{
		Bucket:      "my-bucket",
		Key:         "photos/2026/img2.png",
		Size:        200,
		Status:      "committed",
		ContentType: "image/png",
	}
	m3 := &castorv1.ManifestRecord{
		Bucket:      "my-bucket",
		Key:         "notes.txt",
		Size:        50,
		Status:      "committed",
		ContentType: "text/plain",
	}
	mDeleted := &castorv1.ManifestRecord{
		Bucket: "my-bucket",
		Key:    "deleted.txt",
		Status: "deleted",
	}

	err := s.DB().Update(func(txn *badger.Txn) error {
		for _, m := range []*castorv1.ManifestRecord{m1, m2, m3, mDeleted} {
			val, _ := proto.Marshal(m)
			if err := txn.Set(ManifestKey(m.Bucket, m.Key), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed manifests: %v", err)
	}

	// Test GetManifest
	gotM1, err := s.GetManifest(ctx, "my-bucket", "photos/2026/img1.png")
	if err != nil {
		t.Fatalf("failed to get manifest: %v", err)
	}
	if gotM1.Key != "photos/2026/img1.png" || gotM1.Size != 100 {
		t.Fatalf("manifest data mismatch: %v", gotM1)
	}

	// Deleted manifest should return ErrNotFound
	_, err = s.GetManifest(ctx, "my-bucket", "deleted.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted manifest, got %v", err)
	}

	// Test ListManifests (Flat)
	res, err := s.ListManifests(ctx, "my-bucket", "", "", "", 10)
	if err != nil {
		t.Fatalf("failed to list manifests: %v", err)
	}
	if len(res.Manifests) != 3 {
		t.Fatalf("expected 3 manifests, got %d", len(res.Manifests))
	}

	// Test ListManifests with prefix
	resPrefix, err := s.ListManifests(ctx, "my-bucket", "photos/", "", "", 10)
	if err != nil {
		t.Fatalf("failed to list manifests with prefix: %v", err)
	}
	if len(resPrefix.Manifests) != 2 {
		t.Fatalf("expected 2 manifests under photos/, got %d", len(resPrefix.Manifests))
	}

	// Test ListManifests with delimiter (folder hierarchy)
	resDelim, err := s.ListManifests(ctx, "my-bucket", "", "/", "", 10)
	if err != nil {
		t.Fatalf("failed to list manifests with delimiter: %v", err)
	}
	if len(resDelim.CommonPrefixes) != 1 || resDelim.CommonPrefixes[0] != "photos/" {
		t.Fatalf("expected common prefix 'photos/', got %v", resDelim.CommonPrefixes)
	}
	if len(resDelim.Manifests) != 1 || resDelim.Manifests[0].Key != "notes.txt" {
		t.Fatalf("expected root manifest 'notes.txt', got %v", resDelim.Manifests)
	}
}

func TestStore_ChunkQueries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c1 := &castorv1.ChunkLocationRecord{
		ChunkHash: "hash-1",
		Size:      4096,
		Nodes:     []string{"10.0.0.1:9101", "10.0.0.2:9102"},
		RefCount:  2,
	}
	cZero := &castorv1.ChunkLocationRecord{
		ChunkHash:  "hash-zero",
		Size:       4096,
		Nodes:      []string{"10.0.0.1:9101"},
		RefCount:   0,
		OrphanedAt: timestamppb.New(time.Now().Add(-25 * time.Hour)), // past 24h quarantine
	}

	err := s.DB().Update(func(txn *badger.Txn) error {
		for _, c := range []*castorv1.ChunkLocationRecord{c1, cZero} {
			val, _ := proto.Marshal(c)
			if err := txn.Set(ChunkKey(c.ChunkHash), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed chunks: %v", err)
	}

	// Test GetChunkLocation
	gotC1, err := s.GetChunkLocation(ctx, "hash-1")
	if err != nil {
		t.Fatalf("failed to get chunk: %v", err)
	}
	if gotC1.RefCount != 2 || len(gotC1.Nodes) != 2 {
		t.Fatalf("chunk data mismatch: %v", gotC1)
	}

	// Test CheckChunks deduplication
	existsMap, err := s.CheckChunks(ctx, []string{"hash-1", "hash-zero", "hash-nonexistent"})
	if err != nil {
		t.Fatalf("check chunks failed: %v", err)
	}
	if !existsMap["hash-1"] {
		t.Fatal("expected hash-1 to exist (ref_count > 0)")
	}
	if existsMap["hash-zero"] {
		t.Fatal("expected hash-zero to be false (ref_count == 0)")
	}
	if existsMap["hash-nonexistent"] {
		t.Fatal("expected nonexistent chunk to be false")
	}

	// Test ScanOrphanedChunks (24h quarantine check)
	orphans, err := s.ScanOrphanedChunks(ctx, 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("scan orphaned chunks failed: %v", err)
	}
	if len(orphans) != 1 || orphans[0].ChunkHash != "hash-zero" {
		t.Fatalf("expected hash-zero as orphaned, got: %v", orphans)
	}

	// Test ScanUnderReplicatedChunks (< 3 replicas)
	underReplicated, err := s.ScanUnderReplicatedChunks(ctx, 3, 10)
	if err != nil {
		t.Fatalf("scan under-replicated chunks failed: %v", err)
	}
	if len(underReplicated) != 1 || underReplicated[0].ChunkHash != "hash-1" {
		t.Fatalf("expected hash-1 as under-replicated, got: %v", underReplicated)
	}
}

func TestStore_MultipartQueries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	uploadID := "upload-abc-123"
	mp := &castorv1.MultipartRecord{
		UploadId:  uploadID,
		Bucket:    "my-bucket",
		Key:       "large.iso",
		Status:    "pending",
		CreatedAt: timestamppb.Now(),
	}

	part1 := &castorv1.PartRecord{
		UploadId:   uploadID,
		PartNumber: 1,
		Size:       5242880,
		Etag:       "etag-part-1",
	}
	part2 := &castorv1.PartRecord{
		UploadId:   uploadID,
		PartNumber: 2,
		Size:       5242880,
		Etag:       "etag-part-2",
	}

	err := s.DB().Update(func(txn *badger.Txn) error {
		val, _ := proto.Marshal(mp)
		if err := txn.Set(MultipartKey(uploadID), val); err != nil {
			return err
		}
		valP1, _ := proto.Marshal(part1)
		if err := txn.Set(MultipartPartKey(uploadID, 1), valP1); err != nil {
			return err
		}
		valP2, _ := proto.Marshal(part2)
		if err := txn.Set(MultipartPartKey(uploadID, 2), valP2); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed multipart data: %v", err)
	}

	// Test GetMultipart
	gotMP, err := s.GetMultipart(ctx, uploadID)
	if err != nil {
		t.Fatalf("failed to get multipart: %v", err)
	}
	if gotMP.UploadId != uploadID || gotMP.Key != "large.iso" {
		t.Fatalf("multipart mismatch: %v", gotMP)
	}

	// Test GetPart
	gotP2, err := s.GetPart(ctx, uploadID, 2)
	if err != nil {
		t.Fatalf("failed to get part 2: %v", err)
	}
	if gotP2.PartNumber != 2 || gotP2.Etag != "etag-part-2" {
		t.Fatalf("part 2 mismatch: %v", gotP2)
	}

	// Test ListParts
	parts, err := s.ListParts(ctx, uploadID)
	if err != nil {
		t.Fatalf("failed to list parts: %v", err)
	}
	if len(parts) != 2 || parts[0].PartNumber != 1 || parts[1].PartNumber != 2 {
		t.Fatalf("list parts mismatch: %v", parts)
	}
}
