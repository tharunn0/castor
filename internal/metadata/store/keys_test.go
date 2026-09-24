package store

import (
	"bytes"
	"sort"
	"testing"
)

func TestBucketKeys(t *testing.T) {
	key := BucketKey("photos")
	if string(key) != "bucket:photos" {
		t.Fatalf("unexpected key: %s", key)
	}

	bucket, err := ParseBucketKey(key)
	if err != nil {
		t.Fatalf("unexpected error parsing bucket key: %v", err)
	}
	if bucket != "photos" {
		t.Fatalf("expected photos, got %s", bucket)
	}

	// Error cases
	if _, err := ParseBucketKey([]byte("manifest:photos")); err == nil {
		t.Fatal("expected error on mismatched prefix")
	}
	if _, err := ParseBucketKey([]byte("bucket:")); err == nil {
		t.Fatal("expected error on empty bucket")
	}
}

func TestManifestKeys(t *testing.T) {
	key := ManifestKey("photos", "vacation/beach.png")
	if string(key) != "manifest:photos:vacation/beach.png" {
		t.Fatalf("unexpected key: %s", key)
	}

	bucket, objKey, err := ParseManifestKey(key)
	if err != nil {
		t.Fatalf("unexpected error parsing manifest key: %v", err)
	}
	if bucket != "photos" || objKey != "vacation/beach.png" {
		t.Fatalf("unexpected parsed result: %s, %s", bucket, objKey)
	}

	// Error cases
	if _, _, err := ParseManifestKey([]byte("manifest:photos")); err == nil {
		t.Fatal("expected error on incomplete manifest key")
	}
	if _, _, err := ParseManifestKey([]byte("other:photos:beach.png")); err == nil {
		t.Fatal("expected error on mismatched prefix")
	}
}

func TestChunkKeys(t *testing.T) {
	hash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	key := ChunkKey(hash)
	if string(key) != "chunk:"+hash {
		t.Fatalf("unexpected key: %s", key)
	}

	parsedHash, err := ParseChunkKey(key)
	if err != nil {
		t.Fatalf("unexpected error parsing chunk key: %v", err)
	}
	if parsedHash != hash {
		t.Fatalf("expected %s, got %s", hash, parsedHash)
	}

	// Error cases
	if _, err := ParseChunkKey([]byte("chunk:")); err == nil {
		t.Fatal("expected error on empty hash")
	}
	if _, err := ParseChunkKey([]byte("invalid:abc")); err == nil {
		t.Fatal("expected error on invalid prefix")
	}
}

func TestMultipartKeys(t *testing.T) {
	uploadID := "upload-123"
	key := MultipartKey(uploadID)
	if string(key) != "multipart:upload-123" {
		t.Fatalf("unexpected key: %s", key)
	}

	parsedID, err := ParseMultipartKey(key)
	if err != nil {
		t.Fatalf("unexpected error parsing multipart key: %v", err)
	}
	if parsedID != uploadID {
		t.Fatalf("expected %s, got %s", uploadID, parsedID)
	}

	// Error cases
	if _, err := ParseMultipartKey([]byte("multipart:")); err == nil {
		t.Fatal("expected error on empty upload ID")
	}
}

func TestMultipartPartKeys_SortingAndParsing(t *testing.T) {
	uploadID := "upload-xyz"

	// Check zero padding format
	part1 := MultipartPartKey(uploadID, 1)
	part10 := MultipartPartKey(uploadID, 10)
	part2 := MultipartPartKey(uploadID, 2)
	part100 := MultipartPartKey(uploadID, 100)

	expectedPart1 := "multipart_part:upload-xyz:00001"
	if string(part1) != expectedPart1 {
		t.Fatalf("expected %s, got %s", expectedPart1, string(part1))
	}

	// Test lexicographical sorting (byte order)
	keys := [][]byte{part100, part1, part10, part2}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	expectedOrder := []int32{1, 2, 10, 100}
	for i, k := range keys {
		parsedUploadID, partNum, err := ParseMultipartPartKey(k)
		if err != nil {
			t.Fatalf("failed to parse part key %s: %v", k, err)
		}
		if parsedUploadID != uploadID {
			t.Fatalf("expected uploadID %s, got %s", uploadID, parsedUploadID)
		}
		if partNum != expectedOrder[i] {
			t.Fatalf("expected part %d at index %d, got %d", expectedOrder[i], i, partNum)
		}
	}

	// Error cases
	if _, _, err := ParseMultipartPartKey([]byte("multipart_part:upload-xyz:invalid")); err == nil {
		t.Fatal("expected error on non-integer part number")
	}
	if _, _, err := ParseMultipartPartKey([]byte("multipart_part:upload-xyz")); err == nil {
		t.Fatal("expected error on missing part number separator")
	}
}
