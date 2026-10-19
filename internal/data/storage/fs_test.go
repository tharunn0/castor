package storage

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"testing"
)

const MaxChunkSize = 4 << 20

// randomChunk returns an io.Reader containing n bytes of cryptographically random data.
func randomChunk(n int) (io.Reader, string, error) {

	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return nil, "", err
	}

	return bytes.NewReader(buf), hashGen(buf), nil
}

func TestWriteChunk_Success(t *testing.T) {

	tmpDir := os.TempDir()

	data, expectedHash, _ := randomChunk(MaxChunkSize)

	store, err := New(tmpDir, MaxChunkSize)
	if err != nil {
		t.Fatalf("Failed to initialize storage : %v", err)
	}

	res, err := store.WriteChunk(data, expectedHash)
	if err != nil {
		t.Fatalf("WriteChunk() unexpected error: %v", err)
	}

	written, err := os.ReadFile(res.Path)
	if err != nil {
		t.Fatalf("failed to read written chunk: %v", err)
	}

	computedHash := hashGen(written)

	if computedHash != expectedHash {
		t.Errorf(
			"written chunk hash mismatch: got %q, want %q",
			computedHash,
			expectedHash,
		)
	}
}

func TestWriteChunk_ChunkTooLarge(t *testing.T) {
	// 4 MiB + 1 byte
	data, exHash, _ := randomChunk(MaxChunkSize + 1000)

	tmpDir := os.TempDir()

	store, _ := New(tmpDir, MaxChunkSize)

	_, err := store.WriteChunk(data, exHash)
	if err == nil {
		t.Fatal("WriteChunk() expected error for chunk larger than 4 MiB, got nil")
	}
}

func TestDeleteChunk_Success(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := New(tmpDir, MaxChunkSize)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	data, expectedHash, err := randomChunk(MaxChunkSize)
	if err != nil {
		t.Fatalf("randomChunk() failed: %v", err)
	}

	res, err := store.WriteChunk(data, expectedHash)
	if err != nil {
		t.Fatalf("WriteChunk() failed: %v", err)
	}

	if _, err := os.Stat(res.Path); err != nil {
		t.Fatalf("expected chunk file to exist at %s: %v", res.Path, err)
	}

	deleted, err := store.DeleteChunk(expectedHash)
	if err != nil {
		t.Fatalf("DeleteChunk() unexpected error: %v", err)
	}
	if !deleted {
		t.Fatal("expected deleted = true for existing chunk")
	}

	if _, err := os.Stat(res.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected file to be removed, got err: %v", err)
	}

	if _, err := store.ReadChunk(expectedHash); !errors.Is(err, ErrChunkNotExist) {
		t.Fatalf("expected ErrChunkNotExist, got: %v", err)
	}

	deletedAgain, err := store.DeleteChunk(expectedHash)
	if err != nil {
		t.Fatalf("second DeleteChunk() unexpected error: %v", err)
	}
	if deletedAgain {
		t.Fatal("expected deleted = false on subsequent delete of same chunk")
	}
}

func TestDeleteChunk_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := New(tmpDir, MaxChunkSize)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	const nonExistentHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	deleted, err := store.DeleteChunk(nonExistentHash)
	if err != nil {
		t.Fatalf("DeleteChunk() unexpected error: %v", err)
	}
	if deleted {
		t.Fatal("expected deleted = false for non-existent chunk")
	}
}

func TestDeleteChunk_InvalidHash(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := New(tmpDir, MaxChunkSize)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	invalidHashes := []string{
		"",
		"abcd",
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8555",
	}

	for _, hash := range invalidHashes {
		deleted, err := store.DeleteChunk(hash)
		if !errors.Is(err, ErrInvalidHash) {
			t.Errorf("expected ErrInvalidHash for hash %q, got: %v", hash, err)
		}
		if deleted {
			t.Errorf("expected deleted = false for invalid hash %q", hash)
		}
	}
}
