package storage

import (
	"bytes"
	"crypto/rand"
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

// func TestWriteChunk_EmptyHash(t *testing.T) {
// 	data := []byte("some chunk data")

// 	path := filepath.Join(t.TempDir(), "chunk")

// 	err := WriteChunk(path, data, "")
// 	if err == nil {
// 		t.Fatal("WriteChunk() expected error for empty hash, got nil")
// 	}
// }
