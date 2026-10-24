package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tharunn0/castor/internal/data/telemetry"
	telpkg "github.com/tharunn0/castor/internal/telemetry"
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

	res, err := store.WriteChunk(context.Background(), data, expectedHash)
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

	_, err := store.WriteChunk(context.Background(), data, exHash)
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

	res, err := store.WriteChunk(context.Background(), data, expectedHash)

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

func TestWriteChunk_Telemetry(t *testing.T) {
	tmpDir := t.TempDir()
	reg := prometheus.NewRegistry()
	metrics := telemetry.NewDataMetrics("test-node", &telpkg.Metrics{Registry: reg})

	store, err := New(tmpDir, MaxChunkSize, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	data, expectedHash, err := randomChunk(1024 * 64)
	if err != nil {
		t.Fatalf("randomChunk() failed: %v", err)
	}

	res, err := store.WriteChunk(context.Background(), data, expectedHash)
	if err != nil {
		t.Fatalf("WriteChunk() failed: %v", err)
	}
	if res.Hash != expectedHash {
		t.Fatalf("expected hash %q, got %q", expectedHash, res.Hash)
	}

	successCount := testutil.ToFloat64(metrics.ChunkWritesTotal.WithLabelValues("success"))
	if successCount != 1 {
		t.Errorf("expected success count 1, got %f", successCount)
	}
}

func TestWriteChunk_ChecksumMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	reg := prometheus.NewRegistry()
	metrics := telemetry.NewDataMetrics("test-node", &telpkg.Metrics{Registry: reg})

	store, err := New(tmpDir, MaxChunkSize, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	data, _, err := randomChunk(1024 * 64)
	if err != nil {
		t.Fatalf("randomChunk() failed: %v", err)
	}

	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = store.WriteChunk(context.Background(), data, wrongHash)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("expected ErrHashMismatch, got %v", err)
	}

	mismatches := testutil.ToFloat64(metrics.ChecksumMismatchesTotal)
	if mismatches != 1 {
		t.Errorf("expected mismatches count 1, got %f", mismatches)
	}

	errorCount := testutil.ToFloat64(metrics.ChunkWritesTotal.WithLabelValues("error"))
	if errorCount != 1 {
		t.Errorf("expected error count 1, got %f", errorCount)
	}
}

func TestChunkExists(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := New(tmpDir, MaxChunkSize)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	const nonExistentHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if store.ChunkExists(nonExistentHash) {
		t.Fatal("expected ChunkExists = false for non-existent hash")
	}

	invalidHashes := []string{
		"",
		"1234",
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8555",
	}
	for _, hash := range invalidHashes {
		if store.ChunkExists(hash) {
			t.Errorf("expected ChunkExists = false for invalid hash %q", hash)
		}
	}

	data, expectedHash, err := randomChunk(MaxChunkSize)
	if err != nil {
		t.Fatalf("randomChunk() failed: %v", err)
	}

	if _, err := store.WriteChunk(context.Background(), data, expectedHash); err != nil {
		t.Fatalf("WriteChunk() failed: %v", err)
	}

	if !store.ChunkExists(expectedHash) {
		t.Fatal("expected ChunkExists = true after chunk is written")
	}

	if _, err := store.DeleteChunk(expectedHash); err != nil {
		t.Fatalf("DeleteChunk() failed: %v", err)
	}

	if store.ChunkExists(expectedHash) {
		t.Fatal("expected ChunkExists = false after chunk is deleted")
	}
}

func TestScanChunks(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := New(tmpDir, MaxChunkSize)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// 1. Write 3 valid chunks
	expectedChunks := make(map[string]bool)
	for range 3 {
		data, hash, err := randomChunk(1024 * 64)
		if err != nil {
			t.Fatalf("randomChunk() failed: %v", err)
		}
		if _, err := store.WriteChunk(context.Background(), data, hash); err != nil {
			t.Fatalf("WriteChunk() failed: %v", err)
		}
		expectedChunks[hash] = true
	}

	// 2. Add files that should be ignored by ScanChunks
	// A non-directory file in chunk directory
	_ = os.WriteFile(filepath.Join(store.chunkPath, "stray_file.txt"), []byte("stray"), 0644)
	// An invalid shard directory name (not 2 chars)
	invalidShardDir := filepath.Join(store.chunkPath, "xyz")
	_ = os.MkdirAll(invalidShardDir, 0755)
	_ = os.WriteFile(filepath.Join(invalidShardDir, "dummy"), []byte("dummy"), 0644)

	// In a valid shard directory, create corrupt and tmp files
	var sampleHash string
	for h := range expectedChunks {
		sampleHash = h
		break
	}
	shardDir := filepath.Join(store.chunkPath, sampleHash[:2])
	_ = os.WriteFile(filepath.Join(shardDir, sampleHash+".corrupt"), []byte("corrupt"), 0644)
	_ = os.WriteFile(filepath.Join(shardDir, "short.tmp"), []byte("tmp"), 0644)
	_ = os.WriteFile(filepath.Join(shardDir, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), []byte("nonhex"), 0644)
	_ = os.MkdirAll(filepath.Join(shardDir, "subfolder"), 0755)

	// 3. ScanChunks should only find the 3 valid chunks
	scannedChunks := make(map[string]string)
	err = store.ScanChunks(context.Background(), func(hash, path string) error {
		scannedChunks[hash] = path
		return nil
	})
	if err != nil {
		t.Fatalf("ScanChunks() failed: %v", err)
	}

	if len(scannedChunks) != len(expectedChunks) {
		t.Fatalf("expected %d chunks, got %d", len(expectedChunks), len(scannedChunks))
	}
	for h := range expectedChunks {
		if _, ok := scannedChunks[h]; !ok {
			t.Errorf("expected chunk %s to be visited", h)
		}
	}

	// 4. Test error propagation from callback
	sentinelErr := errors.New("abort iteration")
	err = store.ScanChunks(context.Background(), func(hash, path string) error {
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got: %v", err)
	}

	// 5. Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = store.ScanChunks(ctx, func(hash, path string) error {
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}
