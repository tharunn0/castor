package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

const (
	tmpSuffix = ".tmp"
)

type Storage struct {
	rootDir string

	chunkPath   string
	stagingPath string

	MaxChunkSize int
}

func New(rootDir string, size int) (*Storage, error) {

	stagingDir := filepath.Join(rootDir, "staging")
	chunkDir := filepath.Join(rootDir, "chunk")

	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return nil, fmt.Errorf("create staging directory: %w", err)
	}

	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return nil, fmt.Errorf("create chunks directory: %w", err)
	}
	return &Storage{
		rootDir, chunkDir, stagingDir, size,
	}, nil
}

type WriteResult struct {
	Hash string

	Path string

	Size int
}

type ReadResult struct {
	io.Reader

	io.Closer

	Hash string

	Path string

	Data []byte

	Size int
}

func (s *Storage) WriteChunk(r io.Reader, hashHex string) (*WriteResult, error) {

	// build the staging dir path
	stagingPath := filepath.Join(s.stagingPath, uuid.New().String()+tmpSuffix)

	f, err := os.Create(stagingPath)
	if err != nil {
		log.Println("staging file create error :", err)
		return nil, err
	}
	defer func() {
		f.Close()
		os.Remove(stagingPath) // no-op if renamed
	}()

	// ioLimitReader for limiting s.MaxChunkSize reads
	lReader := io.LimitReader(r, int64(s.MaxChunkSize)+1)

	hasher := sha256.New()
	// create multiWriter to write in to a hasher + file
	mw := io.MultiWriter(f, hasher)

	// use ioCopy to copy data from the source to multiWriter

	n, err := io.Copy(mw, lReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk body : %v", err)
	}

	if n > int64(s.MaxChunkSize) {
		return nil, ErrChunkSizeTooLarge
	}

	hashb := hasher.Sum(nil)
	computedHash := hex.EncodeToString(hashb[:])

	if computedHash != hashHex {
		log.Printf("expected %s , got %s\n", hashHex, computedHash)
		return nil, fmt.Errorf("hash mismatch: \n")
	}

	// atomic rename to /data/chunks/xx/hash
	finalPath := chunkPath(s.chunkPath, computedHash)

	err = os.Rename(stagingPath, finalPath)
	if err != nil {
		log.Println("rename error :", err)
		return nil, err
	}

	if dir, err := os.Open(filepath.Dir(finalPath)); err == nil {
		dir.Sync()
		dir.Close()
	}

	return &WriteResult{
		Hash: computedHash,
		Path: finalPath,
		Size: int(n),
	}, nil
}

func (s *Storage) ReadChunk(hash string) (io.ReadCloser, error) {

	// build target path
	chunkPath := filepath.Join(s.chunkPath, string(hash[:2]), hash)

	if _, err := os.Stat(chunkPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrChunkNotExist
		}
		return nil, err
	}

	f, err := os.Open(chunkPath)
	if err != nil {
		return nil, err
	}

	return &ReadResult{
		Reader: f,
		Closer: f,
	}, nil
}

func chunkPath(path, hashHex string) string {

	os.MkdirAll(filepath.Join(path, hashHex[:2]), 0755)

	if len(hashHex) < 2 {
		return ""
	}

	shard := string(hashHex[:2])

	return filepath.Join(path, shard, "/", hashHex)
}

// generate sha256 and return hex encoded string
func hashGen(data []byte) string {

	b := sha256.Sum256(data)

	return hex.EncodeToString(b[:])
}

var (
	ErrChunkNotExist = errors.New("Chunk does not exist")

	ErrChunkSizeTooLarge = errors.New("Chunk size is too large")

	ErrHashMismatch = errors.New("expected and computed hash do not match")
)
