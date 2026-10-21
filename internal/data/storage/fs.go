package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/data/telemetry"
)

const (
	tmpSuffix = ".tmp"
)

type Storage struct {
	rootDir string

	chunkPath   string
	stagingPath string

	MaxChunkSize int

	logger  *slog.Logger
	metrics *telemetry.DataMetrics
}

type Option func(*Storage)

func WithLogger(logger *slog.Logger) Option {
	return func(s *Storage) {
		s.logger = logger
	}
}

func WithMetrics(metrics *telemetry.DataMetrics) Option {
	return func(s *Storage) {
		s.metrics = metrics
	}
}

func New(rootDir string, size int, opts ...Option) (*Storage, error) {
	stagingDir := filepath.Join(rootDir, "staging")
	chunkDir := filepath.Join(rootDir, "chunk")

	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return nil, fmt.Errorf("create staging directory: %w", err)
	}

	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return nil, fmt.Errorf("create chunks directory: %w", err)
	}

	s := &Storage{
		rootDir:      rootDir,
		chunkPath:    chunkDir,
		stagingPath:  stagingDir,
		MaxChunkSize: size,
		logger:       slog.Default(),
	}

	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}

	return s, nil
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

func (s *Storage) WriteChunk(ctx context.Context, r io.Reader, hashHex string) (*WriteResult, error) {
	start := time.Now()

	stagingPath := filepath.Join(s.stagingPath, uuid.New().String()+tmpSuffix)

	f, err := os.Create(stagingPath)
	if err != nil {
		if s.metrics != nil && s.metrics.ChunkWritesTotal != nil {
			s.metrics.ChunkWritesTotal.WithLabelValues("error").Inc()
		}
		s.logger.Error("staging file create/write error", "error", err)
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			s.logger.Error("staging file close error", "error", err)
		}
		_ = os.Remove(stagingPath)
	}()

	lReader := io.LimitReader(r, int64(s.MaxChunkSize)+1)

	hasher := sha256.New()
	mw := io.MultiWriter(f, hasher)

	n, err := io.Copy(mw, lReader)
	if err != nil {
		if s.metrics != nil && s.metrics.ChunkWritesTotal != nil {
			s.metrics.ChunkWritesTotal.WithLabelValues("error").Inc()
		}
		s.logger.Error("staging file create/write error", "error", err)
		return nil, fmt.Errorf("failed to read chunk body : %v", err)
	}

	if n > int64(s.MaxChunkSize) {
		if s.metrics != nil && s.metrics.ChunkWritesTotal != nil {
			s.metrics.ChunkWritesTotal.WithLabelValues("error").Inc()
		}
		return nil, ErrChunkSizeTooLarge
	}

	hashb := hasher.Sum(nil)
	computedHash := hex.EncodeToString(hashb[:])

	if computedHash != hashHex {
		if s.metrics != nil {
			if s.metrics.ChecksumMismatchesTotal != nil {
				s.metrics.ChecksumMismatchesTotal.Inc()
			}
			if s.metrics.ChunkWritesTotal != nil {
				s.metrics.ChunkWritesTotal.WithLabelValues("error").Inc()
			}
		}
		s.logger.Error("chunk checksum mismatch", "expected", hashHex, "computed", computedHash, "size", n)
		return nil, ErrHashMismatch
	}

	if err := f.Sync(); err != nil {
		if s.metrics != nil && s.metrics.ChunkWritesTotal != nil {
			s.metrics.ChunkWritesTotal.WithLabelValues("error").Inc()
		}
		s.logger.Error("staging file sync error", "error", err)
		return nil, err
	}

	finalPath := chunkPath(s.chunkPath, computedHash)

	err = os.Rename(stagingPath, finalPath)
	if err != nil {
		if s.metrics != nil && s.metrics.ChunkWritesTotal != nil {
			s.metrics.ChunkWritesTotal.WithLabelValues("error").Inc()
		}
		s.logger.Error("staging file create/write error", "error", err)
		return nil, err
	}

	if dir, err := os.Open(filepath.Dir(finalPath)); err == nil {
		if err := dir.Sync(); err != nil {
			s.logger.Error("dir sync error", "error", err)
		}
		if err := dir.Close(); err != nil {
			s.logger.Error("dir close error", "error", err)
		}
	}

	if s.metrics != nil {
		if s.metrics.ChunkWriteDuration != nil {
			s.metrics.ChunkWriteDuration.Observe(time.Since(start).Seconds())
		}
		if s.metrics.ChunkWritesTotal != nil {
			s.metrics.ChunkWritesTotal.WithLabelValues("success").Inc()
		}
	}

	return &WriteResult{
		Hash: computedHash,
		Path: finalPath,
		Size: int(n),
	}, nil
}

func (s *Storage) ReadChunk(hash string) (io.ReadCloser, error) {
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

func (s *Storage) ChunkExists(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return false
	}

	targetPath := filepath.Join(s.chunkPath, hash[:2], hash)
	info, err := os.Stat(targetPath)
	return err == nil && !info.IsDir()
}

func (s *Storage) DeleteChunk(hash string) (bool, error) {
	if len(hash) != 64 {
		return false, ErrInvalidHash
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return false, ErrInvalidHash
	}

	targetPath := filepath.Join(s.chunkPath, hash[:2], hash)

	if err := os.Remove(targetPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("remove chunk: %w", err)
	}

	if dir, err := os.Open(filepath.Dir(targetPath)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return true, nil
}

func chunkPath(path, hashHex string) string {
	if len(hashHex) < 2 {
		return ""
	}

	shard := string(hashHex[:2])
	_ = os.MkdirAll(filepath.Join(path, shard), 0755)

	return filepath.Join(path, shard, hashHex)
}

func hashGen(data []byte) string {
	b := sha256.Sum256(data)
	return hex.EncodeToString(b[:])
}

var (
	ErrChunkNotExist = errors.New("chunk does not exist")

	ErrChunkSizeTooLarge = errors.New("chunk size is too large")

	ErrHashMismatch = errors.New("expected and computed hash do not match")

	ErrInvalidHash = errors.New("invalid chunk hash")
)
