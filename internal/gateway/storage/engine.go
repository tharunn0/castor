package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotImplemented = errors.New("not implemented yet")
	ErrBucketNotFound = errors.New("bucket not found")
)

type StorageEngine struct {
	cfg        config.Config
	metaClient castorv1.MetadataServiceClient
	placement  *PlacementManager
	pool       *BufferPool
}

func New(cfg config.Config, metaClient castorv1.MetadataServiceClient) *StorageEngine {
	pool := NewBufferPool(cfg.ChunkSize, cfg.MaxConcurrentUploads)
	placement := NewPlacementManager(cfg.DataNodes, cfg.WriteQuorum)

	return &StorageEngine{
		cfg:        cfg,
		metaClient: metaClient,
		placement:  placement,
		pool:       pool,
	}
}

func (e *StorageEngine) PutObject(ctx context.Context, bucket, key, ownerID string, reader io.Reader, size int64) (string, error) {
	if bucket == "" {
		return "", errors.New("bucket name is required")
	}
	if key == "" {
		return "", errors.New("object key is required")
	}

	existsResp, err := e.metaClient.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: bucket})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return "", ErrBucketNotFound
		}
		return "", err
	}
	if !existsResp.GetExists() {
		return "", ErrBucketNotFound
	}

	var chunkIDs []string
	var placements []*castorv1.ChunkPlacement
	var totalSize int64
	objHasher := sha256.New()

	if reader != nil {
		bufPtr := e.pool.Acquire()
		defer e.pool.Release(bufPtr)
		buf := *bufPtr

		for {
			n, readErr := io.ReadFull(reader, buf)
			if n > 0 {
				chunkBytes := make([]byte, n)
				copy(chunkBytes, buf[:n])

				h := sha256.Sum256(chunkBytes)
				chunkHash := hex.EncodeToString(h[:])
				objHasher.Write(chunkBytes)

				targets, err := e.placement.SelectNodes(len(e.cfg.DataNodes))
				if err != nil {
					return "", err
				}

				successfulNodes, err := e.placement.WriteChunkQuorum(ctx, chunkHash, chunkBytes, targets)
				if err != nil {
					return "", err
				}

				chunkIDs = append(chunkIDs, chunkHash)
				placements = append(placements, &castorv1.ChunkPlacement{
					ChunkHash:     chunkHash,
					NodeAddresses: successfulNodes,
					Size:          int64(n),
				})
				totalSize += int64(n)
			}

			if readErr != nil {
				if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
					break
				}
				return "", readErr
			}
		}
	}

	etag := hex.EncodeToString(objHasher.Sum(nil))

	_, err = e.metaClient.CommitManifest(ctx, &castorv1.CommitManifestRequest{
		Bucket:          bucket,
		Key:             key,
		Size:            totalSize,
		Etag:            etag,
		ChunkIds:        chunkIDs,
		ChunkPlacements: placements,
		OwnerId:         ownerID,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return "", ErrBucketNotFound
		}
		return "", err
	}

	return etag, nil
}

func (e *StorageEngine) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	return nil, 0, ErrNotImplemented
}

func (e *StorageEngine) DeleteObject(ctx context.Context, bucket, key string) error {
	return ErrNotImplemented
}

func (e *StorageEngine) Close() error {
	return e.placement.Close()
}
