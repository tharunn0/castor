package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotImplemented      = errors.New("not implemented yet")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrObjectNotFound      = errors.New("object not found")
)

type ObjectInfo struct {
	Bucket      string
	Key         string
	Size        int64
	ETag        string
	ContentType string
	UpdatedAt   time.Time
}

// StorageEngine manages bufferpool, placement manager and maintains connection with metadata service
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

func (e *StorageEngine) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error) {
	return e.GetObjectRange(ctx, bucket, key, 0, -1)
}

func (e *StorageEngine) GetObjectRange(ctx context.Context, bucket, key string, startOffset, length int64) (io.ReadCloser, *ObjectInfo, error) {
	if bucket == "" {
		return nil, nil, errors.New("bucket name is required")
	}
	if key == "" {
		return nil, nil, errors.New("object key is required")
	}

	existsResp, err := e.metaClient.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: bucket})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	if !existsResp.GetExists() {
		return nil, nil, ErrBucketNotFound
	}

	manifestResp, err := e.metaClient.GetManifest(ctx, &castorv1.GetManifestRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}
	if manifestResp.GetStatus() == "deleted" {
		return nil, nil, ErrObjectNotFound
	}

	objInfo := &ObjectInfo{
		Bucket:      manifestResp.GetBucket(),
		Key:         manifestResp.GetKey(),
		Size:        manifestResp.GetSize(),
		ETag:        manifestResp.GetEtag(),
		ContentType: manifestResp.GetContentType(),
	}
	if manifestResp.GetUpdatedAt() != nil {
		objInfo.UpdatedAt = manifestResp.GetUpdatedAt().AsTime()
	} else if manifestResp.GetCreatedAt() != nil {
		objInfo.UpdatedAt = manifestResp.GetCreatedAt().AsTime()
	}

	objSize := manifestResp.GetSize()
	if objSize == 0 {
		return io.NopCloser(bytes.NewReader(nil)), objInfo, nil
	}

	if startOffset < 0 {
		startOffset = 0
	}
	if length < 0 || startOffset+length > objSize {
		length = objSize - startOffset
	}
	if startOffset >= objSize || length <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), objInfo, nil
	}

	reqEnd := startOffset + length
	var curOffset int64
	var slices []chunkSlice

	for _, chunk := range manifestResp.GetChunks() {
		chunkStart := curOffset
		chunkEnd := curOffset + chunk.GetSize()
		curOffset = chunkEnd

		if chunkStart < reqEnd && chunkEnd > startOffset {
			sliceStart := max(int64(0), startOffset-chunkStart)
			sliceEnd := min(chunk.GetSize(), reqEnd-chunkStart)
			slices = append(slices, chunkSlice{
				hash:          chunk.GetChunkHash(),
				nodeAddresses: chunk.GetNodeAddresses(),
				size:          chunk.GetSize(),
				sliceStart:    sliceStart,
				sliceEnd:      sliceEnd,
			})
		}
	}

	readerCtx, cancel := context.WithCancel(ctx)
	reader := &chunkStreamReader{
		ctx:          readerCtx,
		cancel:       cancel,
		engine:       e,
		chunks:       slices,
		currentChunk: 0,
	}

	return reader, objInfo, nil
}

type chunkSlice struct {
	hash          string
	nodeAddresses []string
	size          int64
	sliceStart    int64
	sliceEnd      int64
}

type chunkStreamReader struct {
	ctx          context.Context
	cancel       context.CancelFunc
	engine       *StorageEngine
	chunks       []chunkSlice
	currentChunk int
	chunkBuf     []byte
	chunkBufPos  int
}

func (r *chunkStreamReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	for {
		if r.chunkBufPos < len(r.chunkBuf) {
			n := copy(p, r.chunkBuf[r.chunkBufPos:])
			r.chunkBufPos += n
			return n, nil
		}

		if r.currentChunk >= len(r.chunks) {
			return 0, io.EOF
		}

		target := r.chunks[r.currentChunk]
		data, err := r.readChunkWithFailover(target.hash, target.size, target.nodeAddresses)
		if err != nil {
			return 0, err
		}

		sliceStart := target.sliceStart
		sliceEnd := target.sliceEnd
		if sliceEnd > int64(len(data)) {
			sliceEnd = int64(len(data))
		}
		if sliceStart > sliceEnd {
			sliceStart = sliceEnd
		}

		r.chunkBuf = data[sliceStart:sliceEnd]
		r.chunkBufPos = 0
		r.currentChunk++
	}
}

func (r *chunkStreamReader) readChunkWithFailover(chunkHash string, size int64, nodes []string) ([]byte, error) {
	seen := make(map[string]struct{}, len(nodes))
	var candidateNodes []string
	for _, addr := range nodes {
		candidateNodes = append(candidateNodes, addr)
		seen[addr] = struct{}{}
	}
	for _, addr := range r.engine.cfg.DataNodes {
		if _, ok := seen[addr]; !ok {
			candidateNodes = append(candidateNodes, addr)
		}
	}

	var lastErr error
	for _, nodeAddr := range candidateNodes {
		data, err := r.engine.placement.ReadChunkFromNode(r.ctx, nodeAddr, chunkHash, size)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return nil, fmt.Errorf("chunk %s read failed across all replicas: %w", chunkHash, lastErr)
	}
	return nil, fmt.Errorf("no replica nodes available for chunk %s", chunkHash)
}

func (r *chunkStreamReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.chunkBuf = nil
	return nil
}

func (e *StorageEngine) DeleteObject(ctx context.Context, bucket, key string) error {
	return ErrNotImplemented
}

func (e *StorageEngine) Close() error {
	return e.placement.Close()
}

func (e *StorageEngine) CreateBucket(ctx context.Context, bucket, ownerID string, tags map[string]string) error {
	if bucket == "" {
		return errors.New("bucket name is required")
	}
	if ownerID == "" {
		ownerID = "admin"
	}

	_, err := e.metaClient.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
		Bucket:  bucket,
		OwnerId: ownerID,
		Tags:    tags,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			return ErrBucketAlreadyExists
		}
		return err
	}

	return nil
}
