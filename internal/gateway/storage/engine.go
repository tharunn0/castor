package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	ErrNotImplemented      = errors.New("not implemented yet")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
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

type BucketInfo struct {
	Name      string
	OwnerID   string
	CreatedAt time.Time
}

type StorageEngine struct {
	cfg        config.Config
	metaClient castorv1.MetadataServiceClient
	placement  *PlacementManager
	pool       *BufferPool

	metaMu       sync.RWMutex
	metaNodes    []string
	metaClients  map[string]castorv1.MetadataServiceClient
	metaConns    map[string]*grpc.ClientConn
	activeLeader string
}

func New(cfg config.Config, metaClient castorv1.MetadataServiceClient) *StorageEngine {
	pool := NewBufferPool(cfg.ChunkSize, cfg.MaxConcurrentUploads)
	placement := NewPlacementManager(cfg.DataNodes, cfg.WriteQuorum)

	metaClients := make(map[string]castorv1.MetadataServiceClient)
	metaConns := make(map[string]*grpc.ClientConn)

	var metaNodes []string
	if len(cfg.MetadataNodes) > 0 {
		metaNodes = append(metaNodes, cfg.MetadataNodes...)
	} else if cfg.MetadataAddr != "" {
		metaNodes = append(metaNodes, cfg.MetadataAddr)
	}

	activeLeader := ""
	if len(metaNodes) > 0 {
		activeLeader = metaNodes[0]
		if metaClient != nil {
			metaClients[activeLeader] = metaClient
		}
	}

	return &StorageEngine{
		cfg:          cfg,
		metaClient:   metaClient,
		placement:    placement,
		pool:         pool,
		metaNodes:    metaNodes,
		metaClients:  metaClients,
		metaConns:    metaConns,
		activeLeader: activeLeader,
	}
}

func (e *StorageEngine) AddMetadataNode(addr string) {
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	for _, node := range e.metaNodes {
		if node == addr {
			return
		}
	}
	e.metaNodes = append(e.metaNodes, addr)
}

func (e *StorageEngine) AvailableMetadataNodes() []string {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()
	nodes := make([]string, len(e.metaNodes))
	copy(nodes, e.metaNodes)
	return nodes
}

func (e *StorageEngine) getOrCreateClient(addr string) (castorv1.MetadataServiceClient, error) {
	e.metaMu.Lock()
	defer e.metaMu.Unlock()

	if client, ok := e.metaClients[addr]; ok && client != nil {
		return client, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial metadata node %s: %w", addr, err)
	}

	client := castorv1.NewMetadataServiceClient(conn)
	e.metaConns[addr] = conn
	e.metaClients[addr] = client
	return client, nil
}

func isLeaderOrUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Unavailable {
			return true
		}
		msg := s.Message()
		return strings.Contains(msg, "not the raft leader") ||
			strings.Contains(msg, "raft leader unavailable") ||
			strings.Contains(msg, "raft not initialized")
	}
	msg := err.Error()
	return strings.Contains(msg, "not the raft leader") ||
		strings.Contains(msg, "raft leader unavailable") ||
		strings.Contains(msg, "raft not initialized")
}

func extractLeaderHint(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	const prefix = "leader is "
	idx := strings.Index(msg, prefix)
	if idx == -1 {
		return ""
	}
	hint := strings.TrimSpace(msg[idx+len(prefix):])
	if end := strings.IndexAny(hint, " \t\r\n,"); end != -1 {
		hint = hint[:end]
	}
	return hint
}

func (e *StorageEngine) resolveLeaderHint(hint string, nodes []string) string {
	if hint == "" {
		return ""
	}
	for _, n := range nodes {
		if n == hint {
			return n
		}
	}

	hintHost, hintPort, err := net.SplitHostPort(hint)
	if err != nil {
		hintHost = hint
	}

	for _, n := range nodes {
		nodeHost, _, err := net.SplitHostPort(n)
		if err == nil && nodeHost == hintHost {
			return n
		}
		if n == hintHost {
			return n
		}
	}

	if hintPort != "" {
		if raftPortNum, err := strconv.Atoi(hintPort); err == nil {
			possibleGrpcPort := strconv.Itoa(raftPortNum - 1)
			candidate := net.JoinHostPort(hintHost, possibleGrpcPort)
			for _, n := range nodes {
				if n == candidate {
					return n
				}
			}
		}
	}

	return hint
}

func (e *StorageEngine) executeWithLeader(ctx context.Context, op func(client castorv1.MetadataServiceClient) error) error {
	e.metaMu.RLock()
	currentLeader := e.activeLeader
	nodes := make([]string, len(e.metaNodes))
	copy(nodes, e.metaNodes)
	e.metaMu.RUnlock()

	var currentClient castorv1.MetadataServiceClient
	if currentLeader != "" {
		currentClient, _ = e.getOrCreateClient(currentLeader)
	}
	if currentClient == nil {
		currentClient = e.metaClient
	}

	if currentClient != nil {
		err := op(currentClient)
		if err == nil || !isLeaderOrUnavailableError(err) {
			return err
		}

		leaderHint := extractLeaderHint(err)
		if leaderHint != "" {
			resolved := e.resolveLeaderHint(leaderHint, nodes)
			if resolved != "" {
				e.AddMetadataNode(resolved)
				if hintClient, hintErr := e.getOrCreateClient(resolved); hintErr == nil {
					err = op(hintClient)
					if err == nil || !isLeaderOrUnavailableError(err) {
						e.metaMu.Lock()
						e.activeLeader = resolved
						e.metaMu.Unlock()
						return err
					}
				}
			}
		}
	}

	var lastErr error
	for _, node := range nodes {
		if node == currentLeader {
			continue
		}
		client, err := e.getOrCreateClient(node)
		if err != nil {
			lastErr = err
			continue
		}

		err = op(client)
		if err == nil || !isLeaderOrUnavailableError(err) {
			e.metaMu.Lock()
			e.activeLeader = node
			e.metaMu.Unlock()
			return err
		}
		lastErr = err
	}

	if lastErr != nil {
		return lastErr
	}
	return errors.New("no metadata leader available")
}

func (e *StorageEngine) FindLeader(ctx context.Context) (castorv1.MetadataServiceClient, string, error) {
	e.metaMu.RLock()
	leader := e.activeLeader
	e.metaMu.RUnlock()

	if leader != "" {
		client, err := e.getOrCreateClient(leader)
		if err == nil && client != nil {
			return client, leader, nil
		}
	}

	nodes := e.AvailableMetadataNodes()
	for _, node := range nodes {
		client, err := e.getOrCreateClient(node)
		if err != nil {
			continue
		}
		_, err = client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: "__probe__"})
		if err == nil || !isLeaderOrUnavailableError(err) {
			e.metaMu.Lock()
			e.activeLeader = node
			e.metaMu.Unlock()
			return client, node, nil
		}
	}

	if e.metaClient != nil {
		return e.metaClient, e.activeLeader, nil
	}
	return nil, "", errors.New("no metadata leader found")
}


func (e *StorageEngine) PutObject(ctx context.Context, bucket, key, ownerID string, reader io.Reader, size int64) (string, error) {
	if bucket == "" {
		return "", errors.New("bucket name is required")
	}
	if key == "" {
		return "", errors.New("object key is required")
	}

	var existsResp *castorv1.CheckBucketExistsResponse
	err := e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		var err error
		existsResp, err = client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: bucket})
		return err
	})
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

	err = e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		_, err := client.CommitManifest(ctx, &castorv1.CommitManifestRequest{
			Bucket:          bucket,
			Key:             key,
			Size:            totalSize,
			Etag:            etag,
			ChunkIds:        chunkIDs,
			ChunkPlacements: placements,
			OwnerId:         ownerID,
		})
		return err
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

	var existsResp *castorv1.CheckBucketExistsResponse
	err := e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		var err error
		existsResp, err = client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: bucket})
		return err
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	if !existsResp.GetExists() {
		return nil, nil, ErrBucketNotFound
	}

	var manifestResp *castorv1.GetManifestResponse
	err = e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		var err error
		manifestResp, err = client.GetManifest(ctx, &castorv1.GetManifestRequest{
			Bucket: bucket,
			Key:    key,
		})
		return err
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
	if bucket == "" {
		return errors.New("bucket name is required")
	}
	if key == "" {
		return errors.New("object key is required")
	}

	exists, err := e.CheckBucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if !exists {
		return ErrBucketNotFound
	}

	err = e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		_, err := client.DeleteManifest(ctx, &castorv1.DeleteManifestRequest{
			Bucket: bucket,
			Key:    key,
		})
		return err
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return ErrObjectNotFound
		}
		return err
	}

	return nil
}

func (e *StorageEngine) Close() error {
	e.metaMu.Lock()
	defer e.metaMu.Unlock()

	var firstErr error
	for _, conn := range e.metaConns {
		if conn != nil {
			if err := conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := e.placement.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (e *StorageEngine) CreateBucket(ctx context.Context, bucket, ownerID string, tags map[string]string) error {
	if bucket == "" {
		return errors.New("bucket name is required")
	}
	if ownerID == "" {
		ownerID = "admin"
	}

	err := e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		_, err := client.CreateBucket(ctx, &castorv1.CreateBucketMetadataRequest{
			Bucket:  bucket,
			OwnerId: ownerID,
			Tags:    tags,
		})
		return err
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			return ErrBucketAlreadyExists
		}
		return err
	}

	return nil
}

func (e *StorageEngine) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	var resp *castorv1.ListBucketsMetadataResponse
	err := e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		var err error
		resp, err = client.ListBuckets(ctx, &castorv1.ListBucketsMetadataRequest{})
		return err
	})
	if err != nil {
		return nil, err
	}

	buckets := make([]BucketInfo, 0, len(resp.GetBuckets()))
	for _, b := range resp.GetBuckets() {
		var createdAt time.Time
		if b.GetCreatedAt() != nil {
			createdAt = b.GetCreatedAt().AsTime()
		}
		buckets = append(buckets, BucketInfo{
			Name:      b.GetName(),
			OwnerID:   b.GetOwnerId(),
			CreatedAt: createdAt,
		})
	}

	return buckets, nil
}

func (e *StorageEngine) DeleteBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		return errors.New("bucket name is required")
	}

	err := e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		_, err := client.DeleteBucket(ctx, &castorv1.DeleteBucketMetadataRequest{
			Bucket: bucket,
		})
		return err
	})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.NotFound:
				return ErrBucketNotFound
			case codes.FailedPrecondition:
				return ErrBucketNotEmpty
			}
		}
		return err
	}

	return nil
}

func (e *StorageEngine) CheckBucketExists(ctx context.Context, bucket string) (bool, error) {
	if bucket == "" {
		return false, errors.New("bucket name is required")
	}
	var resp *castorv1.CheckBucketExistsResponse
	err := e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		var err error
		resp, err = client.CheckBucketExists(ctx, &castorv1.CheckBucketExistsRequest{Bucket: bucket})
		return err
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return false, nil
		}
		return false, err
	}
	return resp.GetExists(), nil
}

func (e *StorageEngine) GetManifest(ctx context.Context, bucket, key string) (*castorv1.GetManifestResponse, error) {
	if bucket == "" {
		return nil, errors.New("bucket name is required")
	}
	if key == "" {
		return nil, errors.New("object key is required")
	}
	var resp *castorv1.GetManifestResponse
	err := e.executeWithLeader(ctx, func(client castorv1.MetadataServiceClient) error {
		var err error
		resp, err = client.GetManifest(ctx, &castorv1.GetManifestRequest{
			Bucket: bucket,
			Key:    key,
		})
		return err
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return resp, nil
}



