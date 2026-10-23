package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrQuorumNotMet = errors.New("storage: write quorum not satisfied")
	ErrNoNodes      = errors.New("storage: no storage nodes available")
)

type ActiveNodeProvider func(ctx context.Context) ([]*castorv1.NodeCapacity, error)

type PlacementOption func(*PlacementManager)

func WithActiveNodeProvider(provider ActiveNodeProvider) PlacementOption {
	return func(m *PlacementManager) {
		m.activeNodeProvider = provider
	}
}

func WithActiveNodeCacheTTL(ttl time.Duration) PlacementOption {
	return func(m *PlacementManager) {
		if ttl > 0 {
			m.cacheTTL = ttl
		}
	}
}

// PlacementManager manages and interacts with data nodes
type PlacementManager struct {
	mu                 sync.RWMutex
	nodeAddrs          []string
	writeQuorum        int
	clients            map[string]castorv1.DataServiceClient
	conns              map[string]*grpc.ClientConn
	rr                 atomic.Uint64
	activeNodeProvider ActiveNodeProvider
	cachedNodes        []*castorv1.NodeCapacity
	cacheExpiry        time.Time
	cacheTTL           time.Duration
}

func NewPlacementManager(nodes []string, writeQuorum int, opts ...PlacementOption) *PlacementManager {
	m := &PlacementManager{
		nodeAddrs:   append([]string(nil), nodes...),
		writeQuorum: writeQuorum,
		clients:     make(map[string]castorv1.DataServiceClient),
		conns:       make(map[string]*grpc.ClientConn),
		cacheTTL:    3 * time.Second,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *PlacementManager) SetActiveNodeProvider(provider ActiveNodeProvider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeNodeProvider = provider
	m.cachedNodes = nil
	m.cacheExpiry = time.Time{}
}

func (m *PlacementManager) getActiveNodes(ctx context.Context) []*castorv1.NodeCapacity {
	m.mu.RLock()
	provider := m.activeNodeProvider
	cached := m.cachedNodes
	expiry := m.cacheExpiry
	m.mu.RUnlock()

	if provider == nil {
		return nil
	}

	if time.Now().Before(expiry) && cached != nil {
		return cached
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if time.Now().Before(m.cacheExpiry) && m.cachedNodes != nil {
		return m.cachedNodes
	}

	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	nodes, err := provider(callCtx)
	if err != nil {
		if m.cachedNodes != nil {
			return m.cachedNodes
		}
		return nil
	}

	m.cachedNodes = nodes
	m.cacheExpiry = time.Now().Add(m.cacheTTL)
	return nodes
}

func (m *PlacementManager) SelectNodesWithContext(ctx context.Context, count int) ([]string, error) {
	activeNodes := m.getActiveNodes(ctx)

	var candidates []*castorv1.NodeCapacity
	for _, node := range activeNodes {
		if node.GetGrpcAddress() == "" {
			continue
		}
		if node.GetStatus() == castorv1.NodeStatus_NODE_STATUS_OFFLINE {
			continue
		}
		// Minimum free space must allow at least one 4MiB chunk.
		if node.GetFreeBytes() < (4 << 20) {
			continue
		}
		candidates = append(candidates, node)
	}

	if len(candidates) >= m.writeQuorum {
		var healthy []*castorv1.NodeCapacity
		var degraded []*castorv1.NodeCapacity
		for _, node := range candidates {
			if node.GetStatus() == castorv1.NodeStatus_NODE_STATUS_HEALTHY {
				healthy = append(healthy, node)
			} else {
				degraded = append(degraded, node)
			}
		}

		sort.SliceStable(healthy, func(i, j int) bool {
			return healthy[i].GetFreeBytes() > healthy[j].GetFreeBytes()
		})
		sort.SliceStable(degraded, func(i, j int) bool {
			return degraded[i].GetFreeBytes() > degraded[j].GetFreeBytes()
		})

		sorted := append(healthy, degraded...)

		target := count
		if target <= 0 || target > len(sorted) {
			target = len(sorted)
		}
		if target < m.writeQuorum {
			return nil, ErrQuorumNotMet
		}

		selected := make([]string, target)
		for i := range target {
			selected[i] = sorted[i].GetGrpcAddress()
		}
		return selected, nil
	}

	return m.selectStaticNodes(count)
}

func (m *PlacementManager) SelectNodes(count int) ([]string, error) {
	return m.SelectNodesWithContext(context.Background(), count)
}

func (m *PlacementManager) selectStaticNodes(count int) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	n := len(m.nodeAddrs)
	if n == 0 {
		return nil, ErrNoNodes
	}

	target := count
	if target <= 0 || target > n {
		target = n
	}
	if target < m.writeQuorum {
		return nil, ErrQuorumNotMet
	}

	start := int((m.rr.Add(1) - 1) % uint64(n))
	selected := make([]string, target)
	for i := range target {
		selected[i] = m.nodeAddrs[(start+i)%n]
	}
	return selected, nil
}

func (m *PlacementManager) WriteChunkQuorum(ctx context.Context, chunkHash string, data []byte, targets []string) ([]string, error) {
	if len(targets) < m.writeQuorum {
		return nil, ErrQuorumNotMet
	}

	type writeResult struct {
		node string
		err  error
	}

	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resCh := make(chan writeResult, len(targets))

	for _, target := range targets {
		go func(addr string) {
			err := m.StreamChunkToNode(writeCtx, addr, chunkHash, data)
			resCh <- writeResult{node: addr, err: err}
		}(target)
	}

	var successfulNodes []string
	failedCount := 0
	maxFailures := len(targets) - m.writeQuorum

	for range len(targets) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-resCh:
			if res.err == nil {
				successfulNodes = append(successfulNodes, res.node)
				if len(successfulNodes) >= m.writeQuorum {
					return successfulNodes, nil
				}
			} else {
				failedCount++
				if failedCount > maxFailures {
					return nil, fmt.Errorf("%w: %v", ErrQuorumNotMet, res.err)
				}
			}
		}
	}

	if len(successfulNodes) < m.writeQuorum {
		return nil, ErrQuorumNotMet
	}
	return successfulNodes, nil
}

func (m *PlacementManager) StreamChunkToNode(ctx context.Context, nodeAddr string, chunkHash string, data []byte) error {
	client, err := m.getClient(nodeAddr)
	if err != nil {
		return err
	}

	stream, err := client.PutChunk(ctx)
	if err != nil {
		return err
	}

	err = stream.Send(&castorv1.PutChunkRequest{
		Data: &castorv1.PutChunkRequest_Metadata{
			Metadata: &castorv1.PutChunkMetadata{
				ChunkHash: chunkHash,
				ChunkSize: int64(len(data)),
			},
		},
	})
	if err != nil {
		return err
	}

	const frameSize = 64 * 1024
	for offset := 0; offset < len(data); offset += frameSize {
		end := min(offset+frameSize, len(data))
		err = stream.Send(&castorv1.PutChunkRequest{
			Data: &castorv1.PutChunkRequest_ChunkBytes{
				ChunkBytes: data[offset:end],
			},
		})
		if err != nil {
			return err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp.GetChunkHash() != chunkHash {
		return fmt.Errorf("chunk hash mismatch: expected %s, got %s", chunkHash, resp.GetChunkHash())
	}
	return nil
}

func (m *PlacementManager) ReadChunkFromNode(ctx context.Context, nodeAddr string, chunkHash string, length int64) ([]byte, error) {
	client, err := m.getClient(nodeAddr)
	if err != nil {
		return nil, err
	}

	stream, err := client.GetChunk(ctx, &castorv1.GetChunkRequest{
		ChunkHash: chunkHash,
		Length:    length,
	})
	if err != nil {
		return nil, err
	}

	var buf []byte
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		buf = append(buf, resp.GetChunkBytes()...)
	}

	sum := sha256.Sum256(buf)
	if hex.EncodeToString(sum[:]) != chunkHash {
		return nil, fmt.Errorf("chunk checksum mismatch: expected %s, got %s", chunkHash, hex.EncodeToString(sum[:]))
	}
	if int(length) <= len(buf) {
		return buf[:length], nil
	}
	return buf, nil
}

func (m *PlacementManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for addr, conn := range m.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.conns, addr)
		delete(m.clients, addr)
	}
	return firstErr
}

func (m *PlacementManager) getClient(nodeAddr string) (castorv1.DataServiceClient, error) {
	m.mu.RLock()
	client, ok := m.clients[nodeAddr]
	m.mu.RUnlock()
	if ok {
		return client, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if client, ok := m.clients[nodeAddr]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(
		nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		telemetry.ClientDialOption(),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to node %s: %w", nodeAddr, err)
	}

	client = castorv1.NewDataServiceClient(conn)
	m.conns[nodeAddr] = conn
	m.clients[nodeAddr] = client
	return client, nil
}
