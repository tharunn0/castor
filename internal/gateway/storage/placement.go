package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrQuorumNotMet = errors.New("storage: write quorum not satisfied")
	ErrNoNodes      = errors.New("storage: no storage nodes available")
)

// PlacementManager manages and interacts with data nodes
type PlacementManager struct {
	mu          sync.RWMutex
	nodeAddrs   []string
	writeQuorum int
	clients     map[string]castorv1.DataServiceClient
	conns       map[string]*grpc.ClientConn
	rr          atomic.Uint64
}

func NewPlacementManager(nodes []string, writeQuorum int) *PlacementManager {
	return &PlacementManager{
		nodeAddrs:   append([]string(nil), nodes...),
		writeQuorum: writeQuorum,
		clients:     make(map[string]castorv1.DataServiceClient),
		conns:       make(map[string]*grpc.ClientConn),
	}
}

func (m *PlacementManager) SelectNodes(count int) ([]string, error) {
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

	conn, err := grpc.NewClient(nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to node %s: %w", nodeAddr, err)
	}

	client = castorv1.NewDataServiceClient(conn)
	m.conns[nodeAddr] = conn
	m.clients[nodeAddr] = client
	return client, nil
}
