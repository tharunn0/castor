package registry

import (
	"errors"
	"sync"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrNodeIDRequired      = errors.New("node_id is required")
	ErrGRPCAddressRequired = errors.New("grpc_address is required")
)

type nodeEntry struct {
	nodeID      string
	grpcAddress string
	totalBytes  int64
	freeBytes   int64
	lastSeen    time.Time
}

// Registry manages in-memory telemetry for active data-svc storage nodes.
// Heartbeats intentionally bypass Raft consensus to avoid disk WAL contention.
type Registry struct {
	mu             sync.RWMutex
	nodes          map[string]*nodeEntry
	offlineTimeout time.Duration
	nowFunc        func() time.Time
}

type Option func(*Registry)

func WithOfflineTimeout(d time.Duration) Option {
	return func(r *Registry) {
		r.offlineTimeout = d
	}
}

func WithNowFunc(fn func() time.Time) Option {
	return func(r *Registry) {
		r.nowFunc = fn
	}
}

func New(opts ...Option) *Registry {
	r := &Registry{
		nodes:          make(map[string]*nodeEntry),
		offlineTimeout: 9 * time.Second,
		nowFunc:        time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Registry) Register(nodeID, grpcAddress string, totalBytes, freeBytes int64) error {
	if nodeID == "" {
		return ErrNodeIDRequired
	}
	if grpcAddress == "" {
		return ErrGRPCAddressRequired
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes[nodeID] = &nodeEntry{
		nodeID:      nodeID,
		grpcAddress: grpcAddress,
		totalBytes:  totalBytes,
		freeBytes:   freeBytes,
		lastSeen:    r.nowFunc(),
	}
	return nil
}

func (r *Registry) Heartbeat(nodeID, grpcAddress string, totalBytes, freeBytes int64, t time.Time) error {
	if nodeID == "" {
		return ErrNodeIDRequired
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, exists := r.nodes[nodeID]
	if !exists {
		if grpcAddress == "" {
			return ErrGRPCAddressRequired
		}
		entry = &nodeEntry{
			nodeID:      nodeID,
			grpcAddress: grpcAddress,
		}
		r.nodes[nodeID] = entry
	}

	if grpcAddress != "" {
		entry.grpcAddress = grpcAddress
	}
	entry.totalBytes = totalBytes
	entry.freeBytes = freeBytes

	if t.IsZero() {
		entry.lastSeen = r.nowFunc()
	} else {
		entry.lastSeen = t
	}

	return nil
}

func (r *Registry) toNodeCapacity(entry *nodeEntry, now time.Time) *castorv1.NodeCapacity {
	var freeRatio float64
	if entry.totalBytes > 0 {
		freeRatio = float64(entry.freeBytes) / float64(entry.totalBytes)
	}

	status := castorv1.NodeStatus_NODE_STATUS_HEALTHY
	if now.Sub(entry.lastSeen) > r.offlineTimeout {
		status = castorv1.NodeStatus_NODE_STATUS_OFFLINE
	} else if freeRatio < 0.05 {
		status = castorv1.NodeStatus_NODE_STATUS_DEGRADED
	}

	return &castorv1.NodeCapacity{
		NodeId:      entry.nodeID,
		GrpcAddress: entry.grpcAddress,
		TotalBytes:  entry.totalBytes,
		FreeBytes:   entry.freeBytes,
		FreeRatio:   freeRatio,
		Status:      status,
		LastSeen:    timestamppb.New(entry.lastSeen),
	}
}

// GetActiveNodes returns all healthy and degraded nodes within the heartbeat window.
func (r *Registry) GetActiveNodes() []*castorv1.NodeCapacity {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := r.nowFunc()
	active := make([]*castorv1.NodeCapacity, 0, len(r.nodes))

	for _, entry := range r.nodes {
		capacity := r.toNodeCapacity(entry, now)
		if capacity.Status != castorv1.NodeStatus_NODE_STATUS_OFFLINE {
			active = append(active, capacity)
		}
	}

	return active
}

// GetAllNodes returns all registered nodes including offline ones.
func (r *Registry) GetAllNodes() []*castorv1.NodeCapacity {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := r.nowFunc()
	nodes := make([]*castorv1.NodeCapacity, 0, len(r.nodes))

	for _, entry := range r.nodes {
		nodes = append(nodes, r.toNodeCapacity(entry, now))
	}

	return nodes
}

func (r *Registry) GetNode(nodeID string) (*castorv1.NodeCapacity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.nodes[nodeID]
	if !ok {
		return nil, false
	}

	return r.toNodeCapacity(entry, r.nowFunc()), true
}
