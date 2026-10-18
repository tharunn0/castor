package consensus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/telemetry"
	telemetryPkg "github.com/tharunn0/castor/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrNotLeader          = errors.New("not the raft leader")
	ErrLeaderUnavailable  = errors.New("raft leader unavailable")
	ErrRaftNotInitialized = errors.New("raft not initialized")
)

type NotLeaderError struct {
	LeaderAddr string
	LeaderID   string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr != "" {
		return fmt.Sprintf("not the raft leader: leader is %s", e.LeaderAddr)
	}
	return "not the raft leader"
}

func (e *NotLeaderError) Unwrap() error {
	return ErrNotLeader
}

type RaftNode struct {
	raft          *raft.Raft
	fsm           *FSM
	transport     raft.Transport
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	stopCh        chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup

	cfg     config.Config
	logger  *slog.Logger
	tracer  trace.Tracer
	metrics *telemetry.MetadataMetrics
}

type RaftOption func(*RaftNode)

func WithTracer(tracer trace.Tracer) RaftOption {
	return func(r *RaftNode) {
		r.tracer = tracer
	}
}

func WithMetrics(metrics *telemetry.MetadataMetrics) RaftOption {
	return func(r *RaftNode) {
		r.metrics = metrics
	}
}

func WithLogger(logger *slog.Logger) RaftOption {
	return func(r *RaftNode) {
		r.logger = logger
	}
}

func NewRaftNode(cfg config.Config, fsm *FSM, opts ...RaftOption) (*RaftNode, error) {

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, err
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(raftDir, "snapshots")
	snapshotStore, err := raft.NewFileSnapshotStore(snapshotDir, 3, io.Discard)
	if err != nil {
		_ = boltStore.Close()
		return nil, err
	}

	raftAddr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("failed to resolve addr : %s, error: %w", cfg.RaftAddr, err)
	}
	transport, err := raft.NewTCPTransport(raftAddr.String(), nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("%w: addr : %s", err, raftAddr.String())
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 100 * time.Millisecond
	raftConfig.ElectionTimeout = 200 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 100 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond
	raftConfig.LogOutput = cfg.RaftLogOutput

	hasExistingState, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
	if err != nil {
		_ = boltStore.Close()
		_ = transport.Close()
		return nil, err
	}

	if cfg.RaftBootstrap && !hasExistingState {
		var servers []raft.Server
		if len(cfg.Peers) > 0 {
			hasLocal := false
			for peerID, peerAddr := range cfg.Peers {
				if peerID == cfg.NodeID {
					hasLocal = true
				}
				servers = append(servers, raft.Server{
					ID:      raft.ServerID(peerID),
					Address: raft.ServerAddress(peerAddr),
				})
			}
			if !hasLocal {
				servers = append(servers, raft.Server{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				})
			}
		} else {
			servers = append(servers, raft.Server{
				ID:      raftConfig.LocalID,
				Address: transport.LocalAddr(),
			})
		}

		configuration := raft.Configuration{Servers: servers}
		if err := raft.BootstrapCluster(raftConfig, boltStore, boltStore, snapshotStore, transport, configuration); err != nil {
			_ = boltStore.Close()
			_ = transport.Close()
			return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
	}

	raftInstance, err := raft.NewRaft(raftConfig, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		_ = boltStore.Close()
		_ = transport.Close()
		return nil, err
	}

	defaultMetrics := telemetry.NewMetadataMetrics(cfg.NodeID, &telemetryPkg.Metrics{
		Registry: prometheus.NewRegistry(),
	})

	node := &RaftNode{
		raft:          raftInstance,
		transport:     transport,
		logStore:      boltStore,
		stableStore:   boltStore,
		snapshotStore: snapshotStore,
		fsm:           fsm,
		cfg:           cfg,
		logger:        slog.New(slog.DiscardHandler),
		tracer:        otel.GetTracerProvider().Tracer("metadata-svc"),
		metrics:       defaultMetrics,
		stopCh:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(node)
	}

	node.metrics.IsLeader.Set(0)

	node.wg.Add(1)
	go node.monitorLeadership()

	return node, nil
}

func (r *RaftNode) monitorLeadership() {
	defer r.wg.Done()
	leaderCh := r.raft.LeaderCh()
	for {
		select {
		case <-r.stopCh:
			return
		case isLeader, ok := <-leaderCh:
			if !ok {
				return
			}
			if isLeader {
				r.metrics.IsLeader.Set(1)
				r.metrics.LeaderElections.Inc()
				r.logger.Info("raft leadership change",
					"event", "leadership_acquired",
					"node_id", r.cfg.NodeID,
					"term", r.raft.CurrentTerm(),
					"leader_addr", r.LeaderAddr(),
				)
			} else {
				r.metrics.IsLeader.Set(0)
				r.logger.Info("raft leadership change",
					"event", "leadership_lost",
					"node_id", r.cfg.NodeID,
					"new_leader_addr", r.LeaderAddr(),
				)
			}
		}
	}
}

func (r *RaftNode) Apply(cmd *Command, timeout time.Duration) (any, error) {
	return r.ApplyContext(context.Background(), cmd, timeout)
}

func (r *RaftNode) ApplyContext(ctx context.Context, cmd *Command, timeout time.Duration) (any, error) {
	if r.raft == nil {
		return nil, ErrRaftNotInitialized
	}

	ctx, span := r.tracer.Start(ctx, "Raft/Apply",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("raft.command_type", cmd.Type.String()),
			attribute.String("raft.node_id", r.cfg.NodeID),
		),
	)
	defer span.End()

	r.logger.Debug("[Raft Apply]", "cmd Type :", cmd.Type)

	data, err := cmd.Encode()
	if err != nil {
		r.logger.Error("[Raft Apply] Encode Error", "error", err)
		span.RecordError(err)
		return nil, err
	}

	deadline := time.Now().Add(timeout)
	for {
		if time.Until(deadline) <= 0 {
			leader := r.LeaderAddr()
			if leader == "" {
				r.logger.Error("raft apply timeout; leader unavailable", "node_id", r.cfg.NodeID)
				span.RecordError(ErrLeaderUnavailable)
				return nil, ErrLeaderUnavailable
			}
			notLeaderErr := &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
			span.RecordError(notLeaderErr)
			return nil, notLeaderErr
		}

		if !r.IsLeader() {
			leader := r.LeaderAddr()
			if leader != "" {
				notLeaderErr := &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
				span.RecordError(notLeaderErr)
				return nil, notLeaderErr
			}
			select {
			case <-ctx.Done():
				span.RecordError(ctx.Err())
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}

		future := r.raft.Apply(data, timeout)
		err := future.Error()
		if err == nil {
			r.logger.Debug("[Raft Apply] Success", "response", future.Response())
			if res, ok := future.Response().(error); ok && res != nil {
				r.logger.Error("[Raft Apply] Error", "error", res)
				span.RecordError(res)
				return res, nil
			}
			return future.Response(), nil
		}

		if errors.Is(err, raft.ErrNotLeader) {
			leader := r.LeaderAddr()
			if leader != "" {
				notLeaderErr := &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
				span.RecordError(notLeaderErr)
				return nil, notLeaderErr
			}
			select {
			case <-ctx.Done():
				span.RecordError(ctx.Err())
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}

		r.logger.Error("failed to apply raft log", "error", err, "node_id", r.cfg.NodeID, "cmd_type", cmd.Type.String())
		span.RecordError(err)
		return nil, err
	}
}

func (r *RaftNode) IsLeader() bool {
	return r.raft != nil && r.raft.State() == raft.Leader
}

func (r *RaftNode) LeaderAddr() string {
	if r.raft == nil {
		return ""
	}
	return string(r.raft.Leader())
}

func (r *RaftNode) LeaderID() string {
	if r.raft == nil {
		return ""
	}
	leaderAddr := r.raft.Leader()
	if leaderAddr == "" {
		return ""
	}

	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return ""
	}

	for _, s := range future.Configuration().Servers {
		if s.Address == leaderAddr {
			return string(s.ID)
		}
	}
	return ""
}

func (r *RaftNode) Join(nodeID string, addr string) error {
	if r.raft == nil {
		return ErrRaftNotInitialized
	}
	if !r.IsLeader() {
		leader := r.LeaderAddr()
		if leader == "" {
			return ErrLeaderUnavailable
		}
		return &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
	}
	future := r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			leader := r.LeaderAddr()
			if leader == "" {
				return ErrLeaderUnavailable
			}
			return &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
		}
		return err
	}
	return nil
}

func (r *RaftNode) Leave(nodeID string) error {
	if r.raft == nil {
		return ErrRaftNotInitialized
	}
	if !r.IsLeader() {
		leader := r.LeaderAddr()
		if leader == "" {
			return ErrLeaderUnavailable
		}
		return &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
	}
	future := r.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			leader := r.LeaderAddr()
			if leader == "" {
				return ErrLeaderUnavailable
			}
			return &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
		}
		return err
	}
	return nil
}

func (r *RaftNode) Shutdown() error {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	r.wg.Wait()

	r.metrics.IsLeader.Set(0)

	var errs []error
	if r.raft != nil {
		future := r.raft.Shutdown()
		if err := future.Error(); err != nil {
			errs = append(errs, err)
		}
	}
	if closer, ok := r.logStore.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (r *RaftNode) Raft() *raft.Raft {
	return r.raft
}
