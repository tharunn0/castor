package consensus

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/tharunn0/castor/internal/metadata/config"
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
	cfg           config.Config
	logger        *slog.Logger
}

func NewRaftNode(cfg config.Config, logger *slog.Logger, fsm *FSM) (*RaftNode, error) {
	if logger == nil {
		logger = slog.Default()
	}
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

	return &RaftNode{
		raft:          raftInstance,
		transport:     transport,
		logStore:      boltStore,
		stableStore:   boltStore,
		snapshotStore: snapshotStore,
		fsm:           fsm,
		cfg:           cfg,
		logger:        logger,
	}, nil
}

func (r *RaftNode) Apply(cmd *Command, timeout time.Duration) (any, error) {
	if r.raft == nil {
		return nil, ErrRaftNotInitialized
	}

	r.logger.Debug("[Raft Apply]", "cmd Type :", cmd.Type)

	data, err := cmd.Encode()
	if err != nil {
		r.logger.Error("[Raft Apply] Encode Error", "error", err)
		return nil, err
	}

	deadline := time.Now().Add(timeout)
	for {
		if time.Until(deadline) <= 0 {
			leader := r.LeaderAddr()
			if leader == "" {
				return nil, ErrLeaderUnavailable
			}
			return nil, &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
		}

		if !r.IsLeader() {
			leader := r.LeaderAddr()
			if leader != "" {
				return nil, &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}

		future := r.raft.Apply(data, timeout)
		err := future.Error()
		if err == nil {
			r.logger.Debug("[Raft Apply] Success", "response", future.Response())
			if res, ok := future.Response().(error); ok && res != nil {
				r.logger.Error("[Raft Apply] Error", "error", res)
				return res, nil
			}
			return future.Response(), nil
		}

		if errors.Is(err, raft.ErrNotLeader) {
			leader := r.LeaderAddr()
			if leader != "" {
				return nil, &NotLeaderError{LeaderAddr: leader, LeaderID: r.LeaderID()}
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}

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

type RaftPeerStatus struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

type RaftStatus struct {
	NodeID            string           `json:"node_id"`
	Role              string           `json:"role"`
	Term              uint64           `json:"term"`
	LeaderID          string           `json:"leader_id"`
	LeaderAddr        string           `json:"leader_addr"`
	CommitIndex       uint64           `json:"commit_index"`
	AppliedIndex      uint64           `json:"applied_index"`
	LastLogIndex      uint64           `json:"last_log_index"`
	LastSnapshotIndex uint64           `json:"last_snapshot_index"`
	Peers             []RaftPeerStatus `json:"peers"`
}

func (r *RaftNode) Status() RaftStatus {
	status := RaftStatus{
		NodeID: r.cfg.NodeID,
		Role:   "UNKNOWN",
		Peers:  []RaftPeerStatus{},
	}
	if r.raft == nil {
		return status
	}

	switch r.raft.State() {
	case raft.Leader:
		status.Role = "LEADER"
	case raft.Follower:
		status.Role = "FOLLOWER"
	case raft.Candidate:
		status.Role = "CANDIDATE"
	case raft.Shutdown:
		status.Role = "SHUTDOWN"
	default:
		status.Role = strings.ToUpper(r.raft.State().String())
	}

	status.Term = r.raft.CurrentTerm()
	status.CommitIndex = r.raft.CommitIndex()
	status.AppliedIndex = r.raft.AppliedIndex()
	status.LastLogIndex = r.raft.LastIndex()

	status.LeaderAddr = r.LeaderAddr()
	status.LeaderID = r.LeaderID()

	stats := r.raft.Stats()
	if snapStr, ok := stats["last_snapshot_index"]; ok {
		status.LastSnapshotIndex, _ = strconv.ParseUint(snapStr, 10, 64)
	}

	future := r.raft.GetConfiguration()
	if err := future.Error(); err == nil {
		for _, s := range future.Configuration().Servers {
			if string(s.ID) == r.cfg.NodeID {
				continue
			}
			suffrage := "Voter"
			if s.Suffrage == raft.Nonvoter {
				suffrage = "Nonvoter"
			} else if s.Suffrage == raft.Staging {
				suffrage = "Staging"
			}
			status.Peers = append(status.Peers, RaftPeerStatus{
				NodeID:   string(s.ID),
				Address:  string(s.Address),
				Suffrage: suffrage,
			})
		}
	}

	return status
}

