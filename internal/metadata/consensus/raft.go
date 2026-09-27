package consensus

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/tharunn0/castor/internal/metadata/config"
)

type RaftNode struct {
	raft          *raft.Raft
	fsm           *FSM
	transport     raft.Transport
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	cfg           config.Config
}

func NewRaftNode(cfg config.Config, fsm *FSM) (*RaftNode, error) {
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
		return nil, err
	}
	transport, err := raft.NewTCPTransport(raftAddr.String(), nil, 3, 10*time.Second, io.Discard)
	if err != nil {
		_ = boltStore.Close()
		return nil, err
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 100 * time.Millisecond
	raftConfig.ElectionTimeout = 200 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 100 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond
	raftConfig.LogOutput = io.Discard

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
	}, nil
}

func (r *RaftNode) Apply(cmd *Command, timeout time.Duration) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	data, err := cmd.Encode()
	if err != nil {
		return err
	}

	deadline := time.Now().Add(timeout)
	for {
		future := r.raft.Apply(data, timeout)
		err := future.Error()
		if err == nil {
			if res, ok := future.Response().(error); ok && res != nil {
				return res
			}
			return nil
		}

		if errors.Is(err, raft.ErrNotLeader) && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		return err
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
		return fmt.Errorf("raft not initialized")
	}
	future := r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	return future.Error()
}

func (r *RaftNode) Leave(nodeID string) error {
	if r.raft == nil {
		return fmt.Errorf("raft not initialized")
	}
	future := r.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
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
