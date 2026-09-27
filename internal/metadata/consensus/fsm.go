package consensus

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tharunn0/castor/internal/metadata/store"
)

type FSM struct {
	store *store.Store
}

func NewFSM(s *store.Store) *FSM {
	return &FSM{
		store: s,
	}
}

func (f *FSM) Apply(log *raft.Log) any {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := f.applyCommand(ctx, cmd)
	if err != nil {
		return err
	}

	return res
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.store}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.store.Restore(rc)
}

func (f *FSM) applyCreateBucket(ctx context.Context, cmd *Command) (any, error) {
	req, err := cmd.DecodeCreateBucket()
	if err != nil {
		return nil, err
	}
	return f.store.CreateBucket(ctx, req.Bucket, req.OwnerId, req.Tags)
}

func (f *FSM) applyDeleteBucket(ctx context.Context, cmd *Command) (any, error) {
	req, err := cmd.DecodeDeleteBucket()
	if err != nil {
		return nil, err
	}
	return nil, f.store.DeleteBucket(ctx, req.Bucket)
}

func (f *FSM) applyCommitManifest(ctx context.Context, cmd *Command) (any, error) {
	req, err := cmd.DecodeCommitManifest()
	if err != nil {
		return nil, err
	}
	if err := f.store.EnsureBucket(ctx, req.Bucket, req.OwnerId); err != nil {
		return nil, err
	}
	return f.store.CommitManifest(ctx, req)
}

func (f *FSM) applyDeleteManifest(ctx context.Context, cmd *Command) (any, error) {
	req, err := cmd.DecodeDeleteManifest()
	if err != nil {
		return nil, err
	}
	return nil, f.store.DeleteManifest(ctx, req.Bucket, req.Key)
}

func (f *FSM) applyUpdateChunkLocation(ctx context.Context, cmd *Command) (any, error) {
	req, err := cmd.DecodeUpdateChunkLocation()
	if err != nil {
		return nil, err
	}
	return nil, f.store.UpdateChunkLocation(ctx, req.ChunkHash, req.NodeAddresses)
}

func (f *FSM) applyRemoveChunkLocations(ctx context.Context, cmd *Command) (any, error) {
	req, err := cmd.DecodeRemoveChunkLocations()
	if err != nil {
		return nil, err
	}
	return nil, f.store.DeleteChunkLocation(ctx, req.ChunkHash)
}

func (f *FSM) applyCommand(ctx context.Context, cmd *Command) (any, error) {
	switch cmd.Type {
	case CmdCreateBucket:
		return f.applyCreateBucket(ctx, cmd)
	case CmdDeleteBucket:
		return f.applyDeleteBucket(ctx, cmd)
	case CmdCommitManifest:
		return f.applyCommitManifest(ctx, cmd)
	case CmdDeleteManifest:
		return f.applyDeleteManifest(ctx, cmd)
	case CmdUpdateChunkLocation:
		return f.applyUpdateChunkLocation(ctx, cmd)
	case CmdRemoveChunkLocations:
		return f.applyRemoveChunkLocations(ctx, cmd)
	default:
		return nil, fmt.Errorf("unsupported command type: %v", cmd.Type)
	}
}

type fsmSnapshot struct {
	store *store.Store
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.store.Backup(sink); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {
}

var _ raft.FSM = (*FSM)(nil)
var _ raft.FSMSnapshot = (*fsmSnapshot)(nil)
