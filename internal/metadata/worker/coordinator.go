package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/tharunn0/castor/internal/metadata/consensus"
)

var (
	ErrCoordinatorStopped = errors.New("leader coordinator is stopped")
)

// LeadershipProvider abstracts RaftNode leadership status querying and observer registration.
type LeadershipProvider interface {
	IsLeader() bool
	AddLeadershipObserver(observer consensus.LeadershipObserver) func()
}

// Worker represents a background maintenance routine managed by LeaderCoordinator.
type Worker interface {
	Name() string
	Run(ctx context.Context)
}

type LeaderCoordinator struct {
	raft    LeadershipProvider
	workers []Worker
	logger  *slog.Logger

	mu           sync.Mutex
	baseCtx      context.Context
	unsub        func()
	started      bool
	running      bool
	stopped      bool
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
}

type Option func(*LeaderCoordinator)

func WithLogger(logger *slog.Logger) Option {
	return func(c *LeaderCoordinator) {
		if logger != nil {
			c.logger = logger
		}
	}
}

func WithWorkers(workers ...Worker) Option {
	return func(c *LeaderCoordinator) {
		for _, w := range workers {
			if w != nil {
				c.workers = append(c.workers, w)
			}
		}
	}
}

func NewLeaderCoordinator(raft LeadershipProvider, opts ...Option) *LeaderCoordinator {
	c := &LeaderCoordinator{
		raft:   raft,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *LeaderCoordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return ErrCoordinatorStopped
	}
	if c.started {
		c.mu.Unlock()
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.baseCtx = ctx
	c.started = true
	raft := c.raft
	c.mu.Unlock()

	if raft != nil {
		unsub := raft.AddLeadershipObserver(c)
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			unsub()
			return nil
		}
		c.unsub = unsub
		c.mu.Unlock()
	}

	go func() {
		<-ctx.Done()
		_ = c.Stop()
	}()

	return nil
}

func (c *LeaderCoordinator) Stop() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true

	unsub := c.unsub
	c.unsub = nil

	c.stopWorkersLocked()
	c.mu.Unlock()

	if unsub != nil {
		unsub()
	}

	return nil
}

func (c *LeaderCoordinator) OnLeadershipChange(isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}

	if isLeader {
		c.startWorkersLocked()
	} else {
		c.stopWorkersLocked()
	}
}

func (c *LeaderCoordinator) startWorkersLocked() {
	if c.running || len(c.workers) == 0 {
		return
	}
	c.running = true

	base := c.baseCtx
	if base == nil {
		base = context.Background()
	}
	c.workerCtx, c.workerCancel = context.WithCancel(base)

	for _, w := range c.workers {
		c.wg.Add(1)
		go func(w Worker) {
			defer c.wg.Done()
			c.logger.Info("starting leader worker", "worker", w.Name())
			w.Run(c.workerCtx)
			c.logger.Info("leader worker stopped", "worker", w.Name())
		}(w)
	}
}

func (c *LeaderCoordinator) stopWorkersLocked() {
	if !c.running {
		return
	}
	c.running = false

	if c.workerCancel != nil {
		c.workerCancel()
		c.workerCancel = nil
	}

	c.wg.Wait()
}

func (c *LeaderCoordinator) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}
