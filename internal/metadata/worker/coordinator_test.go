package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tharunn0/castor/internal/metadata/consensus"
)

type mockLeadershipProvider struct {
	mu        sync.Mutex
	isLeader  bool
	observers []consensus.LeadershipObserver
}

func (m *mockLeadershipProvider) IsLeader() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isLeader
}

func (m *mockLeadershipProvider) AddLeadershipObserver(obs consensus.LeadershipObserver) func() {
	m.mu.Lock()
	m.observers = append(m.observers, obs)
	leader := m.isLeader
	m.mu.Unlock()

	if leader {
		obs.OnLeadershipChange(true)
	}

	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for i, o := range m.observers {
			if o == obs {
				m.observers = append(m.observers[:i], m.observers[i+1:]...)
				return
			}
		}
	}
}

func (m *mockLeadershipProvider) setLeader(leader bool) {
	m.mu.Lock()
	m.isLeader = leader
	obsList := make([]consensus.LeadershipObserver, len(m.observers))
	copy(obsList, m.observers)
	m.mu.Unlock()

	for _, obs := range obsList {
		obs.OnLeadershipChange(leader)
	}
}

type mockWorker struct {
	name       string
	startCount atomic.Int32
	stopCount  atomic.Int32
	running    atomic.Bool
}

func (w *mockWorker) Name() string {
	return w.name
}

func (w *mockWorker) Run(ctx context.Context) {
	w.startCount.Add(1)
	w.running.Store(true)
	defer func() {
		w.running.Store(false)
		w.stopCount.Add(1)
	}()

	<-ctx.Done()
}

func TestLeaderCoordinator_Lifecycle(t *testing.T) {
	mockRaft := &mockLeadershipProvider{isLeader: false}
	worker1 := &mockWorker{name: "worker-1"}
	worker2 := &mockWorker{name: "worker-2"}

	coord := NewLeaderCoordinator(mockRaft, WithWorkers(worker1, worker2))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coord.Start(ctx); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	if coord.IsRunning() {
		t.Fatal("expected coordinator to not be running workers when raft is not leader")
	}

	// 1. Leadership acquired
	mockRaft.setLeader(true)

	// Wait briefly for workers to start
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if coord.IsRunning() && worker1.running.Load() && worker2.running.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !coord.IsRunning() {
		t.Fatal("expected coordinator to be running after leadership acquired")
	}
	if !worker1.running.Load() || !worker2.running.Load() {
		t.Fatal("expected all workers to be running")
	}
	if worker1.startCount.Load() != 1 || worker2.startCount.Load() != 1 {
		t.Fatalf("expected 1 start call per worker, got w1=%d, w2=%d", worker1.startCount.Load(), worker2.startCount.Load())
	}

	// 2. Leadership lost
	mockRaft.setLeader(false)

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !coord.IsRunning() && !worker1.running.Load() && !worker2.running.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if coord.IsRunning() {
		t.Fatal("expected coordinator to stop workers when leadership lost")
	}
	if worker1.running.Load() || worker2.running.Load() {
		t.Fatal("expected workers to be stopped")
	}
	if worker1.stopCount.Load() != 1 || worker2.stopCount.Load() != 1 {
		t.Fatalf("expected 1 stop call per worker, got w1=%d, w2=%d", worker1.stopCount.Load(), worker2.stopCount.Load())
	}

	// 3. Leadership re-acquired
	mockRaft.setLeader(true)

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if coord.IsRunning() && worker1.running.Load() && worker2.running.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !coord.IsRunning() {
		t.Fatal("expected coordinator to restart workers on re-acquiring leadership")
	}
	if worker1.startCount.Load() != 2 || worker2.startCount.Load() != 2 {
		t.Fatalf("expected 2 start calls per worker, got w1=%d, w2=%d", worker1.startCount.Load(), worker2.startCount.Load())
	}

	// 4. Stop coordinator cleanly
	if err := coord.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	if coord.IsRunning() {
		t.Fatal("expected coordinator to not be running after Stop()")
	}
	if worker1.running.Load() || worker2.running.Load() {
		t.Fatal("expected workers to terminate upon Stop()")
	}

	// 5. Start after Stop should fail
	if err := coord.Start(ctx); !errors.Is(err, ErrCoordinatorStopped) {
		t.Fatalf("expected ErrCoordinatorStopped, got %v", err)
	}
}

func TestLeaderCoordinator_ContextCancellation(t *testing.T) {
	mockRaft := &mockLeadershipProvider{isLeader: true}
	worker := &mockWorker{name: "ctx-worker"}

	coord := NewLeaderCoordinator(mockRaft, WithWorkers(worker))

	ctx, cancel := context.WithCancel(context.Background())
	if err := coord.Start(ctx); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if coord.IsRunning() && worker.running.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !coord.IsRunning() {
		t.Fatal("expected coordinator to be running")
	}

	// Cancel base context
	cancel()

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !coord.IsRunning() && !worker.running.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if coord.IsRunning() {
		t.Fatal("expected coordinator to stop after base context was cancelled")
	}
	if worker.running.Load() {
		t.Fatal("expected worker to stop after base context was cancelled")
	}
}
