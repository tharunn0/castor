package registry

import (
	"sync"
	"testing"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
)

func TestRegistry_RegisterAndHeartbeat(t *testing.T) {
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	reg := New(
		WithOfflineTimeout(10*time.Second),
		WithNowFunc(func() time.Time { return now }),
	)

	// 1. Validation errors
	if err := reg.Register("", "10.0.0.1:9101", 1000, 500); err == nil {
		t.Fatal("expected error registering with empty node ID")
	}
	if err := reg.Register("node-1", "", 1000, 500); err == nil {
		t.Fatal("expected error registering with empty grpc address")
	}

	// 2. Successful registration
	if err := reg.Register("node-1", "10.0.0.1:9101", 1000, 500); err != nil {
		t.Fatalf("failed to register node: %v", err)
	}

	node, ok := reg.GetNode("node-1")
	if !ok {
		t.Fatal("expected node-1 to exist")
	}
	if node.Status != castorv1.NodeStatus_NODE_STATUS_HEALTHY {
		t.Fatalf("expected HEALTHY, got %v", node.Status)
	}
	if node.FreeRatio != 0.5 {
		t.Fatalf("expected FreeRatio 0.5, got %f", node.FreeRatio)
	}

	// 3. Update via Heartbeat
	if err := reg.Heartbeat("node-1", "10.0.0.1:9101", 1000, 20, now); err != nil {
		t.Fatalf("failed to process heartbeat: %v", err)
	}

	node, _ = reg.GetNode("node-1")
	if node.Status != castorv1.NodeStatus_NODE_STATUS_DEGRADED {
		t.Fatalf("expected DEGRADED (<5%% space), got %v", node.Status)
	}

	// 4. Auto-register on heartbeat
	if err := reg.Heartbeat("node-2", "10.0.0.2:9102", 2000, 1500, now); err != nil {
		t.Fatalf("failed to auto-register node-2: %v", err)
	}

	active := reg.GetActiveNodes()
	if len(active) != 2 {
		t.Fatalf("expected 2 active nodes, got %d", len(active))
	}
}

func TestRegistry_OfflineTransition(t *testing.T) {
	currentTime := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	reg := New(
		WithOfflineTimeout(5*time.Second),
		WithNowFunc(func() time.Time { return currentTime }),
	)

	_ = reg.Register("node-1", "10.0.0.1:9101", 1000, 500)
	_ = reg.Register("node-2", "10.0.0.2:9102", 1000, 500)

	// Advance time past offline timeout for node-1 only
	currentTime = currentTime.Add(6 * time.Second)
	_ = reg.Heartbeat("node-2", "10.0.0.2:9102", 1000, 500, currentTime)

	active := reg.GetActiveNodes()
	if len(active) != 1 {
		t.Fatalf("expected 1 active node after timeout, got %d", len(active))
	}
	if active[0].NodeId != "node-2" {
		t.Fatalf("expected node-2 to be active, got %s", active[0].NodeId)
	}

	all := reg.GetAllNodes()
	if len(all) != 2 {
		t.Fatalf("expected 2 total nodes in GetAllNodes, got %d", len(all))
	}

	node1, _ := reg.GetNode("node-1")
	if node1.Status != castorv1.NodeStatus_NODE_STATUS_OFFLINE {
		t.Fatalf("expected node-1 to be OFFLINE, got %v", node1.Status)
	}
}

func TestRegistry_Concurrency(t *testing.T) {
	reg := New()
	var wg sync.WaitGroup

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := "node-" + string(rune('a'+id))
			addr := "10.0.0." + string(rune('1'+id)) + ":9101"
			for range 50 {
				_ = reg.Heartbeat(nodeID, addr, 10000, 5000, time.Now())
				_ = reg.GetActiveNodes()
			}
		}(i)
	}

	wg.Wait()
	if len(reg.GetActiveNodes()) != 10 {
		t.Fatalf("expected 10 active nodes, got %d", len(reg.GetActiveNodes()))
	}
}
