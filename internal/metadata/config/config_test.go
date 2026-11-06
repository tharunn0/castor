package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	os.Clearenv()

	cfg := Load()
	if cfg.NodeID != "metadata-1" {
		t.Errorf("expected default NodeID metadata-1, got %s", cfg.NodeID)
	}
	if cfg.MetricsAddr != ":9096" {
		t.Errorf("expected default MetricsAddr :9096, got %s", cfg.MetricsAddr)
	}
	if cfg.GRPCAddr != ":9090" {
		t.Errorf("expected default GRPCAddr :9090, got %s", cfg.GRPCAddr)
	}
}

func TestLoad_CustomMetricsAddr(t *testing.T) {
	t.Setenv("METRICS_ADDR", ":19096")
	t.Setenv("NODE_ID", "metadata-test")

	cfg := Load()
	if cfg.MetricsAddr != ":19096" {
		t.Errorf("expected custom MetricsAddr :19096, got %s", cfg.MetricsAddr)
	}
	if cfg.NodeID != "metadata-test" {
		t.Errorf("expected custom NodeID metadata-test, got %s", cfg.NodeID)
	}
}

func TestLoad_RaftAdvertiseAddr(t *testing.T) {
	t.Setenv("RAFT_ADDR", ":9091")
	t.Setenv("RAFT_ADVERTISE_ADDR", "metadata-0.metadata.castor.svc.cluster.local:9091")

	cfg := Load()
	if cfg.RaftAddr != ":9091" {
		t.Fatalf("RaftAddr = %q, want %q", cfg.RaftAddr, ":9091")
	}
	if cfg.RaftAdvertiseAddr != "metadata-0.metadata.castor.svc.cluster.local:9091" {
		t.Fatalf("RaftAdvertiseAddr = %q", cfg.RaftAdvertiseAddr)
	}
}

func TestLoad_RaftAdvertiseAddrDefaultsEmpty(t *testing.T) {
	t.Setenv("RAFT_ADVERTISE_ADDR", "")

	if cfg := Load(); cfg.RaftAdvertiseAddr != "" {
		t.Fatalf("RaftAdvertiseAddr = %q, want empty", cfg.RaftAdvertiseAddr)
	}
}
