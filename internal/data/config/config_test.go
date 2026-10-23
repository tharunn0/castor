package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	os.Unsetenv("NODE_ID")
	os.Unsetenv("GRPC_ADDR")
	os.Unsetenv("METRICS_ADDR")
	os.Unsetenv("DATA_DIR")

	cfg := Load()

	if cfg.NodeId != "data-1" {
		t.Errorf("expected default NodeId 'data-1', got %q", cfg.NodeId)
	}
	if cfg.GRPCAddr != ":9101" {
		t.Errorf("expected default GRPCAddr ':9101', got %q", cfg.GRPCAddr)
	}
	if cfg.MetricsAddr != ":9104" {
		t.Errorf("expected default MetricsAddr ':9104', got %q", cfg.MetricsAddr)
	}
	if cfg.DataDir != "data" {
		t.Errorf("expected default DataDir 'data', got %q", cfg.DataDir)
	}
	if cfg.MetadataAddr != "127.0.0.1:9090" {
		t.Errorf("expected default MetadataAddr '127.0.0.1:9090', got %q", cfg.MetadataAddr)
	}
	if len(cfg.MetadataNodes) != 1 || cfg.MetadataNodes[0] != "127.0.0.1:9090" {
		t.Errorf("expected default MetadataNodes ['127.0.0.1:9090'], got %v", cfg.MetadataNodes)
	}
	if cfg.AdvertiseAddr != ":9101" {
		t.Errorf("expected default AdvertiseAddr ':9101', got %q", cfg.AdvertiseAddr)
	}
	if cfg.HeartbeatInterval != 3*time.Second {
		t.Errorf("expected default HeartbeatInterval 3s, got %v", cfg.HeartbeatInterval)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("NODE_ID", "data-node-99")
	t.Setenv("GRPC_ADDR", ":9201")
	t.Setenv("METRICS_ADDR", ":9204")
	t.Setenv("DATA_DIR", "/mnt/data")
	t.Setenv("METADATA_NODES", "meta-1:9090,meta-2:9090,meta-3:9090")
	t.Setenv("ADVERTISE_ADDR", "10.0.0.99:9201")
	t.Setenv("HEARTBEAT_INTERVAL", "5s")

	cfg := Load()

	if cfg.NodeId != "data-node-99" {
		t.Errorf("expected NodeId 'data-node-99', got %q", cfg.NodeId)
	}
	if cfg.GRPCAddr != ":9201" {
		t.Errorf("expected GRPCAddr ':9201', got %q", cfg.GRPCAddr)
	}
	if cfg.MetricsAddr != ":9204" {
		t.Errorf("expected MetricsAddr ':9204', got %q", cfg.MetricsAddr)
	}
	if cfg.DataDir != "/mnt/data" {
		t.Errorf("expected DataDir '/mnt/data', got %q", cfg.DataDir)
	}
	if len(cfg.MetadataNodes) != 3 || cfg.MetadataNodes[1] != "meta-2:9090" {
		t.Errorf("expected 3 metadata nodes, got %v", cfg.MetadataNodes)
	}
	if cfg.AdvertiseAddr != "10.0.0.99:9201" {
		t.Errorf("expected AdvertiseAddr '10.0.0.99:9201', got %q", cfg.AdvertiseAddr)
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Errorf("expected HeartbeatInterval 5s, got %v", cfg.HeartbeatInterval)
	}
}
