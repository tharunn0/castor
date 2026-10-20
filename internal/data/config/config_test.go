package config

import (
	"os"
	"testing"
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
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("NODE_ID", "data-node-99")
	t.Setenv("GRPC_ADDR", ":9201")
	t.Setenv("METRICS_ADDR", ":9204")
	t.Setenv("DATA_DIR", "/mnt/data")

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
}
