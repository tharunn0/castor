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
