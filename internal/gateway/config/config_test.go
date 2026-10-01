package config

import (
	"reflect"
	"testing"
)

func TestConfigLoadDefaults(t *testing.T) {
	for _, env := range []string{
		"S3_ADDR", "CONSOLE_ADDR", "METADATA_ADDR", "DATA_NODES",
		"ROOT_ACCESS_KEY_ID", "ROOT_SECRET_ACCESS_KEY", "CHUNK_SIZE",
		"WRITE_QUORUM", "MAX_CONCURRENT_UPLOADS",
	} {
		t.Setenv(env, "")
	}

	cfg := Load()

	if cfg.S3Addr != ":9000" {
		t.Errorf("expected S3Addr :9000, got %s", cfg.S3Addr)
	}
	if cfg.ConsoleAddr != ":9001" {
		t.Errorf("expected ConsoleAddr :9001, got %s", cfg.ConsoleAddr)
	}
	if cfg.MetadataAddr != "127.0.0.1:9090" {
		t.Errorf("expected MetadataAddr 127.0.0.1:9090, got %s", cfg.MetadataAddr)
	}
	expectedNodes := []string{"127.0.0.1:9101", "127.0.0.1:9102", "127.0.0.1:9103"}
	if !reflect.DeepEqual(cfg.DataNodes, expectedNodes) {
		t.Errorf("expected DataNodes %v, got %v", expectedNodes, cfg.DataNodes)
	}
	if cfg.RootAccessKey != "admin" {
		t.Errorf("expected RootAccessKey admin, got %s", cfg.RootAccessKey)
	}
	if cfg.RootSecretKey != "admin123" {
		t.Errorf("expected RootSecretKey admin123, got %s", cfg.RootSecretKey)
	}
	if cfg.ChunkSize != 4*1024*1024 {
		t.Errorf("expected ChunkSize %d, got %d", 4*1024*1024, cfg.ChunkSize)
	}
	if cfg.WriteQuorum != 2 {
		t.Errorf("expected WriteQuorum 2, got %d", cfg.WriteQuorum)
	}
	if cfg.MaxConcurrentUploads != 64 {
		t.Errorf("expected MaxConcurrentUploads 64, got %d", cfg.MaxConcurrentUploads)
	}
}

func TestConfigLoadCustom(t *testing.T) {
	t.Setenv("S3_ADDR", ":8000")
	t.Setenv("CONSOLE_ADDR", ":8001")
	t.Setenv("METADATA_ADDR", "meta.internal:9090")
	t.Setenv("DATA_NODES", "node1:9101, node2:9101")
	t.Setenv("ROOT_ACCESS_KEY_ID", "custom_key")
	t.Setenv("ROOT_SECRET_ACCESS_KEY", "custom_secret")
	t.Setenv("CHUNK_SIZE", "8388608")
	t.Setenv("WRITE_QUORUM", "3")
	t.Setenv("MAX_CONCURRENT_UPLOADS", "32")

	cfg := Load()

	if cfg.S3Addr != ":8000" {
		t.Errorf("expected S3Addr :8000, got %s", cfg.S3Addr)
	}
	if cfg.ConsoleAddr != ":8001" {
		t.Errorf("expected ConsoleAddr :8001, got %s", cfg.ConsoleAddr)
	}
	if cfg.MetadataAddr != "meta.internal:9090" {
		t.Errorf("expected MetadataAddr meta.internal:9090, got %s", cfg.MetadataAddr)
	}
	expectedNodes := []string{"node1:9101", "node2:9101"}
	if !reflect.DeepEqual(cfg.DataNodes, expectedNodes) {
		t.Errorf("expected DataNodes %v, got %v", expectedNodes, cfg.DataNodes)
	}
	if cfg.RootAccessKey != "custom_key" {
		t.Errorf("expected RootAccessKey custom_key, got %s", cfg.RootAccessKey)
	}
	if cfg.RootSecretKey != "custom_secret" {
		t.Errorf("expected RootSecretKey custom_secret, got %s", cfg.RootSecretKey)
	}
	if cfg.ChunkSize != 8388608 {
		t.Errorf("expected ChunkSize 8388608, got %d", cfg.ChunkSize)
	}
	if cfg.WriteQuorum != 3 {
		t.Errorf("expected WriteQuorum 3, got %d", cfg.WriteQuorum)
	}
	if cfg.MaxConcurrentUploads != 32 {
		t.Errorf("expected MaxConcurrentUploads 32, got %d", cfg.MaxConcurrentUploads)
	}
}
