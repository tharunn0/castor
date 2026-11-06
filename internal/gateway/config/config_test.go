package config

import (
	"errors"
	"testing"
	"time"

	"github.com/tharunn0/castor/internal/telemetry"
)

func TestConfigLoadDefaults(t *testing.T) {
	for _, env := range []string{
		"S3_ADDR", "CONSOLE_ADDR", "METADATA_ADDR", "AUTH_ADDR",
		"ROOT_ACCESS_KEY_ID", "ROOT_SECRET_ACCESS_KEY", "CHUNK_SIZE",
		"WRITE_QUORUM", "WRITE_QUORUM_GRACE_PERIOD", "MAX_CONCURRENT_UPLOADS",
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
	if cfg.AuthAddr != "http://127.0.0.1:9095" {
		t.Errorf("expected AuthAddr http://127.0.0.1:9095, got %s", cfg.AuthAddr)
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
	if cfg.WriteQuorumGracePeriod != 100*time.Millisecond {
		t.Errorf("expected WriteQuorumGracePeriod 100ms, got %v", cfg.WriteQuorumGracePeriod)
	}
	if cfg.MaxConcurrentUploads != 64 {
		t.Errorf("expected MaxConcurrentUploads 64, got %d", cfg.MaxConcurrentUploads)
	}
}

func TestConfigLoadCustom(t *testing.T) {
	t.Setenv("S3_ADDR", ":8000")
	t.Setenv("CONSOLE_ADDR", ":8001")
	t.Setenv("METADATA_ADDR", "meta.internal:9090")
	t.Setenv("AUTH_ADDR", "http://auth.internal:9095")
	t.Setenv("ROOT_ACCESS_KEY_ID", "custom_key")
	t.Setenv("ROOT_SECRET_ACCESS_KEY", "custom_secret")
	t.Setenv("CHUNK_SIZE", "8388608")
	t.Setenv("WRITE_QUORUM", "3")
	t.Setenv("WRITE_QUORUM_GRACE_PERIOD", "250ms")
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
	if cfg.AuthAddr != "http://auth.internal:9095" {
		t.Errorf("expected AuthAddr http://auth.internal:9095, got %s", cfg.AuthAddr)
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
	if cfg.WriteQuorumGracePeriod != 250*time.Millisecond {
		t.Errorf("expected WriteQuorumGracePeriod 250ms, got %v", cfg.WriteQuorumGracePeriod)
	}
	if cfg.MaxConcurrentUploads != 32 {
		t.Errorf("expected MaxConcurrentUploads 32, got %d", cfg.MaxConcurrentUploads)
	}
}

func TestConfigLoadGracePeriodDisabled(t *testing.T) {
	t.Setenv("WRITE_QUORUM_GRACE_PERIOD", "0s")
	cfg := Load()
	if cfg.WriteQuorumGracePeriod != 0 {
		t.Errorf("expected WriteQuorumGracePeriod 0, got %v", cfg.WriteQuorumGracePeriod)
	}
}

func TestConfigValidateProduction(t *testing.T) {
	base := Config{RootAccessKey: "root", RootSecretKey: "root-secret", AuthInternalToken: "tok", Env: telemetry.EnvProd}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{name: "valid production config", mutate: func(*Config) {}, wantErr: nil},
		{name: "default root access key", mutate: func(c *Config) { c.RootAccessKey = DefaultRootAccessKey }, wantErr: ErrDefaultRootCredsInProd},
		{name: "default root secret key", mutate: func(c *Config) { c.RootSecretKey = DefaultRootSecretKey }, wantErr: ErrDefaultRootCredsInProd},
		{name: "missing internal token", mutate: func(c *Config) { c.AuthInternalToken = "" }, wantErr: ErrInternalTokenRequired},
		{name: "staging enforces checks", mutate: func(c *Config) {
			c.Env = telemetry.EnvStage
			c.RootSecretKey = DefaultRootSecretKey
		}, wantErr: ErrDefaultRootCredsInProd},
		{name: "staging valid config", mutate: func(c *Config) { c.Env = telemetry.EnvStage }, wantErr: nil},
		{name: "dev allows defaults", mutate: func(c *Config) {
			c.Env = telemetry.EnvDev
			c.RootAccessKey = DefaultRootAccessKey
			c.RootSecretKey = DefaultRootSecretKey
			c.AuthInternalToken = ""
		}, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			if err := cfg.Validate(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigLoadInternalTokenAndEnv(t *testing.T) {
	t.Setenv("AUTH_INTERNAL_TOKEN", "tok")
	t.Setenv("APP_ENV", "prod")

	cfg := Load()
	if cfg.AuthInternalToken != "tok" {
		t.Fatalf("AuthInternalToken = %q, want %q", cfg.AuthInternalToken, "tok")
	}
	if cfg.Env != telemetry.EnvProd {
		t.Fatalf("Env = %q, want %q", cfg.Env, telemetry.EnvProd)
	}
}

func TestConfigValidateTLSPair(t *testing.T) {
	tests := []struct {
		name    string
		cert    string
		key     string
		wantErr error
	}{
		{name: "no tls", cert: "", key: "", wantErr: nil},
		{name: "cert and key", cert: "/tls/tls.crt", key: "/tls/tls.key", wantErr: nil},
		{name: "cert only", cert: "/tls/tls.crt", key: "", wantErr: ErrIncompleteTLSConfig},
		{name: "key only", cert: "", key: "/tls/tls.key", wantErr: ErrIncompleteTLSConfig},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{S3TLSCertFile: tt.cert, S3TLSKeyFile: tt.key, Env: telemetry.EnvDev}
			if err := cfg.Validate(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigLoadTLSFiles(t *testing.T) {
	t.Setenv("S3_TLS_CERT_FILE", "/tls/tls.crt")
	t.Setenv("S3_TLS_KEY_FILE", "/tls/tls.key")

	cfg := Load()
	if cfg.S3TLSCertFile != "/tls/tls.crt" || cfg.S3TLSKeyFile != "/tls/tls.key" {
		t.Fatalf("got cert=%q key=%q", cfg.S3TLSCertFile, cfg.S3TLSKeyFile)
	}
}

func TestConfigLoadTimeouts(t *testing.T) {
	tests := []struct {
		name                 string
		call, leader, chunk  string
		wantCall, wantLeader time.Duration
		wantChunk            time.Duration
	}{
		{
			name:     "defaults",
			wantCall: DefaultMetadataCallTimeout, wantLeader: DefaultMetadataLeaderWait, wantChunk: DefaultDataChunkTimeout,
		},
		{
			name: "custom values", call: "1500ms", leader: "5s", chunk: "1m",
			wantCall: 1500 * time.Millisecond, wantLeader: 5 * time.Second, wantChunk: time.Minute,
		},
		{
			name: "invalid and non-positive fall back", call: "nope", leader: "0s", chunk: "-1s",
			wantCall: DefaultMetadataCallTimeout, wantLeader: DefaultMetadataLeaderWait, wantChunk: DefaultDataChunkTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("METADATA_CALL_TIMEOUT", tt.call)
			t.Setenv("METADATA_LEADER_WAIT", tt.leader)
			t.Setenv("DATA_CHUNK_TIMEOUT", tt.chunk)

			cfg := Load()
			if cfg.MetadataCallTimeout != tt.wantCall {
				t.Fatalf("MetadataCallTimeout = %v, want %v", cfg.MetadataCallTimeout, tt.wantCall)
			}
			if cfg.MetadataLeaderWait != tt.wantLeader {
				t.Fatalf("MetadataLeaderWait = %v, want %v", cfg.MetadataLeaderWait, tt.wantLeader)
			}
			if cfg.DataChunkTimeout != tt.wantChunk {
				t.Fatalf("DataChunkTimeout = %v, want %v", cfg.DataChunkTimeout, tt.wantChunk)
			}
		})
	}
}
