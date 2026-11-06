package config

import (
	"errors"
	"testing"

	"github.com/tharunn0/castor/internal/telemetry"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name: "valid with AdminSecretKey",
			cfg: Config{
				AdminSecretKey: "secret-key",
			},
			wantErr: nil,
		},
		{
			name: "valid with AdminKey fallback",
			cfg: Config{
				AdminKey: "legacy-key",
			},
			wantErr: nil,
		},
		{
			name: "missing admin secret key",
			cfg: Config{
				AdminSecretKey: "",
				AdminKey:       "",
			},
			wantErr: ErrAdminSecretKeyRequired,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLoad_Validation(t *testing.T) {
	// Clean environment variables for test isolation
	t.Setenv("ADMIN_SECRET_KEY", "")
	t.Setenv("AUTH_ADMIN_SECRET_KEY", "")
	t.Setenv("ADMIN_KEY", "")
	t.Setenv("AUTH_ADMIN_KEY", "")

	_, err := Load()
	if !errors.Is(err, ErrAdminSecretKeyRequired) {
		t.Fatalf("expected ErrAdminSecretKeyRequired when key is missing, got %v", err)
	}

	t.Setenv("ADMIN_SECRET_KEY", "test-admin-secret")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading config with valid key: %v", err)
	}
	if cfg.AdminSecretKey != "test-admin-secret" {
		t.Fatalf("expected AdminSecretKey %q, got %q", "test-admin-secret", cfg.AdminSecretKey)
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("ADMIN_SECRET_KEY", "default-test-secret")
	t.Setenv("AUTH_READ_TIMEOUT", "")
	t.Setenv("AUTH_WRITE_TIMEOUT", "")
	t.Setenv("AUTH_IDLE_TIMEOUT", "")
	t.Setenv("AUTH_SHUTDOWN_TIMEOUT", "")
	t.Setenv("AUTH_DB_MAX_CONNS", "")
	t.Setenv("AUTH_DB_MIN_CONNS", "")
	t.Setenv("AUTH_DB_MAX_CONN_LIFETIME", "")
	t.Setenv("AUTH_DB_MAX_CONN_IDLE_TIME", "")
	t.Setenv("AUTH_DB_HEALTH_CHECK_PERIOD", "")
	t.Setenv("AUTH_DB_CONNECT_TIMEOUT", "")
	t.Setenv("AUTH_BCRYPT_COST", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading defaults: %v", err)
	}

	if cfg.ReadTimeout.Seconds() != 10 {
		t.Fatalf("expected ReadTimeout 10s, got %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout.Seconds() != 10 {
		t.Fatalf("expected WriteTimeout 10s, got %v", cfg.WriteTimeout)
	}
	if cfg.IdleTimeout.Seconds() != 120 {
		t.Fatalf("expected IdleTimeout 120s, got %v", cfg.IdleTimeout)
	}
	if cfg.ShutdownTimeout.Seconds() != 10 {
		t.Fatalf("expected ShutdownTimeout 10s, got %v", cfg.ShutdownTimeout)
	}
	if cfg.DBMaxConns != 25 {
		t.Fatalf("expected DBMaxConns 25, got %d", cfg.DBMaxConns)
	}
	if cfg.DBMinConns != 5 {
		t.Fatalf("expected DBMinConns 5, got %d", cfg.DBMinConns)
	}
	if cfg.DBMaxConnLifetime.Minutes() != 60 {
		t.Fatalf("expected DBMaxConnLifetime 1h, got %v", cfg.DBMaxConnLifetime)
	}
	if cfg.DBMaxConnIdleTime.Minutes() != 30 {
		t.Fatalf("expected DBMaxConnIdleTime 30m, got %v", cfg.DBMaxConnIdleTime)
	}
	if cfg.DBHealthCheckPeriod.Minutes() != 1 {
		t.Fatalf("expected DBHealthCheckPeriod 1m, got %v", cfg.DBHealthCheckPeriod)
	}
	if cfg.DBConnectTimeout.Seconds() != 5 {
		t.Fatalf("expected DBConnectTimeout 5s, got %v", cfg.DBConnectTimeout)
	}
	if cfg.BcryptCost != 12 {
		t.Fatalf("expected BcryptCost 12, got %d", cfg.BcryptCost)
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	t.Setenv("ADMIN_SECRET_KEY", "custom-secret")
	t.Setenv("AUTH_READ_TIMEOUT", "15s")
	t.Setenv("AUTH_WRITE_TIMEOUT", "20s")
	t.Setenv("AUTH_IDLE_TIMEOUT", "60s")
	t.Setenv("AUTH_SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("AUTH_DB_MAX_CONNS", "50")
	t.Setenv("AUTH_DB_MIN_CONNS", "10")
	t.Setenv("AUTH_DB_MAX_CONN_LIFETIME", "2h")
	t.Setenv("AUTH_DB_MAX_CONN_IDLE_TIME", "45m")
	t.Setenv("AUTH_DB_HEALTH_CHECK_PERIOD", "2m")
	t.Setenv("AUTH_DB_CONNECT_TIMEOUT", "8s")
	t.Setenv("AUTH_BCRYPT_COST", "14")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading custom config: %v", err)
	}

	if cfg.ReadTimeout.Seconds() != 15 {
		t.Fatalf("expected ReadTimeout 15s, got %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout.Seconds() != 20 {
		t.Fatalf("expected WriteTimeout 20s, got %v", cfg.WriteTimeout)
	}
	if cfg.IdleTimeout.Seconds() != 60 {
		t.Fatalf("expected IdleTimeout 60s, got %v", cfg.IdleTimeout)
	}
	if cfg.ShutdownTimeout.Seconds() != 5 {
		t.Fatalf("expected ShutdownTimeout 5s, got %v", cfg.ShutdownTimeout)
	}
	if cfg.DBMaxConns != 50 {
		t.Fatalf("expected DBMaxConns 50, got %d", cfg.DBMaxConns)
	}
	if cfg.DBMinConns != 10 {
		t.Fatalf("expected DBMinConns 10, got %d", cfg.DBMinConns)
	}
	if cfg.DBMaxConnLifetime.Hours() != 2 {
		t.Fatalf("expected DBMaxConnLifetime 2h, got %v", cfg.DBMaxConnLifetime)
	}
	if cfg.DBMaxConnIdleTime.Minutes() != 45 {
		t.Fatalf("expected DBMaxConnIdleTime 45m, got %v", cfg.DBMaxConnIdleTime)
	}
	if cfg.DBHealthCheckPeriod.Minutes() != 2 {
		t.Fatalf("expected DBHealthCheckPeriod 2m, got %v", cfg.DBHealthCheckPeriod)
	}
	if cfg.DBConnectTimeout.Seconds() != 8 {
		t.Fatalf("expected DBConnectTimeout 8s, got %v", cfg.DBConnectTimeout)
	}
	if cfg.BcryptCost != 14 {
		t.Fatalf("expected BcryptCost 14, got %d", cfg.BcryptCost)
	}
}

func TestLoad_InvalidFallbacks(t *testing.T) {
	t.Setenv("ADMIN_SECRET_KEY", "fallback-secret")
	t.Setenv("AUTH_READ_TIMEOUT", "invalid-duration")
	t.Setenv("AUTH_DB_MAX_CONNS", "-5")
	t.Setenv("AUTH_DB_MIN_CONNS", "100") // min conns > max conns
	t.Setenv("AUTH_BCRYPT_COST", "99")   // out of range (max is 31)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading fallback config: %v", err)
	}

	if cfg.ReadTimeout.Seconds() != 10 {
		t.Fatalf("expected fallback ReadTimeout 10s, got %v", cfg.ReadTimeout)
	}
	if cfg.DBMaxConns != 25 {
		t.Fatalf("expected DBMaxConns fallback 25, got %d", cfg.DBMaxConns)
	}
	if cfg.DBMinConns != 25 {
		t.Fatalf("expected DBMinConns clamped to max conns 25, got %d", cfg.DBMinConns)
	}
	if cfg.BcryptCost != 12 {
		t.Fatalf("expected BcryptCost fallback 12, got %d", cfg.BcryptCost)
	}
}

func TestValidate_Production(t *testing.T) {
	base := Config{AdminSecretKey: "admin", JWTSecret: "a-real-jwt-secret", InternalToken: "tok", Env: telemetry.EnvProd}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{name: "valid production config", mutate: func(*Config) {}, wantErr: nil},
		{name: "missing internal token", mutate: func(c *Config) { c.InternalToken = "" }, wantErr: ErrInternalTokenRequired},
		{name: "default jwt secret", mutate: func(c *Config) { c.JWTSecret = DefaultJWTSecret }, wantErr: ErrDefaultJWTSecretInProd},
		{name: "staging enforces checks", mutate: func(c *Config) {
			c.Env = telemetry.EnvStage
			c.InternalToken = ""
		}, wantErr: ErrInternalTokenRequired},
		{name: "staging valid config", mutate: func(c *Config) { c.Env = telemetry.EnvStage }, wantErr: nil},
		{name: "dev allows defaults", mutate: func(c *Config) {
			c.Env = telemetry.EnvDev
			c.InternalToken = ""
			c.JWTSecret = DefaultJWTSecret
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

func TestLoad_InternalTokenAndEnv(t *testing.T) {
	t.Setenv("ADMIN_SECRET_KEY", "admin")
	t.Setenv("APP_ENV", "production")
	t.Setenv("AUTH_JWT_SECRET", "a-real-jwt-secret")
	t.Setenv("AUTH_INTERNAL_TOKEN", "")

	if _, err := Load(); !errors.Is(err, ErrInternalTokenRequired) {
		t.Fatalf("expected ErrInternalTokenRequired, got %v", err)
	}

	t.Setenv("AUTH_INTERNAL_TOKEN", "tok")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.InternalToken != "tok" || cfg.Env != telemetry.EnvProd {
		t.Fatalf("got InternalToken=%q Env=%q", cfg.InternalToken, cfg.Env)
	}
}
