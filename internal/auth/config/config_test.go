package config

import (
	"errors"
	"testing"
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
