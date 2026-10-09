package model

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCredentialStatus_Validate(t *testing.T) {
	tests := []struct {
		name    string
		status  CredentialStatus
		wantErr error
	}{
		{name: "valid active", status: StatusActive, wantErr: nil},
		{name: "valid revoked", status: StatusRevoked, wantErr: nil},
		{name: "empty status", status: "", wantErr: ErrInvalidCredentialStatus},
		{name: "unknown status", status: "PENDING", wantErr: ErrInvalidCredentialStatus},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.status.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestCreateCredentialInput_Validate(t *testing.T) {
	validUserID := uuid.New()

	tests := []struct {
		name    string
		input   CreateCredentialInput
		wantErr error
	}{
		{
			name: "valid input",
			input: CreateCredentialInput{
				AccessKeyID:     "CAST2026AKIAEXAMPLE1",
				SecretAccessKey: "secret_1234567890_abcdefghij",
				UserID:          validUserID,
				Label:           "Dev Laptop",
				Status:          StatusActive,
			},
			wantErr: nil,
		},
		{
			name: "access key too short",
			input: CreateCredentialInput{
				AccessKeyID:     "AK",
				SecretAccessKey: "secret_key_123",
				UserID:          validUserID,
				Label:           "Dev",
				Status:          StatusActive,
			},
			wantErr: ErrInvalidAccessKeyID,
		},
		{
			name: "access key too long",
			input: CreateCredentialInput{
				AccessKeyID:     strings.Repeat("A", 33),
				SecretAccessKey: "secret_key_123",
				UserID:          validUserID,
				Label:           "Dev",
				Status:          StatusActive,
			},
			wantErr: ErrInvalidAccessKeyID,
		},
		{
			name: "access key non-alphanumeric",
			input: CreateCredentialInput{
				AccessKeyID:     "KEY-WITH-DASHES!",
				SecretAccessKey: "secret_key_123",
				UserID:          validUserID,
				Label:           "Dev",
				Status:          StatusActive,
			},
			wantErr: ErrInvalidAccessKeyID,
		},
		{
			name: "secret access key empty",
			input: CreateCredentialInput{
				AccessKeyID:     "ACCESSKEY123",
				SecretAccessKey: "",
				UserID:          validUserID,
				Label:           "Dev",
				Status:          StatusActive,
			},
			wantErr: ErrInvalidSecretAccessKey,
		},
		{
			name: "secret access key too long",
			input: CreateCredentialInput{
				AccessKeyID:     "ACCESSKEY123",
				SecretAccessKey: strings.Repeat("s", 65),
				UserID:          validUserID,
				Label:           "Dev",
				Status:          StatusActive,
			},
			wantErr: ErrInvalidSecretAccessKey,
		},
		{
			name: "nil user id",
			input: CreateCredentialInput{
				AccessKeyID:     "ACCESSKEY123",
				SecretAccessKey: "secret_key_123",
				UserID:          uuid.Nil,
				Label:           "Dev",
				Status:          StatusActive,
			},
			wantErr: ErrInvalidUserID,
		},
		{
			name: "label too long",
			input: CreateCredentialInput{
				AccessKeyID:     "ACCESSKEY123",
				SecretAccessKey: "secret_key_123",
				UserID:          validUserID,
				Label:           strings.Repeat("l", 65),
				Status:          StatusActive,
			},
			wantErr: ErrInvalidLabel,
		},
		{
			name: "invalid status",
			input: CreateCredentialInput{
				AccessKeyID:     "ACCESSKEY123",
				SecretAccessKey: "secret_key_123",
				UserID:          validUserID,
				Label:           "Dev",
				Status:          "EXPIRED",
			},
			wantErr: ErrInvalidCredentialStatus,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.input.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestS3Credential_Validate(t *testing.T) {
	validCred := S3Credential{
		AccessKeyID:     "CAST2026AKIAEXAMPLE1",
		SecretAccessKey: "secret_1234567890_abcdefghij",
		UserID:          uuid.New(),
		Label:           "CLI Access",
		Status:          StatusActive,
		CreatedAt:       time.Now().UTC(),
	}

	tests := []struct {
		name    string
		modify  func(c *S3Credential)
		wantErr error
	}{
		{
			name:    "valid credential",
			modify:  func(c *S3Credential) {},
			wantErr: nil,
		},
		{
			name: "invalid access key",
			modify: func(c *S3Credential) {
				c.AccessKeyID = "bad"
				c.AccessKeyID = "A!"
			},
			wantErr: ErrInvalidAccessKeyID,
		},
		{
			name: "invalid secret key",
			modify: func(c *S3Credential) {
				c.SecretAccessKey = ""
			},
			wantErr: ErrInvalidSecretAccessKey,
		},
		{
			name: "nil user id",
			modify: func(c *S3Credential) {
				c.UserID = uuid.Nil
			},
			wantErr: ErrInvalidUserID,
		},
		{
			name: "invalid label",
			modify: func(c *S3Credential) {
				c.Label = strings.Repeat("x", 65)
			},
			wantErr: ErrInvalidLabel,
		},
		{
			name: "invalid status",
			modify: func(c *S3Credential) {
				c.Status = "SUSPENDED"
			},
			wantErr: ErrInvalidCredentialStatus,
		},
		{
			name: "zero created_at",
			modify: func(c *S3Credential) {
				c.CreatedAt = time.Time{}
			},
			wantErr: ErrInvalidCreatedAt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validCred
			tc.modify(&c)
			err := c.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestGenerateAccessKeyID(t *testing.T) {
	seen := make(map[string]bool)
	for _ = range 100 {
		key, err := GenerateAccessKeyID()
		if err != nil {
			t.Fatalf("unexpected error generating access key id: %v", err)
		}
		if len(key) != 20 {
			t.Fatalf("expected key length 20, got %d (%s)", len(key), key)
		}
		if !strings.HasPrefix(key, "AKIA") {
			t.Fatalf("expected key to start with AKIA, got %s", key)
		}
		if err := validateAccessKeyID(key); err != nil {
			t.Fatalf("generated key failed validation: %v", err)
		}
		if seen[key] {
			t.Fatalf("duplicate access key generated: %s", key)
		}
		seen[key] = true
	}
}

func TestGenerateSecretAccessKey(t *testing.T) {
	seen := make(map[string]bool)
	for _ = range 100 {
		secret, err := GenerateSecretAccessKey()
		if err != nil {
			t.Fatalf("unexpected error generating secret access key: %v", err)
		}
		if len(secret) != 40 {
			t.Fatalf("expected secret length 40, got %d (%s)", len(secret), secret)
		}
		if err := validateSecretAccessKey(secret); err != nil {
			t.Fatalf("generated secret failed validation: %v", err)
		}
		if seen[secret] {
			t.Fatalf("duplicate secret key generated: %s", secret)
		}
		seen[secret] = true
	}
}

func TestGenerateCredentials(t *testing.T) {
	accessKey, secretKey, err := GenerateCredentials()
	if err != nil {
		t.Fatalf("unexpected error generating credentials: %v", err)
	}

	input := CreateCredentialInput{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		UserID:          uuid.New(),
		Label:           "Generated Credential",
		Status:          StatusActive,
	}

	if err := input.Validate(); err != nil {
		t.Fatalf("expected generated credentials to pass input validation, got: %v", err)
	}
}

