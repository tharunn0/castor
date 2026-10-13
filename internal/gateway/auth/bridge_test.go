package auth

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	gwauth "github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
)

func newTestLogger() *slog.Logger {
	h := slog.DiscardHandler
	return slog.New(h)
}

type mockClient struct {
	validateKeyFunc func(ctx context.Context, accessKeyID string) (*ValidateKeyResponse, error)
}

func (m *mockClient) ValidateKey(ctx context.Context, accessKeyID string) (*ValidateKeyResponse, error) {
	if m.validateKeyFunc != nil {
		return m.validateKeyFunc(ctx, accessKeyID)
	}
	return nil, ErrCredentialNotFound
}

func TestBridge_GetUserAccount(t *testing.T) {
	rootKey := "admin"
	rootSecret := "adminSecret123"

	mock := &mockClient{
		validateKeyFunc: func(ctx context.Context, accessKeyID string) (*ValidateKeyResponse, error) {
			switch accessKeyID {
			case "AKIAACTIVEUSER123456":
				return &ValidateKeyResponse{
					AccessKeyID:     "AKIAACTIVEUSER123456",
					SecretAccessKey: "userSecretKey456789",
					UserID:          "usr-1",
					Status:          StatusActive,
					CreatedAt:       time.Now(),
				}, nil
			case "AKIAREVOKEDUSER12345":
				return nil, ErrCredentialRevoked
			case "AKIANOTFOUND12345678":
				return nil, ErrCredentialNotFound
			case "AKIASERVERERROR12345":
				return nil, errors.New("connection failed")
			default:
				return nil, ErrCredentialNotFound
			}
		},
	}

	bridge := NewBridge(mock, rootKey, rootSecret, newTestLogger())

	t.Run("empty access key", func(t *testing.T) {
		_, err := bridge.GetUserAccount("")
		if !errors.Is(err, gwauth.ErrNoSuchUser) {
			t.Fatalf("expected ErrNoSuchUser, got %v", err)
		}
	})

	t.Run("root credential bypass", func(t *testing.T) {
		acc, err := bridge.GetUserAccount(rootKey)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acc.Access != rootKey || acc.Secret != rootSecret || acc.Role != gwauth.RoleAdmin {
			t.Fatalf("unexpected root account: %+v", acc)
		}
	})

	t.Run("active user account", func(t *testing.T) {
		acc, err := bridge.GetUserAccount("AKIAACTIVEUSER123456")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acc.Access != "AKIAACTIVEUSER123456" || acc.Secret != "userSecretKey456789" || acc.Role != gwauth.RoleUserPlus || acc.Arn != "usr-1" {
			t.Fatalf("unexpected user account: %+v", acc)
		}
	})

	t.Run("revoked credential returns ErrNoSuchUser", func(t *testing.T) {
		_, err := bridge.GetUserAccount("AKIAREVOKEDUSER12345")
		if !errors.Is(err, gwauth.ErrNoSuchUser) {
			t.Fatalf("expected ErrNoSuchUser, got %v", err)
		}
	})

	t.Run("not found credential returns ErrNoSuchUser", func(t *testing.T) {
		_, err := bridge.GetUserAccount("AKIANOTFOUND12345678")
		if !errors.Is(err, gwauth.ErrNoSuchUser) {
			t.Fatalf("expected ErrNoSuchUser, got %v", err)
		}
	})

	t.Run("server error propagated", func(t *testing.T) {
		_, err := bridge.GetUserAccount("AKIASERVERERROR12345")
		if err == nil || errors.Is(err, gwauth.ErrNoSuchUser) {
			t.Fatalf("expected server error to be returned, got %v", err)
		}
	})

	t.Run("nil client returns ErrNoSuchUser", func(t *testing.T) {
		nilBridge := NewBridge(nil, rootKey, rootSecret, newTestLogger())
		_, err := nilBridge.GetUserAccount("AKIAACTIVEUSER123456")
		if !errors.Is(err, gwauth.ErrNoSuchUser) {
			t.Fatalf("expected ErrNoSuchUser, got %v", err)
		}
	})
}

func TestBridge_ResolveAccounts(t *testing.T) {
	rootKey := "admin"
	rootSecret := "adminSecret"

	mock := &mockClient{
		validateKeyFunc: func(ctx context.Context, accessKeyID string) (*ValidateKeyResponse, error) {
			if accessKeyID == "AKIAVALID1" {
				return &ValidateKeyResponse{
					AccessKeyID:     "AKIAVALID1",
					SecretAccessKey: "secret1",
					Status:          StatusActive,
				}, nil
			}
			return nil, ErrCredentialNotFound
		},
	}

	bridge := NewBridge(mock, rootKey, rootSecret, newTestLogger())

	keys := []string{"admin", "AKIAVALID1", "AKIAMISSING1", "AKIAMISSING2"}
	missing, err := bridge.ResolveAccounts(keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(missing) != 2 || missing[0] != "AKIAMISSING1" || missing[1] != "AKIAMISSING2" {
		t.Fatalf("expected missing accounts [AKIAMISSING1, AKIAMISSING2], got %v", missing)
	}
}

func TestBridge_UnsupportedMethods(t *testing.T) {
	bridge := NewBridge(nil, "admin", "adminSecret", newTestLogger())

	checkUnsupported := func(name string, err error) {
		t.Helper()
		apiErr, ok := err.(s3err.APIError)
		if !ok || apiErr.Code != "MethodNotAllowed" && apiErr.Code != "NotImplemented" && apiErr.Description != s3err.GetAPIError(s3err.ErrAdminMethodNotSupported).Description {
			t.Fatalf("%s: expected ErrAdminMethodNotSupported, got %v", name, err)
		}
	}

	checkUnsupported("CreateAccount", bridge.CreateAccount(gwauth.Account{}))
	checkUnsupported("UpdateUserAccount", bridge.UpdateUserAccount("user", gwauth.MutableProps{}))
	checkUnsupported("DeleteUserAccount", bridge.DeleteUserAccount("user"))

	_, err := bridge.ListUserAccounts()
	checkUnsupported("ListUserAccounts", err)

	if err := bridge.Shutdown(); err != nil {
		t.Fatalf("expected Shutdown to return nil, got %v", err)
	}
}
