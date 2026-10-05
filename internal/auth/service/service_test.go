package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
)

type mockUserRepo struct {
	createUserFunc func(ctx context.Context, input model.CreateUserInput) (*model.User, error)
}

func (m *mockUserRepo) CreateUser(ctx context.Context, input model.CreateUserInput) (*model.User, error) {
	if m.createUserFunc != nil {
		return m.createUserFunc(ctx, input)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) GetUserByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	return nil, repository.ErrNotImplemented
}

func TestService_Register_AdminKeyScenarios(t *testing.T) {
	configuredAdminKey := "super-secret-admin-key"

	tests := []struct {
		name         string
		cfgAdminKey  string
		reqAdminKey  string
		expectedRole model.Role
	}{
		{
			name:         "valid admin key grants admin role",
			cfgAdminKey:  configuredAdminKey,
			reqAdminKey:  configuredAdminKey,
			expectedRole: model.RoleAdmin,
		},
		{
			name:         "incorrect admin key grants user role",
			cfgAdminKey:  configuredAdminKey,
			reqAdminKey:  "wrong-key",
			expectedRole: model.RoleUser,
		},
		{
			name:         "empty request admin key grants user role",
			cfgAdminKey:  configuredAdminKey,
			reqAdminKey:  "",
			expectedRole: model.RoleUser,
		},
		{
			name:         "unconfigured server admin key grants user role even if key provided",
			cfgAdminKey:  "",
			reqAdminKey:  "some-key",
			expectedRole: model.RoleUser,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{AdminKey: tc.cfgAdminKey}

			var capturedInput model.CreateUserInput
			repo := &mockUserRepo{
				createUserFunc: func(ctx context.Context, input model.CreateUserInput) (*model.User, error) {
					capturedInput = input
					return &model.User{
						ID:        uuid.New(),
						Username:  input.Username,
						Email:     input.Email,
						Role:      input.Role,
						CreatedAt: time.Now().UTC(),
					}, nil
				},
			}

			svc := NewService(cfg, repo)
			resp, err := svc.Register(context.Background(), model.RegisterRequest{
				Username: "testuser",
				Email:    "test@example.com",
				Password: "password123",
				AdminKey: tc.reqAdminKey,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedInput.Role != tc.expectedRole {
				t.Errorf("expected role %s passed to repo, got %s", tc.expectedRole, capturedInput.Role)
			}
			if resp.Role != tc.expectedRole {
				t.Errorf("expected response role %s, got %s", tc.expectedRole, resp.Role)
			}

			if err := bcrypt.CompareHashAndPassword([]byte(capturedInput.PasswordHash), []byte("password123")); err != nil {
				t.Errorf("password hash verification failed: %v", err)
			}
		})
	}
}

func TestService_Register_ValidationErrors(t *testing.T) {
	svc := NewService(config.Config{}, &mockUserRepo{})

	_, err := svc.Register(context.Background(), model.RegisterRequest{
		Username: "ab",
		Email:    "invalid-email",
		Password: "123",
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestService_Register_RepoError(t *testing.T) {
	expectedErr := repository.ErrUserAlreadyExists
	repo := &mockUserRepo{
		createUserFunc: func(ctx context.Context, input model.CreateUserInput) (*model.User, error) {
			return nil, expectedErr
		},
	}

	svc := NewService(config.Config{}, repo)
	_, err := svc.Register(context.Background(), model.RegisterRequest{
		Username: "existinguser",
		Email:    "existing@example.com",
		Password: "password123",
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}
