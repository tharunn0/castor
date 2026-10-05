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
	createUserFunc        func(ctx context.Context, input model.CreateUserInput) (*model.User, error)
	getUserByUsernameFunc func(ctx context.Context, username string) (*model.User, error)
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
	if m.getUserByUsernameFunc != nil {
		return m.getUserByUsernameFunc(ctx, username)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	return nil, repository.ErrNotImplemented
}

func TestService_Register_AdminKeyScenarios(t *testing.T) {
	configuredAdminKey := "super-secret-admin-key"

	tests := []struct {
		name              string
		cfgAdminKey       string
		cfgAdminSecretKey string
		reqRole           model.Role
		reqAdminKey       string
		expectedRole      model.Role
	}{
		{
			name:         "valid admin key with admin role requested grants admin role",
			cfgAdminKey:  configuredAdminKey,
			reqRole:      model.RoleAdmin,
			reqAdminKey:  configuredAdminKey,
			expectedRole: model.RoleAdmin,
		},
		{
			name:              "valid admin_secret_key with admin role requested grants admin role",
			cfgAdminSecretKey: configuredAdminKey,
			reqRole:           model.RoleAdmin,
			reqAdminKey:       configuredAdminKey,
			expectedRole:      model.RoleAdmin,
		},
		{
			name:         "incorrect admin key with admin role requested grants user role",
			cfgAdminKey:  configuredAdminKey,
			reqRole:      model.RoleAdmin,
			reqAdminKey:  "wrong-key",
			expectedRole: model.RoleUser,
		},
		{
			name:              "incorrect admin key with admin_secret_key and admin role requested grants user role",
			cfgAdminSecretKey: configuredAdminKey,
			reqRole:           model.RoleAdmin,
			reqAdminKey:       "wrong-key",
			expectedRole:      model.RoleUser,
		},
		{
			name:         "empty request admin key with admin role requested grants user role",
			cfgAdminKey:  configuredAdminKey,
			reqRole:      model.RoleAdmin,
			reqAdminKey:  "",
			expectedRole: model.RoleUser,
		},
		{
			name:         "valid admin key but user role requested keeps user role without checking key",
			cfgAdminKey:  configuredAdminKey,
			reqRole:      model.RoleUser,
			reqAdminKey:  configuredAdminKey,
			expectedRole: model.RoleUser,
		},
		{
			name:         "valid admin key but empty role requested defaults to user role",
			cfgAdminKey:  configuredAdminKey,
			reqRole:      "",
			reqAdminKey:  configuredAdminKey,
			expectedRole: model.RoleUser,
		},
		{
			name:         "unconfigured server admin key with admin role requested grants user role",
			cfgAdminKey:  "",
			reqRole:      model.RoleAdmin,
			reqAdminKey:  "some-key",
			expectedRole: model.RoleUser,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{
				AdminKey:       tc.cfgAdminKey,
				AdminSecretKey: tc.cfgAdminSecretKey,
			}

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
				Role:     tc.reqRole,
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

func TestService_Login(t *testing.T) {
	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte("correctpassword"), 10)
	userID := uuid.New()
	mockUser := &model.User{
		ID:           userID,
		Username:     "validuser",
		Email:        "valid@example.com",
		PasswordHash: string(hashedPassword),
		Role:         model.RoleUser,
		CreatedAt:    time.Now().UTC(),
	}

	cfg := config.Config{
		JWTSecret: "test-jwt-secret-key-32-chars-long!",
		JWTExpiry: 1 * time.Hour,
	}

	t.Run("successful login", func(t *testing.T) {
		repo := &mockUserRepo{
			getUserByUsernameFunc: func(ctx context.Context, username string) (*model.User, error) {
				if username == "validuser" {
					return mockUser, nil
				}
				return nil, repository.ErrUserNotFound
			},
		}

		svc := NewService(cfg, repo)
		resp, err := svc.Login(context.Background(), model.LoginRequest{
			Username: "validuser",
			Password: "correctpassword",
		})
		if err != nil {
			t.Fatalf("unexpected error on login: %v", err)
		}
		if resp.Token == "" {
			t.Error("expected non-empty JWT token")
		}
		if resp.Username != "validuser" {
			t.Errorf("expected username validuser, got %s", resp.Username)
		}
		if resp.ID != userID {
			t.Errorf("expected ID %v, got %v", userID, resp.ID)
		}
		if resp.Role != model.RoleUser {
			t.Errorf("expected role USER, got %s", resp.Role)
		}
	})

	t.Run("incorrect password", func(t *testing.T) {
		repo := &mockUserRepo{
			getUserByUsernameFunc: func(ctx context.Context, username string) (*model.User, error) {
				return mockUser, nil
			},
		}

		svc := NewService(cfg, repo)
		_, err := svc.Login(context.Background(), model.LoginRequest{
			Username: "validuser",
			Password: "wrongpassword",
		})
		if !errors.Is(err, model.ErrInvalidCredentials) {
			t.Fatalf("expected ErrInvalidCredentials, got %v", err)
		}
	})

	t.Run("user not found", func(t *testing.T) {
		repo := &mockUserRepo{
			getUserByUsernameFunc: func(ctx context.Context, username string) (*model.User, error) {
				return nil, repository.ErrUserNotFound
			},
		}

		svc := NewService(cfg, repo)
		_, err := svc.Login(context.Background(), model.LoginRequest{
			Username: "nonexistent",
			Password: "password123",
		})
		if !errors.Is(err, model.ErrInvalidCredentials) {
			t.Fatalf("expected ErrInvalidCredentials, got %v", err)
		}
	})

	t.Run("validation error", func(t *testing.T) {
		svc := NewService(cfg, &mockUserRepo{})
		_, err := svc.Login(context.Background(), model.LoginRequest{
			Username: "al",
			Password: "123",
		})
		if err == nil {
			t.Fatal("expected validation error, got nil")
		}
	})
}
