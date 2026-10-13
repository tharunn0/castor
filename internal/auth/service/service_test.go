package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"github.com/tharunn0/castor/internal/auth/authctx"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
	authtel "github.com/tharunn0/castor/internal/auth/telemetry"
	"github.com/tharunn0/castor/internal/telemetry"
)

type mockUserRepo struct {
	createUserFunc        func(ctx context.Context, input model.CreateUserInput) (*model.User, error)
	getUserByUsernameFunc func(ctx context.Context, username string) (*model.User, error)
	getUserByIDFunc       func(ctx context.Context, id uuid.UUID) (*model.User, error)
}

func (m *mockUserRepo) CreateUser(ctx context.Context, input model.CreateUserInput) (*model.User, error) {
	if m.createUserFunc != nil {
		return m.createUserFunc(ctx, input)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) GetUserByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	if m.getUserByIDFunc != nil {
		return m.getUserByIDFunc(ctx, id)
	}
	return nil, repository.ErrNotImplemented
}

type mockCredentialRepo struct {
	createCredentialFunc         func(ctx context.Context, input model.CreateCredentialInput) (*model.S3Credential, error)
	getCredentialByAccessKeyFunc func(ctx context.Context, accessKeyID string) (*model.S3Credential, error)
	listCredentialsByUserIDFunc  func(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error)
	updateCredentialStatusFunc   func(ctx context.Context, accessKeyID string, status model.CredentialStatus) error
	deleteCredentialFunc         func(ctx context.Context, accessKeyID string) error
}

func (m *mockCredentialRepo) CreateCredential(ctx context.Context, input model.CreateCredentialInput) (*model.S3Credential, error) {
	if m.createCredentialFunc != nil {
		return m.createCredentialFunc(ctx, input)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockCredentialRepo) GetCredentialByAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	if m.getCredentialByAccessKeyFunc != nil {
		return m.getCredentialByAccessKeyFunc(ctx, accessKeyID)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockCredentialRepo) ListCredentialsByUserID(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	if m.listCredentialsByUserIDFunc != nil {
		return m.listCredentialsByUserIDFunc(ctx, userID)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockCredentialRepo) UpdateCredentialStatus(ctx context.Context, accessKeyID string, status model.CredentialStatus) error {
	if m.updateCredentialStatusFunc != nil {
		return m.updateCredentialStatusFunc(ctx, accessKeyID, status)
	}
	return repository.ErrNotImplemented
}

func (m *mockCredentialRepo) DeleteCredential(ctx context.Context, accessKeyID string) error {
	if m.deleteCredentialFunc != nil {
		return m.deleteCredentialFunc(ctx, accessKeyID)
	}
	return repository.ErrNotImplemented
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

func (m *mockUserRepo) CreateCredential(ctx context.Context, input model.CreateCredentialInput) (*model.S3Credential, error) {
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) GetCredentialByAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) ListCredentialsByUserID(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	return nil, repository.ErrNotImplemented
}

func (m *mockUserRepo) UpdateCredentialStatus(ctx context.Context, accessKeyID string, status model.CredentialStatus) error {
	return repository.ErrNotImplemented
}

func (m *mockUserRepo) DeleteCredential(ctx context.Context, accessKeyID string) error {
	return repository.ErrNotImplemented
}

func (m *mockUserRepo) Ping(ctx context.Context) error {
	return nil
}

func (m *mockUserRepo) Close() {}

type mockRepo struct {
	user *mockUserRepo
	cred *mockCredentialRepo
}

func newMockRepo(u *mockUserRepo, c *mockCredentialRepo) *mockRepo {
	if u == nil {
		u = &mockUserRepo{}
	}
	if c == nil {
		c = &mockCredentialRepo{}
	}
	return &mockRepo{user: u, cred: c}
}

func (m *mockRepo) CreateUser(ctx context.Context, input model.CreateUserInput) (*model.User, error) {
	return m.user.CreateUser(ctx, input)
}

func (m *mockRepo) GetUserByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	return m.user.GetUserByID(ctx, id)
}

func (m *mockRepo) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	return m.user.GetUserByUsername(ctx, username)
}

func (m *mockRepo) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	return m.user.GetUserByEmail(ctx, email)
}

func (m *mockRepo) CreateCredential(ctx context.Context, input model.CreateCredentialInput) (*model.S3Credential, error) {
	return m.cred.CreateCredential(ctx, input)
}

func (m *mockRepo) GetCredentialByAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	return m.cred.GetCredentialByAccessKey(ctx, accessKeyID)
}

func (m *mockRepo) ListCredentialsByUserID(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	return m.cred.ListCredentialsByUserID(ctx, userID)
}

func (m *mockRepo) UpdateCredentialStatus(ctx context.Context, accessKeyID string, status model.CredentialStatus) error {
	return m.cred.UpdateCredentialStatus(ctx, accessKeyID, status)
}

func (m *mockRepo) DeleteCredential(ctx context.Context, accessKeyID string) error {
	return m.cred.DeleteCredential(ctx, accessKeyID)
}

func (m *mockRepo) Ping(ctx context.Context) error {
	return nil
}

func (m *mockRepo) Close() {}

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

func TestService_CreateCredential(t *testing.T) {
	cfg := config.Config{}
	userID := uuid.New()

	t.Run("successful credential creation", func(t *testing.T) {
		userRepo := &mockUserRepo{
			getUserByIDFunc: func(ctx context.Context, id uuid.UUID) (*model.User, error) {
				return &model.User{ID: id, Username: "testuser"}, nil
			},
		}

		credRepo := &mockCredentialRepo{
			createCredentialFunc: func(ctx context.Context, in model.CreateCredentialInput) (*model.S3Credential, error) {
				return &model.S3Credential{
					AccessKeyID:     in.AccessKeyID,
					SecretAccessKey: in.SecretAccessKey,
					UserID:          in.UserID,
					Label:           in.Label,
					Status:          in.Status,
					CreatedAt:       time.Now().UTC(),
				}, nil
			},
		}

		svc := NewService(cfg, newMockRepo(userRepo, credRepo))
		cred, err := svc.CreateCredential(context.Background(), userID, "Test Laptop")
		if err != nil {
			t.Fatalf("unexpected error creating credential: %v", err)
		}
		if cred.UserID != userID {
			t.Errorf("expected user ID %s, got %s", userID, cred.UserID)
		}
		if cred.Label != "Test Laptop" {
			t.Errorf("expected label 'Test Laptop', got %s", cred.Label)
		}
		if cred.Status != model.StatusActive {
			t.Errorf("expected status ACTIVE, got %s", cred.Status)
		}
		if cred.SecretAccessKey == "" {
			t.Error("expected secret access key to be populated on creation")
		}
	})

	t.Run("extract user id from context when nil", func(t *testing.T) {
		userRepo := &mockUserRepo{
			getUserByIDFunc: func(ctx context.Context, id uuid.UUID) (*model.User, error) {
				return &model.User{ID: id, Username: "testuser"}, nil
			},
		}

		credRepo := &mockCredentialRepo{
			createCredentialFunc: func(ctx context.Context, in model.CreateCredentialInput) (*model.S3Credential, error) {
				return &model.S3Credential{
					AccessKeyID: in.AccessKeyID,
					UserID:      in.UserID,
					Status:      in.Status,
				}, nil
			},
		}

		svc := NewService(cfg, newMockRepo(userRepo, credRepo))
		ctxWithUser := authctx.WithUser(context.Background(), authctx.UserContext{UserID: userID})
		cred, err := svc.CreateCredential(ctxWithUser, uuid.Nil, "Context Key")
		if err != nil {
			t.Fatalf("unexpected error creating credential from context: %v", err)
		}
		if cred.UserID != userID {
			t.Errorf("expected user ID %s from context, got %s", userID, cred.UserID)
		}
	})

	t.Run("nil user id error", func(t *testing.T) {
		svc := NewService(cfg, &mockUserRepo{})
		_, err := svc.CreateCredential(context.Background(), uuid.Nil, "label")
		if !errors.Is(err, model.ErrInvalidUserID) {
			t.Fatalf("expected ErrInvalidUserID, got %v", err)
		}
	})

	t.Run("label too long error", func(t *testing.T) {
		svc := NewService(cfg, &mockUserRepo{})
		longLabel := strings.Repeat("x", 65)
		_, err := svc.CreateCredential(context.Background(), userID, longLabel)
		if !errors.Is(err, model.ErrInvalidLabel) {
			t.Fatalf("expected ErrInvalidLabel, got %v", err)
		}
	})

	t.Run("user not found error", func(t *testing.T) {
		userRepo := &mockUserRepo{
			getUserByIDFunc: func(ctx context.Context, id uuid.UUID) (*model.User, error) {
				return nil, repository.ErrUserNotFound
			},
		}
		svc := NewService(cfg, userRepo)
		_, err := svc.CreateCredential(context.Background(), userID, "label")
		if !errors.Is(err, repository.ErrUserNotFound) {
			t.Fatalf("expected ErrUserNotFound, got %v", err)
		}
	})
}

func TestService_ListCredentials(t *testing.T) {
	cfg := config.Config{}
	userID := uuid.New()

	t.Run("successful list sanitizes secret keys", func(t *testing.T) {
		credRepo := &mockCredentialRepo{
			listCredentialsByUserIDFunc: func(ctx context.Context, id uuid.UUID) ([]*model.S3Credential, error) {
				return []*model.S3Credential{
					{
						AccessKeyID:     "AKIAKEYONE12345678",
						SecretAccessKey: "supersecret1",
						UserID:          id,
						Status:          model.StatusActive,
					},
					{
						AccessKeyID:     "AKIAKEYTWO12345678",
						SecretAccessKey: "supersecret2",
						UserID:          id,
						Status:          model.StatusRevoked,
					},
				}, nil
			},
		}

		svc := NewService(cfg, newMockRepo(nil, credRepo))
		creds, err := svc.ListCredentials(context.Background(), userID)
		if err != nil {
			t.Fatalf("unexpected error listing credentials: %v", err)
		}
		if len(creds) != 2 {
			t.Fatalf("expected 2 credentials, got %d", len(creds))
		}
		for _, c := range creds {
			if c.SecretAccessKey != "" {
				t.Errorf("expected secret access key to be stripped, got %q", c.SecretAccessKey)
			}
		}
	})

	t.Run("nil user id error", func(t *testing.T) {
		svc := NewService(cfg, &mockUserRepo{})
		_, err := svc.ListCredentials(context.Background(), uuid.Nil)
		if !errors.Is(err, model.ErrInvalidUserID) {
			t.Fatalf("expected ErrInvalidUserID, got %v", err)
		}
	})
}

func TestService_RevokeCredential(t *testing.T) {
	cfg := config.Config{}
	userID := uuid.New()
	accessKey := "AKIAEXAMPLEKEY123456"

	t.Run("successful revocation", func(t *testing.T) {
		var updatedStatus model.CredentialStatus
		credRepo := &mockCredentialRepo{
			getCredentialByAccessKeyFunc: func(ctx context.Context, key string) (*model.S3Credential, error) {
				return &model.S3Credential{
					AccessKeyID: key,
					UserID:      userID,
					Status:      model.StatusActive,
				}, nil
			},
			updateCredentialStatusFunc: func(ctx context.Context, key string, status model.CredentialStatus) error {
				updatedStatus = status
				return nil
			},
		}

		svc := NewService(cfg, newMockRepo(nil, credRepo))
		err := svc.RevokeCredential(context.Background(), userID, accessKey)
		if err != nil {
			t.Fatalf("unexpected error revoking credential: %v", err)
		}
		if updatedStatus != model.StatusRevoked {
			t.Errorf("expected status %s, got %s", model.StatusRevoked, updatedStatus)
		}
	})

	t.Run("ownership mismatch error", func(t *testing.T) {
		otherUser := uuid.New()
		credRepo := &mockCredentialRepo{
			getCredentialByAccessKeyFunc: func(ctx context.Context, key string) (*model.S3Credential, error) {
				return &model.S3Credential{
					AccessKeyID: key,
					UserID:      otherUser,
					Status:      model.StatusActive,
				}, nil
			},
		}

		svc := NewService(cfg, newMockRepo(nil, credRepo))
		err := svc.RevokeCredential(context.Background(), userID, accessKey)
		if !errors.Is(err, repository.ErrCredentialNotFound) {
			t.Fatalf("expected ErrCredentialNotFound on ownership mismatch, got %v", err)
		}
	})

	t.Run("credential not found error", func(t *testing.T) {
		credRepo := &mockCredentialRepo{
			getCredentialByAccessKeyFunc: func(ctx context.Context, key string) (*model.S3Credential, error) {
				return nil, repository.ErrCredentialNotFound
			},
		}

		svc := NewService(cfg, newMockRepo(nil, credRepo))
		err := svc.RevokeCredential(context.Background(), userID, accessKey)
		if !errors.Is(err, repository.ErrCredentialNotFound) {
			t.Fatalf("expected ErrCredentialNotFound, got %v", err)
		}
	})

	t.Run("nil user id error", func(t *testing.T) {
		svc := NewService(cfg, &mockUserRepo{})
		err := svc.RevokeCredential(context.Background(), uuid.Nil, accessKey)
		if !errors.Is(err, model.ErrInvalidUserID) {
			t.Fatalf("expected ErrInvalidUserID, got %v", err)
		}
	})

	t.Run("empty access key error", func(t *testing.T) {
		svc := NewService(cfg, &mockUserRepo{})
		err := svc.RevokeCredential(context.Background(), userID, "")
		if !errors.Is(err, model.ErrInvalidAccessKeyID) {
			t.Fatalf("expected ErrInvalidAccessKeyID, got %v", err)
		}
	})
}

func TestService_ValidateAccessKey(t *testing.T) {
	cfg := config.Config{}
	accessKey := "AKIAVALIDKEY12345678"

	t.Run("successful validation of active credential", func(t *testing.T) {
		credRepo := &mockCredentialRepo{
			getCredentialByAccessKeyFunc: func(ctx context.Context, key string) (*model.S3Credential, error) {
				return &model.S3Credential{
					AccessKeyID:     key,
					SecretAccessKey: "secret",
					Status:          model.StatusActive,
				}, nil
			},
		}

		svc := NewService(cfg, newMockRepo(nil, credRepo))
		cred, err := svc.ValidateAccessKey(context.Background(), accessKey)
		if err != nil {
			t.Fatalf("unexpected error validating active key: %v", err)
		}
		if cred.AccessKeyID != accessKey {
			t.Errorf("expected access key %s, got %s", accessKey, cred.AccessKeyID)
		}
	})

	t.Run("revoked credential error", func(t *testing.T) {
		credRepo := &mockCredentialRepo{
			getCredentialByAccessKeyFunc: func(ctx context.Context, key string) (*model.S3Credential, error) {
				return &model.S3Credential{
					AccessKeyID: key,
					Status:      model.StatusRevoked,
				}, nil
			},
		}

		svc := NewService(cfg, newMockRepo(nil, credRepo))
		_, err := svc.ValidateAccessKey(context.Background(), accessKey)
		if !errors.Is(err, model.ErrCredentialRevoked) {
			t.Fatalf("expected ErrCredentialRevoked, got %v", err)
		}
	})

	t.Run("not found credential error", func(t *testing.T) {
		credRepo := &mockCredentialRepo{
			getCredentialByAccessKeyFunc: func(ctx context.Context, key string) (*model.S3Credential, error) {
				return nil, repository.ErrCredentialNotFound
			},
		}

		svc := NewService(cfg, newMockRepo(nil, credRepo))
		_, err := svc.ValidateAccessKey(context.Background(), accessKey)
		if !errors.Is(err, repository.ErrCredentialNotFound) {
			t.Fatalf("expected ErrCredentialNotFound, got %v", err)
		}
	})

	t.Run("empty access key error", func(t *testing.T) {
		svc := NewService(cfg, &mockUserRepo{})
		_, err := svc.ValidateAccessKey(context.Background(), "")
		if !errors.Is(err, model.ErrInvalidAccessKeyID) {
			t.Fatalf("expected ErrInvalidAccessKeyID, got %v", err)
		}
	})
}

func TestService_Telemetry(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	telMetrics := telemetry.NewMetrics("test_auth_service", "dev")
	authMetrics := authtel.NewAuthMetrics(telMetrics)

	cfg := config.Config{
		JWTSecret: "test-secret-key-that-is-long-enough",
		JWTExpiry: time.Hour,
	}

	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte("StrongPass123"), 4)
	userRepo := &mockUserRepo{
		getUserByUsernameFunc: func(ctx context.Context, username string) (*model.User, error) {
			return &model.User{
				ID:           uuid.New(),
				Username:     "alice",
				PasswordHash: string(hashedPassword),
				Role:         model.RoleUser,
			}, nil
		},
	}

	svc := NewService(cfg, userRepo).
		WithTracer(tp.Tracer("auth-service-test")).
		WithMetrics(authMetrics)

	ctx := context.Background()
	_, err := svc.Login(ctx, model.LoginRequest{
		Username: "alice",
		Password: "StrongPass123",
	})
	if err != nil {
		t.Fatalf("unexpected login error: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected spans to be captured")
	}

	foundLogin := false
	foundVerify := false
	for _, s := range spans {
		if s.Name == "auth.login" {
			foundLogin = true
		}
		if s.Name == "auth.verify_password" {
			foundVerify = true
		}
	}

	if !foundLogin {
		t.Error("expected auth.login span")
	}
	if !foundVerify {
		t.Error("expected auth.verify_password span")
	}
}

func TestService_Register_ConfigurableBcryptCost(t *testing.T) {
	var savedHash string
	userRepo := &mockUserRepo{
		createUserFunc: func(ctx context.Context, input model.CreateUserInput) (*model.User, error) {
			savedHash = input.PasswordHash
			return &model.User{
				ID:           uuid.New(),
				Username:     input.Username,
				Email:        input.Email,
				PasswordHash: input.PasswordHash,
				Role:         input.Role,
				CreatedAt:    time.Now().UTC(),
			}, nil
		},
	}

	cfg := config.Config{
		BcryptCost: 4, // minimum cost for fast execution in tests
	}
	svc := NewService(cfg, userRepo)

	_, err := svc.Register(context.Background(), model.RegisterRequest{
		Username: "bob",
		Email:    "bob@example.com",
		Password: "SecurePassword123!",
	})
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	cost, err := bcrypt.Cost([]byte(savedHash))
	if err != nil {
		t.Fatalf("failed to get bcrypt cost: %v", err)
	}
	if cost != 4 {
		t.Errorf("expected bcrypt cost 4, got %d", cost)
	}
}


