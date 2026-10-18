package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/authctx"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
	authtel "github.com/tharunn0/castor/internal/auth/telemetry"
	"github.com/tharunn0/castor/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/bcrypt"
)

type AuthService interface {
	Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
	Login(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error)
	CreateCredential(ctx context.Context, userID uuid.UUID, label string) (*model.S3Credential, error)
	ListCredentials(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error)
	RevokeCredential(ctx context.Context, userID uuid.UUID, accessKeyID string) error
	ValidateAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error)
}

type Service struct {
	cfg     config.Config
	repo    repository.Repository
	logger  *slog.Logger
	tracer  trace.Tracer
	metrics *authtel.AuthMetrics
}

func NewService(cfg config.Config, repo repository.Repository) *Service {
	return &Service{
		cfg:     cfg,
		repo:    repo,
		logger:  slog.Default(),
		tracer:  otel.GetTracerProvider().Tracer("auth-service"),
		metrics: authtel.NewAuthMetrics(telemetry.DefaultMetrics()),
	}
}

func (s *Service) WithLogger(l *slog.Logger) *Service {
	if l != nil {
		s.logger = l
	}
	return s
}

func (s *Service) WithTracer(tr trace.Tracer) *Service {
	if tr != nil {
		s.tracer = tr
	}
	return s
}

func (s *Service) WithMetrics(m *authtel.AuthMetrics) *Service {
	s.metrics = m
	return s
}

func (s *Service) Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
	ctx, span := s.tracer.Start(ctx, "auth.register")
	defer span.End()

	if err := req.Validate(); err != nil {
		return nil, err
	}

	adminSecret := s.cfg.AdminSecretKey
	if adminSecret == "" {
		adminSecret = s.cfg.AdminKey
	}

	role := model.RoleUser
	if req.Role == model.RoleAdmin && adminSecret != "" && req.AdminKey == adminSecret {
		role = model.RoleAdmin
	}

	hashCtx, hashSpan := s.tracer.Start(ctx, "auth.hash_password")
	cost := s.cfg.BcryptCost
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		cost = 12
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), cost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}
	hashSpan.End()

	user, err := s.repo.CreateUser(hashCtx, model.CreateUserInput{
		Username:     req.Username,
		Email:        req.Email,
		PasswordHash: string(hash),
		Role:         role,
	})
	if err != nil {
		return nil, err
	}

	return &model.RegisterResponse{
		ID:        user.ID,
		Username:  user.Username,
		Email:     user.Email,
		Role:      user.Role,
		CreatedAt: user.CreatedAt,
	}, nil
}

func (s *Service) Login(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error) {
	ctx, span := s.tracer.Start(ctx, "auth.login")
	defer span.End()

	if err := req.Validate(); err != nil {
		s.metrics.LoginAttemptsTotal.WithLabelValues("failure").Inc()
		return nil, err
	}

	user, err := s.repo.GetUserByUsername(ctx, req.Username)
	if err != nil {
		s.metrics.LoginAttemptsTotal.WithLabelValues("failure").Inc()
		if errors.Is(err, repository.ErrUserNotFound) {
			return nil, model.ErrInvalidCredentials
		}
		return nil, err
	}

	cmpCtx, cmpSpan := s.tracer.Start(ctx, "auth.verify_password")
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		s.metrics.LoginAttemptsTotal.WithLabelValues("failure").Inc()
		return nil, model.ErrInvalidCredentials
	}
	cmpSpan.End()

	ttl := s.cfg.JWTExpiry
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	s.logger.DebugContext(cmpCtx, "jwt created with duration", "ttl", ttl)
	token, err := jwt.GenerateToken(user.ID, user.Username, user.Role, s.cfg.JWTSecret, ttl)
	if err != nil {
		s.metrics.LoginAttemptsTotal.WithLabelValues("failure").Inc()
		return nil, fmt.Errorf("generate token: %w", err)
	}

	s.metrics.LoginAttemptsTotal.WithLabelValues("success").Inc()

	expiresAt := time.Now().UTC().Add(ttl)

	return &model.LoginResponse{
		Token:     token,
		ExpiresAt: expiresAt,
		ID:        user.ID,
		Username:  user.Username,
		Email:     user.Email,
		Role:      user.Role,
	}, nil
}

func (s *Service) CreateCredential(ctx context.Context, userID uuid.UUID, label string) (*model.S3Credential, error) {
	ctx, span := s.tracer.Start(ctx, "auth.create_credential")
	defer span.End()

	if userID == uuid.Nil {
		if ctxUserID, ok := authctx.UserID(ctx); ok && ctxUserID != uuid.Nil {
			userID = ctxUserID
		} else {
			return nil, model.ErrInvalidUserID
		}
	}
	if len(label) > 64 {
		return nil, model.ErrInvalidLabel
	}

	if _, err := s.repo.GetUserByID(ctx, userID); err != nil {
		return nil, err
	}

	accessKey, secretKey, err := model.GenerateCredentials()
	if err != nil {
		return nil, fmt.Errorf("generate credentials: %w", err)
	}

	input := model.CreateCredentialInput{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		UserID:          userID,
		Label:           label,
		Status:          model.StatusActive,
	}

	cred, err := s.repo.CreateCredential(ctx, input)
	if err != nil {
		return nil, err
	}

	s.metrics.KeysCreatedTotal.Inc()

	return cred, nil
}

func (s *Service) ListCredentials(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	ctx, span := s.tracer.Start(ctx, "auth.list_credentials")
	defer span.End()

	if userID == uuid.Nil {
		if ctxUserID, ok := authctx.UserID(ctx); ok && ctxUserID != uuid.Nil {
			userID = ctxUserID
		} else {
			return nil, model.ErrInvalidUserID
		}
	}

	creds, err := s.repo.ListCredentialsByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}

	for i := range creds {
		creds[i].SecretAccessKey = ""
	}

	return creds, nil
}

func (s *Service) RevokeCredential(ctx context.Context, userID uuid.UUID, accessKeyID string) error {
	ctx, span := s.tracer.Start(ctx, "auth.revoke_credential")
	defer span.End()

	if userID == uuid.Nil {
		if ctxUserID, ok := authctx.UserID(ctx); ok && ctxUserID != uuid.Nil {
			userID = ctxUserID
		} else {
			return model.ErrInvalidUserID
		}
	}
	if accessKeyID == "" {
		return model.ErrInvalidAccessKeyID
	}

	cred, err := s.repo.GetCredentialByAccessKey(ctx, accessKeyID)
	if err != nil {
		return err
	}

	if cred.UserID != userID {
		return repository.ErrCredentialNotFound
	}

	if err := s.repo.UpdateCredentialStatus(ctx, accessKeyID, model.StatusRevoked); err != nil {
		return err
	}

	s.metrics.KeysRevokedTotal.Inc()

	return nil
}

func (s *Service) ValidateAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	start := time.Now()
	ctx, span := s.tracer.Start(ctx, "auth.validate_access_key")
	defer span.End()

	defer func() {
		s.metrics.KeyValidationLatency.Observe(time.Since(start).Seconds())
	}()

	if accessKeyID == "" {
		s.metrics.KeyValidationsTotal.WithLabelValues("invalid").Inc()
		return nil, model.ErrInvalidAccessKeyID
	}

	cred, err := s.repo.GetCredentialByAccessKey(ctx, accessKeyID)
	if err != nil {
		s.metrics.KeyValidationsTotal.WithLabelValues("invalid").Inc()
		return nil, err
	}

	if cred.Status != model.StatusActive {
		s.metrics.KeyValidationsTotal.WithLabelValues("revoked").Inc()
		return nil, model.ErrCredentialRevoked
	}

	s.metrics.KeyValidationsTotal.WithLabelValues("valid").Inc()

	return cred, nil
}
