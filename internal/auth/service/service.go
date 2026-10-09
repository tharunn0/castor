package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"github.com/tharunn0/castor/internal/auth/authctx"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
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
	cfg      config.Config
	userRepo repository.UserRepository
	credRepo repository.CredentialRepository
}

func NewService(cfg config.Config, userRepo repository.UserRepository, credRepos ...repository.CredentialRepository) *Service {
	var credRepo repository.CredentialRepository
	if len(credRepos) > 0 {
		credRepo = credRepos[0]
	} else if cr, ok := userRepo.(repository.CredentialRepository); ok {
		credRepo = cr
	}
	return &Service{
		cfg:      cfg,
		userRepo: userRepo,
		credRepo: credRepo,
	}
}

func (s *Service) Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
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

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	user, err := s.userRepo.CreateUser(ctx, model.CreateUserInput{
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
	if err := req.Validate(); err != nil {
		return nil, err
	}

	user, err := s.userRepo.GetUserByUsername(ctx, req.Username)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return nil, model.ErrInvalidCredentials
		}
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, model.ErrInvalidCredentials
	}

	ttl := s.cfg.JWTExpiry
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	token, err := jwt.GenerateToken(user.ID, user.Username, user.Role, s.cfg.JWTSecret, ttl)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

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
	if s.credRepo == nil {
		return nil, repository.ErrNotImplemented
	}
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

	if _, err := s.userRepo.GetUserByID(ctx, userID); err != nil {
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

	return s.credRepo.CreateCredential(ctx, input)
}

func (s *Service) ListCredentials(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	if s.credRepo == nil {
		return nil, repository.ErrNotImplemented
	}
	if userID == uuid.Nil {
		if ctxUserID, ok := authctx.UserID(ctx); ok && ctxUserID != uuid.Nil {
			userID = ctxUserID
		} else {
			return nil, model.ErrInvalidUserID
		}
	}

	creds, err := s.credRepo.ListCredentialsByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}

	for i := range creds {
		creds[i].SecretAccessKey = ""
	}

	return creds, nil
}

func (s *Service) RevokeCredential(ctx context.Context, userID uuid.UUID, accessKeyID string) error {
	if s.credRepo == nil {
		return repository.ErrNotImplemented
	}
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

	cred, err := s.credRepo.GetCredentialByAccessKey(ctx, accessKeyID)
	if err != nil {
		return err
	}

	if cred.UserID != userID {
		return repository.ErrCredentialNotFound
	}

	return s.credRepo.UpdateCredentialStatus(ctx, accessKeyID, model.StatusRevoked)
}

func (s *Service) ValidateAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	if s.credRepo == nil {
		return nil, repository.ErrNotImplemented
	}
	if accessKeyID == "" {
		return nil, model.ErrInvalidAccessKeyID
	}

	cred, err := s.credRepo.GetCredentialByAccessKey(ctx, accessKeyID)
	if err != nil {
		return nil, err
	}

	if cred.Status != model.StatusActive {
		return nil, model.ErrCredentialRevoked
	}

	return cred, nil
}
