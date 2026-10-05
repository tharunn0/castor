package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
)

type AuthService interface {
	Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
	Login(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error)
}

type Service struct {
	cfg  config.Config
	repo repository.UserRepository
}

func NewService(cfg config.Config, repo repository.UserRepository) *Service {
	return &Service{
		cfg:  cfg,
		repo: repo,
	}
}

func (s *Service) Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	role := model.RoleUser
	if req.AdminKey != "" && s.cfg.AdminKey != "" && req.AdminKey == s.cfg.AdminKey {
		role = model.RoleAdmin
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	user, err := s.repo.CreateUser(ctx, model.CreateUserInput{
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

	user, err := s.repo.GetUserByUsername(ctx, req.Username)
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
