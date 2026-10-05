package service

import (
	"context"
	"fmt"

	"golang.org/x/crypto/bcrypt"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
)

type AuthService interface {
	Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
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
