package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/model"
)

var (
	ErrUserNotFound           = errors.New("user not found")
	ErrUserAlreadyExists       = errors.New("user already exists")
	ErrCredentialNotFound     = errors.New("credential not found")
	ErrCredentialAlreadyExists = errors.New("credential already exists")
	ErrNotImplemented         = errors.New("not implemented")
)

type UserRepository interface {
	CreateUser(ctx context.Context, input model.CreateUserInput) (*model.User, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (*model.User, error)
	GetUserByUsername(ctx context.Context, username string) (*model.User, error)
	GetUserByEmail(ctx context.Context, email string) (*model.User, error)
}

type CredentialRepository interface {
	CreateCredential(ctx context.Context, input model.CreateCredentialInput) (*model.S3Credential, error)
	GetCredentialByAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error)
	ListCredentialsByUserID(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error)
	UpdateCredentialStatus(ctx context.Context, accessKeyID string, status model.CredentialStatus) error
	DeleteCredential(ctx context.Context, accessKeyID string) error
}

type Repository interface {
	UserRepository
	CredentialRepository
	Ping(ctx context.Context) error
	Close()
}
