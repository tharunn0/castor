package repository

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tharunn0/castor/internal/auth/model"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Ping(ctx context.Context) error {
	if r.pool == nil {
		return ErrNotImplemented
	}
	return r.pool.Ping(ctx)
}

func (r *PostgresRepository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

func (r *PostgresRepository) CreateUser(ctx context.Context, input model.CreateUserInput) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) GetUserByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) CreateCredential(ctx context.Context, input model.CreateCredentialInput) (*model.S3Credential, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) GetCredentialByAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) ListCredentialsByUserID(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) UpdateCredentialStatus(ctx context.Context, accessKeyID string, status model.CredentialStatus) error {
	return ErrNotImplemented
}

func (r *PostgresRepository) DeleteCredential(ctx context.Context, accessKeyID string) error {
	return ErrNotImplemented
}

var _ Repository = (*PostgresRepository)(nil)
