package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	if r.pool == nil {
		return nil, ErrNotImplemented
	}

	query := `
		INSERT INTO users (id, username, email, password_hash, role, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, username, email, password_hash, role, created_at
	`
	id := uuid.New()
	createdAt := time.Now().UTC()

	var user model.User
	err := r.pool.QueryRow(ctx, query, id, input.Username, input.Email, input.PasswordHash, string(input.Role), createdAt).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.PasswordHash,
		&user.Role,
		&user.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, ErrUserAlreadyExists
		}
		return nil, fmt.Errorf("insert user: %w", err)
	}

	return &user, nil
}

func (r *PostgresRepository) GetUserByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (r *PostgresRepository) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	if r.pool == nil {
		return nil, ErrNotImplemented
	}

	query := `
		SELECT id, username, email, password_hash, role, created_at
		FROM users
		WHERE username = $1
	`
	var user model.User
	var roleStr string
	err := r.pool.QueryRow(ctx, query, username).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.PasswordHash,
		&roleStr,
		&user.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("get user by username: %w", err)
	}
	user.Role = model.Role(roleStr)
	return &user, nil
}

func (r *PostgresRepository) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	if r.pool == nil {
		return nil, ErrNotImplemented
	}

	query := `
		SELECT id, username, email, password_hash, role, created_at
		FROM users
		WHERE email = $1
	`
	var user model.User
	var roleStr string
	err := r.pool.QueryRow(ctx, query, email).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.PasswordHash,
		&roleStr,
		&user.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("get user by email: %w", err)
	}
	user.Role = model.Role(roleStr)
	return &user, nil
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
