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
	if r.pool == nil {
		return nil, ErrNotImplemented
	}

	query := `
		SELECT id, username, email, password_hash, role, created_at
		FROM users
		WHERE id = $1
	`
	var user model.User
	var roleStr string
	err := r.pool.QueryRow(ctx, query, id).Scan(
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
		return nil, fmt.Errorf("get user by id: %w", err)
	}
	user.Role = model.Role(roleStr)
	return &user, nil
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
	if r.pool == nil {
		return nil, ErrNotImplemented
	}

	status := input.Status
	if status == "" {
		status = model.StatusActive
	}
	createdAt := time.Now().UTC()

	query := `
		INSERT INTO s3_credentials (access_key_id, secret_access_key, user_id, label, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING access_key_id, secret_access_key, user_id, label, status, created_at
	`
	var cred model.S3Credential
	var statusStr string
	var scannedLabel *string
	var label *string
	if input.Label != "" {
		label = &input.Label
	}

	err := r.pool.QueryRow(ctx, query, input.AccessKeyID, input.SecretAccessKey, input.UserID, label, string(status), createdAt).Scan(
		&cred.AccessKeyID,
		&cred.SecretAccessKey,
		&cred.UserID,
		&scannedLabel,
		&statusStr,
		&cred.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "23505":
				return nil, ErrCredentialAlreadyExists
			case "23503":
				return nil, ErrUserNotFound
			}
		}
		return nil, fmt.Errorf("insert credential: %w", err)
	}
	if scannedLabel != nil {
		cred.Label = *scannedLabel
	}
	cred.Status = model.CredentialStatus(statusStr)
	return &cred, nil
}

func (r *PostgresRepository) GetCredentialByAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	if r.pool == nil {
		return nil, ErrNotImplemented
	}

	query := `
		SELECT access_key_id, secret_access_key, user_id, label, status, created_at
		FROM s3_credentials
		WHERE access_key_id = $1
	`
	var cred model.S3Credential
	var statusStr string
	var scannedLabel *string
	err := r.pool.QueryRow(ctx, query, accessKeyID).Scan(
		&cred.AccessKeyID,
		&cred.SecretAccessKey,
		&cred.UserID,
		&scannedLabel,
		&statusStr,
		&cred.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrCredentialNotFound
		}
		return nil, fmt.Errorf("get credential by access key: %w", err)
	}
	if scannedLabel != nil {
		cred.Label = *scannedLabel
	}
	cred.Status = model.CredentialStatus(statusStr)
	return &cred, nil
}

func (r *PostgresRepository) ListCredentialsByUserID(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	if r.pool == nil {
		return nil, ErrNotImplemented
	}

	query := `
		SELECT access_key_id, secret_access_key, user_id, label, status, created_at
		FROM s3_credentials
		WHERE user_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.pool.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	defer rows.Close()

	creds := make([]*model.S3Credential, 0)
	for rows.Next() {
		var cred model.S3Credential
		var statusStr string
		var scannedLabel *string
		if err := rows.Scan(
			&cred.AccessKeyID,
			&cred.SecretAccessKey,
			&cred.UserID,
			&scannedLabel,
			&statusStr,
			&cred.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan credential: %w", err)
		}
		if scannedLabel != nil {
			cred.Label = *scannedLabel
		}
		cred.Status = model.CredentialStatus(statusStr)
		creds = append(creds, &cred)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}

	return creds, nil
}

func (r *PostgresRepository) UpdateCredentialStatus(ctx context.Context, accessKeyID string, status model.CredentialStatus) error {
	if r.pool == nil {
		return ErrNotImplemented
	}

	query := `
		UPDATE s3_credentials
		SET status = $1
		WHERE access_key_id = $2
	`
	tag, err := r.pool.Exec(ctx, query, string(status), accessKeyID)
	if err != nil {
		return fmt.Errorf("update credential status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrCredentialNotFound
	}
	return nil
}

func (r *PostgresRepository) DeleteCredential(ctx context.Context, accessKeyID string) error {
	if r.pool == nil {
		return ErrNotImplemented
	}

	query := `
		DELETE FROM s3_credentials
		WHERE access_key_id = $1
	`
	tag, err := r.pool.Exec(ctx, query, accessKeyID)
	if err != nil {
		return fmt.Errorf("delete credential: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrCredentialNotFound
	}
	return nil
}

var _ Repository = (*PostgresRepository)(nil)
