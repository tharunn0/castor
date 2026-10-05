package model

import "errors"

var (
	ErrInvalidUserID           = errors.New("invalid user id: cannot be empty or nil")
	ErrInvalidUsername         = errors.New("invalid username: must be 3-64 characters and contain only letters, numbers, underscores, dashes, or dots")
	ErrInvalidEmail            = errors.New("invalid email: must be a valid email address of at most 255 characters")
	ErrInvalidPasswordHash     = errors.New("invalid password hash: cannot be empty or exceed 255 characters")
	ErrInvalidRole             = errors.New("invalid role: must be ADMIN or USER")
	ErrInvalidCreatedAt        = errors.New("invalid created_at: timestamp cannot be zero")
	ErrInvalidAccessKeyID      = errors.New("invalid access key id: must be 3-32 alphanumeric characters")
	ErrInvalidSecretAccessKey  = errors.New("invalid secret access key: must be 1-64 characters")
	ErrInvalidLabel            = errors.New("invalid label: cannot exceed 64 characters")
	ErrInvalidCredentialStatus = errors.New("invalid credential status: must be ACTIVE or REVOKED")
)
