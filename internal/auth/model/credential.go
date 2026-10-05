package model

import (
	"time"

	"github.com/google/uuid"
)

type CredentialStatus string

const (
	StatusActive  CredentialStatus = "ACTIVE"
	StatusRevoked CredentialStatus = "REVOKED"
)

type S3Credential struct {
	AccessKeyID     string           `json:"access_key_id"`
	SecretAccessKey string           `json:"secret_access_key,omitempty"`
	UserID          uuid.UUID        `json:"user_id"`
	Label           string           `json:"label,omitempty"`
	Status          CredentialStatus `json:"status"`
	CreatedAt       time.Time        `json:"created_at"`
}

type CreateCredentialInput struct {
	AccessKeyID     string
	SecretAccessKey string
	UserID          uuid.UUID
	Label           string
	Status          CredentialStatus
}
