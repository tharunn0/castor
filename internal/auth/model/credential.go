package model

import (
	"regexp"
	"time"

	"github.com/google/uuid"
)

var validAccessKeyRegex = regexp.MustCompile(`^[a-zA-Z0-9]{3,32}$`)

type CredentialStatus string

const (
	StatusActive  CredentialStatus = "ACTIVE"
	StatusRevoked CredentialStatus = "REVOKED"
)

func (s CredentialStatus) Validate() error {
	switch s {
	case StatusActive, StatusRevoked:
		return nil
	default:
		return ErrInvalidCredentialStatus
	}
}

type S3Credential struct {
	AccessKeyID     string           `json:"access_key_id"`
	SecretAccessKey string           `json:"secret_access_key,omitempty"`
	UserID          uuid.UUID        `json:"user_id"`
	Label           string           `json:"label,omitempty"`
	Status          CredentialStatus `json:"status"`
	CreatedAt       time.Time        `json:"created_at"`
}

func (c *S3Credential) Validate() error {
	if err := validateAccessKeyID(c.AccessKeyID); err != nil {
		return err
	}
	if err := validateSecretAccessKey(c.SecretAccessKey); err != nil {
		return err
	}
	if c.UserID == uuid.Nil {
		return ErrInvalidUserID
	}
	if err := validateLabel(c.Label); err != nil {
		return err
	}
	if err := c.Status.Validate(); err != nil {
		return err
	}
	if c.CreatedAt.IsZero() {
		return ErrInvalidCreatedAt
	}
	return nil
}

type CreateCredentialInput struct {
	AccessKeyID     string
	SecretAccessKey string
	UserID          uuid.UUID
	Label           string
	Status          CredentialStatus
}

func (in *CreateCredentialInput) Validate() error {
	if err := validateAccessKeyID(in.AccessKeyID); err != nil {
		return err
	}
	if err := validateSecretAccessKey(in.SecretAccessKey); err != nil {
		return err
	}
	if in.UserID == uuid.Nil {
		return ErrInvalidUserID
	}
	if err := validateLabel(in.Label); err != nil {
		return err
	}
	if err := in.Status.Validate(); err != nil {
		return err
	}
	return nil
}

func validateAccessKeyID(key string) error {
	if !validAccessKeyRegex.MatchString(key) {
		return ErrInvalidAccessKeyID
	}
	return nil
}

func validateSecretAccessKey(key string) error {
	if len(key) < 1 || len(key) > 64 {
		return ErrInvalidSecretAccessKey
	}
	return nil
}

func validateLabel(label string) error {
	if len(label) > 64 {
		return ErrInvalidLabel
	}
	return nil
}
