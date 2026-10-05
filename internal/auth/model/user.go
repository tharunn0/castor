package model

import (
	"net/mail"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var validUsernameRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-\.]{3,64}$`)

type Role string

const (
	RoleAdmin Role = "ADMIN"
	RoleUser  Role = "USER"
)

func (r Role) Validate() error {
	switch r {
	case RoleAdmin, RoleUser:
		return nil
	default:
		return ErrInvalidRole
	}
}

type User struct {
	ID           uuid.UUID `json:"id"`
	Username     string    `json:"username"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	Role         Role      `json:"role"`
	CreatedAt    time.Time `json:"created_at"`
}

func (u *User) Validate() error {
	if u.ID == uuid.Nil {
		return ErrInvalidUserID
	}
	if err := validateUsername(u.Username); err != nil {
		return err
	}
	if err := validateEmail(u.Email); err != nil {
		return err
	}
	if err := validatePasswordHash(u.PasswordHash); err != nil {
		return err
	}
	if err := u.Role.Validate(); err != nil {
		return err
	}
	if u.CreatedAt.IsZero() {
		return ErrInvalidCreatedAt
	}
	return nil
}

type CreateUserInput struct {
	Username     string
	Email        string
	PasswordHash string
	Role         Role
}

func (in *CreateUserInput) Validate() error {
	if err := validateUsername(in.Username); err != nil {
		return err
	}
	if err := validateEmail(in.Email); err != nil {
		return err
	}
	if err := validatePasswordHash(in.PasswordHash); err != nil {
		return err
	}
	if err := in.Role.Validate(); err != nil {
		return err
	}
	return nil
}

type RegisterRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     Role   `json:"role,omitempty"`
	AdminKey string `json:"admin_key,omitempty"`
}

func (r *RegisterRequest) Validate() error {
	if err := validateUsername(r.Username); err != nil {
		return err
	}
	if err := validateEmail(r.Email); err != nil {
		return err
	}
	if len(r.Password) < 6 || len(r.Password) > 128 {
		return ErrInvalidPassword
	}
	if r.Role != "" {
		if err := r.Role.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type RegisterResponse struct {
	ID        uuid.UUID `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Role      Role      `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

func validateUsername(username string) error {
	if !validUsernameRegex.MatchString(username) {
		return ErrInvalidUsername
	}
	return nil
}

func validateEmail(email string) error {
	if len(email) == 0 || len(email) > 255 {
		return ErrInvalidEmail
	}
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email {
		return ErrInvalidEmail
	}
	return nil
}

func validatePasswordHash(hash string) error {
	if len(hash) == 0 || len(hash) > 255 {
		return ErrInvalidPasswordHash
	}
	return nil
}
