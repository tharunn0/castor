package model

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRole_Validate(t *testing.T) {
	tests := []struct {
		name    string
		role    Role
		wantErr error
	}{
		{name: "valid admin", role: RoleAdmin, wantErr: nil},
		{name: "valid user", role: RoleUser, wantErr: nil},
		{name: "empty role", role: "", wantErr: ErrInvalidRole},
		{name: "unknown role", role: "SUPERADMIN", wantErr: ErrInvalidRole},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.role.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestCreateUserInput_Validate(t *testing.T) {
	tests := []struct {
		name    string
		input   CreateUserInput
		wantErr error
	}{
		{
			name: "valid input",
			input: CreateUserInput{
				Username:     "alice_123",
				Email:        "alice@example.com",
				PasswordHash: "$2a$12$e8YkZ8Y...",
				Role:         RoleUser,
			},
			wantErr: nil,
		},
		{
			name: "username too short",
			input: CreateUserInput{
				Username:     "al",
				Email:        "alice@example.com",
				PasswordHash: "hash123",
				Role:         RoleUser,
			},
			wantErr: ErrInvalidUsername,
		},
		{
			name: "username too long",
			input: CreateUserInput{
				Username:     strings.Repeat("a", 65),
				Email:        "alice@example.com",
				PasswordHash: "hash123",
				Role:         RoleUser,
			},
			wantErr: ErrInvalidUsername,
		},
		{
			name: "username with illegal character",
			input: CreateUserInput{
				Username:     "alice@work",
				Email:        "alice@example.com",
				PasswordHash: "hash123",
				Role:         RoleUser,
			},
			wantErr: ErrInvalidUsername,
		},
		{
			name: "empty email",
			input: CreateUserInput{
				Username:     "alice",
				Email:        "",
				PasswordHash: "hash123",
				Role:         RoleUser,
			},
			wantErr: ErrInvalidEmail,
		},
		{
			name: "invalid email format",
			input: CreateUserInput{
				Username:     "alice",
				Email:        "alice_at_example.com",
				PasswordHash: "hash123",
				Role:         RoleUser,
			},
			wantErr: ErrInvalidEmail,
		},
		{
			name: "email too long",
			input: CreateUserInput{
				Username:     "alice",
				Email:        strings.Repeat("a", 250) + "@b.com",
				PasswordHash: "hash123",
				Role:         RoleUser,
			},
			wantErr: ErrInvalidEmail,
		},
		{
			name: "empty password hash",
			input: CreateUserInput{
				Username:     "alice",
				Email:        "alice@example.com",
				PasswordHash: "",
				Role:         RoleUser,
			},
			wantErr: ErrInvalidPasswordHash,
		},
		{
			name: "password hash too long",
			input: CreateUserInput{
				Username:     "alice",
				Email:        "alice@example.com",
				PasswordHash: strings.Repeat("h", 256),
				Role:         RoleUser,
			},
			wantErr: ErrInvalidPasswordHash,
		},
		{
			name: "invalid role",
			input: CreateUserInput{
				Username:     "alice",
				Email:        "alice@example.com",
				PasswordHash: "hash123",
				Role:         "GUEST",
			},
			wantErr: ErrInvalidRole,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.input.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestUser_Validate(t *testing.T) {
	validUser := User{
		ID:           uuid.New(),
		Username:     "bob_admin",
		Email:        "bob@example.com",
		PasswordHash: "hashed_secret",
		Role:         RoleAdmin,
		CreatedAt:    time.Now().UTC(),
	}

	tests := []struct {
		name    string
		modify  func(u *User)
		wantErr error
	}{
		{
			name:    "valid user",
			modify:  func(u *User) {},
			wantErr: nil,
		},
		{
			name: "nil user id",
			modify: func(u *User) {
				u.ID = uuid.Nil
			},
			wantErr: ErrInvalidUserID,
		},
		{
			name: "invalid username",
			modify: func(u *User) {
				u.Username = "no"
			},
			wantErr: ErrInvalidUsername,
		},
		{
			name: "invalid email",
			modify: func(u *User) {
				u.Email = "bad-email"
			},
			wantErr: ErrInvalidEmail,
		},
		{
			name: "invalid password hash",
			modify: func(u *User) {
				u.PasswordHash = ""
			},
			wantErr: ErrInvalidPasswordHash,
		},
		{
			name: "invalid role",
			modify: func(u *User) {
				u.Role = "INVALID"
			},
			wantErr: ErrInvalidRole,
		},
		{
			name: "zero created_at",
			modify: func(u *User) {
				u.CreatedAt = time.Time{}
			},
			wantErr: ErrInvalidCreatedAt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u := validUser
			tc.modify(&u)
			err := u.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestRegisterRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     RegisterRequest
		wantErr error
	}{
		{
			name: "valid request",
			req: RegisterRequest{
				Username: "charlie",
				Email:    "charlie@example.com",
				Password: "password123",
				Role:     RoleUser,
				AdminKey: "",
			},
			wantErr: nil,
		},
		{
			name: "valid request with empty role",
			req: RegisterRequest{
				Username: "charlie",
				Email:    "charlie@example.com",
				Password: "password123",
			},
			wantErr: nil,
		},
		{
			name: "invalid username",
			req: RegisterRequest{
				Username: "c",
				Email:    "charlie@example.com",
				Password: "password123",
			},
			wantErr: ErrInvalidUsername,
		},
		{
			name: "invalid email",
			req: RegisterRequest{
				Username: "charlie",
				Email:    "invalid-email",
				Password: "password123",
			},
			wantErr: ErrInvalidEmail,
		},
		{
			name: "password too short",
			req: RegisterRequest{
				Username: "charlie",
				Email:    "charlie@example.com",
				Password: "123",
			},
			wantErr: ErrInvalidPassword,
		},
		{
			name: "password too long",
			req: RegisterRequest{
				Username: "charlie",
				Email:    "charlie@example.com",
				Password: strings.Repeat("p", 129),
			},
			wantErr: ErrInvalidPassword,
		},
		{
			name: "invalid role",
			req: RegisterRequest{
				Username: "charlie",
				Email:    "charlie@example.com",
				Password: "password123",
				Role:     "NOT_A_ROLE",
			},
			wantErr: ErrInvalidRole,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

