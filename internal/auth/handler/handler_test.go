package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/httperr"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
)

type mockAuthService struct {
	registerFunc func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
}

func (m *mockAuthService) Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
	if m.registerFunc != nil {
		return m.registerFunc(ctx, req)
	}
	return nil, repository.ErrNotImplemented
}

func TestAuthHandler_Register(t *testing.T) {
	tests := []struct {
		name           string
		endpoint       string
		requestBody    any
		serviceFunc    func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
		expectedStatus int
	}{
		{
			name:     "successful registration",
			endpoint: "/register",
			requestBody: model.RegisterRequest{
				Username: "newuser",
				Email:    "new@example.com",
				Password: "password123",
			},
			serviceFunc: func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
				return &model.RegisterResponse{
					ID:        uuid.New(),
					Username:  req.Username,
					Email:     req.Email,
					Role:      model.RoleUser,
					CreatedAt: time.Now().UTC(),
				}, nil
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name:     "successful registration on api prefix route",
			endpoint: "/api/auth/register",
			requestBody: model.RegisterRequest{
				Username: "adminuser",
				Email:    "admin@example.com",
				Password: "password123",
				AdminKey: "secret",
			},
			serviceFunc: func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
				return &model.RegisterResponse{
					ID:        uuid.New(),
					Username:  req.Username,
					Email:     req.Email,
					Role:      model.RoleAdmin,
					CreatedAt: time.Now().UTC(),
				}, nil
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "invalid json body",
			endpoint:       "/register",
			requestBody:    "invalid-json",
			serviceFunc:    nil,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:     "validation error from service",
			endpoint: "/register",
			requestBody: model.RegisterRequest{
				Username: "ab",
				Email:    "invalid-email",
				Password: "123",
			},
			serviceFunc: func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
				return nil, model.ErrInvalidUsername
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:     "user already exists error",
			endpoint: "/register",
			requestBody: model.RegisterRequest{
				Username: "existing",
				Email:    "existing@example.com",
				Password: "password123",
			},
			serviceFunc: func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
				return nil, repository.ErrUserAlreadyExists
			},
			expectedStatus: http.StatusConflict,
		},
		{
			name:     "unexpected internal server error",
			endpoint: "/register",
			requestBody: model.RegisterRequest{
				Username: "user",
				Email:    "user@example.com",
				Password: "password123",
			},
			serviceFunc: func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
				return nil, errors.New("db disk full")
			},
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New(fiber.Config{
				ErrorHandler: httperr.ErrorHandler,
			})
			h := NewAuthHandler(&mockAuthService{registerFunc: tc.serviceFunc})
			h.RegisterRoutes(app)

			var bodyBytes []byte
			if s, ok := tc.requestBody.(string); ok {
				bodyBytes = []byte(s)
			} else {
				bodyBytes, _ = json.Marshal(tc.requestBody)
			}

			req := httptest.NewRequest(http.MethodPost, tc.endpoint, bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}

			if resp.StatusCode != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d", tc.expectedStatus, resp.StatusCode)
			}
		})
	}
}
