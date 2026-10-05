package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/httperr"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
)

type mockAuthService struct {
	registerFunc func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
	loginFunc    func(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error)
}

func (m *mockAuthService) Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
	if m.registerFunc != nil {
		return m.registerFunc(ctx, req)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockAuthService) Login(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error) {
	if m.loginFunc != nil {
		return m.loginFunc(ctx, req)
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
			endpoint: "/api/v1/register",
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
			name:           "invalid json body",
			endpoint:       "/api/v1/register",
			requestBody:    "invalid-json",
			serviceFunc:    nil,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:     "validation error from service",
			endpoint: "/api/v1/register",
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
			endpoint: "/api/v1/register",
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
			endpoint: "/api/v1/register",
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

func TestAuthHandler_Login(t *testing.T) {
	tests := []struct {
		name           string
		endpoint       string
		requestBody    any
		serviceFunc    func(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error)
		expectedStatus int
	}{
		{
			name:     "successful login",
			endpoint: "/api/v1/login",
			requestBody: model.LoginRequest{
				Username: "testuser",
				Password: "password123",
			},
			serviceFunc: func(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error) {
				return &model.LoginResponse{
					Token:     "mock-jwt-token",
					ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
					ID:        uuid.New(),
					Username:  req.Username,
					Email:     "test@example.com",
					Role:      model.RoleUser,
				}, nil
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "invalid json body",
			endpoint:       "/api/v1/login",
			requestBody:    "malformed-json",
			serviceFunc:    nil,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:     "invalid credentials error",
			endpoint: "/api/v1/login",
			requestBody: model.LoginRequest{
				Username: "testuser",
				Password: "wrongpassword",
			},
			serviceFunc: func(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error) {
				return nil, model.ErrInvalidCredentials
			},
			expectedStatus: http.StatusUnauthorized,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New(fiber.Config{
				ErrorHandler: httperr.ErrorHandler,
			})
			h := NewAuthHandler(&mockAuthService{loginFunc: tc.serviceFunc})
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

			if tc.expectedStatus == http.StatusOK {
				cookies := resp.Cookies()
				foundCookie := false
				for _, c := range cookies {
					if c.Name == "jwt_token" && c.Value == "mock-jwt-token" {
						foundCookie = true
						break
					}
				}
				if !foundCookie {
					t.Error("expected jwt_token cookie to be set in response")
				}
			}
		})
	}
}

func TestAuthHandler_Dashboard(t *testing.T) {
	jwtSecret := "test-dashboard-jwt-secret-32-chars!"
	userID := uuid.New()
	validToken, err := jwt.GenerateToken(userID, "alice", model.RoleUser, jwtSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	app := fiber.New(fiber.Config{
		ErrorHandler: httperr.ErrorHandler,
	})
	h := NewAuthHandler(&mockAuthService{})
	h.RegisterRoutes(app, jwtSecret)

	// 1. Unauthenticated request to /api/v1/dashboard -> 401 Unauthorized
	reqUnauth := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	respUnauth, err := app.Test(reqUnauth)
	if err != nil {
		t.Fatalf("unauthenticated request failed: %v", err)
	}
	if respUnauth.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauthenticated request, got %d", respUnauth.StatusCode)
	}

	// 2. Authenticated request with Bearer token to /api/v1/dashboard -> 200 OK
	reqAuth := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	reqAuth.Header.Set("Authorization", "Bearer "+validToken)
	respAuth, err := app.Test(reqAuth)
	if err != nil {
		t.Fatalf("authenticated request failed: %v", err)
	}
	if respAuth.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for authenticated request, got %d", respAuth.StatusCode)
	}

	body, _ := io.ReadAll(respAuth.Body)
	var payload struct {
		Message string `json:"message"`
		User    struct {
			ID       string `json:"id"`
			Username string `json:"username"`
			Role     string `json:"role"`
		} `json:"user"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to parse dashboard response: %v", err)
	}
	if payload.User.Username != "alice" || payload.User.ID != userID.String() {
		t.Errorf("unexpected user in payload: %+v", payload)
	}
}
