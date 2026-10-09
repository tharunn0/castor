package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	registerFunc         func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
	loginFunc            func(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error)
	createCredentialFunc func(ctx context.Context, userID uuid.UUID, label string) (*model.S3Credential, error)
	listCredentialsFunc  func(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error)
	revokeCredentialFunc func(ctx context.Context, userID uuid.UUID, accessKeyID string) error
	validateAccessKeyFunc func(ctx context.Context, accessKeyID string) (*model.S3Credential, error)
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

func (m *mockAuthService) CreateCredential(ctx context.Context, userID uuid.UUID, label string) (*model.S3Credential, error) {
	if m.createCredentialFunc != nil {
		return m.createCredentialFunc(ctx, userID, label)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockAuthService) ListCredentials(ctx context.Context, userID uuid.UUID) ([]*model.S3Credential, error) {
	if m.listCredentialsFunc != nil {
		return m.listCredentialsFunc(ctx, userID)
	}
	return nil, repository.ErrNotImplemented
}

func (m *mockAuthService) RevokeCredential(ctx context.Context, userID uuid.UUID, accessKeyID string) error {
	if m.revokeCredentialFunc != nil {
		return m.revokeCredentialFunc(ctx, userID, accessKeyID)
	}
	return repository.ErrNotImplemented
}

func (m *mockAuthService) ValidateAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	if m.validateAccessKeyFunc != nil {
		return m.validateAccessKeyFunc(ctx, accessKeyID)
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

func TestAuthHandler_Keys(t *testing.T) {
	jwtSecret := "test-keys-jwt-secret-12345"
	userID := uuid.New()
	validToken, err := jwt.GenerateToken(userID, "alice", model.RoleUser, jwtSecret, time.Hour)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	app := fiber.New(fiber.Config{
		ErrorHandler: httperr.ErrorHandler,
	})

	mockSvc := &mockAuthService{
		createCredentialFunc: func(ctx context.Context, uID uuid.UUID, label string) (*model.S3Credential, error) {
			if uID != userID {
				return nil, repository.ErrUserNotFound
			}
			return &model.S3Credential{
				AccessKeyID:     "AKIAKEY1234567890123",
				SecretAccessKey: "secret_1234567890abcdefghijklmnopqrst",
				UserID:          uID,
				Label:           label,
				Status:          model.StatusActive,
				CreatedAt:       time.Now().UTC(),
			}, nil
		},
		listCredentialsFunc: func(ctx context.Context, uID uuid.UUID) ([]*model.S3Credential, error) {
			return []*model.S3Credential{
				{
					AccessKeyID: "AKIAKEY1234567890123",
					UserID:      uID,
					Label:       "My Key",
					Status:      model.StatusActive,
					CreatedAt:   time.Now().UTC(),
				},
			}, nil
		},
		revokeCredentialFunc: func(ctx context.Context, uID uuid.UUID, keyID string) error {
			if keyID == "UNKNOWNKEY" {
				return repository.ErrCredentialNotFound
			}
			return nil
		},
	}

	h := NewAuthHandler(mockSvc)
	h.RegisterRoutes(app, jwtSecret)

	// POST /api/v1/keys - Unauthenticated -> 401
	reqPostUnauth := httptest.NewRequest(http.MethodPost, "/api/v1/keys", nil)
	respPostUnauth, err := app.Test(reqPostUnauth)
	if err != nil {
		t.Fatalf("unauthenticated post keys failed: %v", err)
	}
	if respPostUnauth.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauth post keys, got %d", respPostUnauth.StatusCode)
	}

	// POST /api/v1/keys - Authenticated -> 201 Created
	reqPostAuth := httptest.NewRequest(http.MethodPost, "/api/v1/keys", strings.NewReader(`{"label":"Laptop"}`))
	reqPostAuth.Header.Set("Content-Type", "application/json")
	reqPostAuth.Header.Set("Authorization", "Bearer "+validToken)
	respPostAuth, err := app.Test(reqPostAuth)
	if err != nil {
		t.Fatalf("auth post keys failed: %v", err)
	}
	if respPostAuth.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 for post keys, got %d", respPostAuth.StatusCode)
	}

	var createdCred model.S3Credential
	body, _ := io.ReadAll(respPostAuth.Body)
	if err := json.Unmarshal(body, &createdCred); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if createdCred.AccessKeyID != "AKIAKEY1234567890123" || createdCred.Label != "Laptop" {
		t.Errorf("unexpected created credential: %+v", createdCred)
	}

	// GET /api/v1/keys - Unauthenticated -> 401
	reqGetUnauth := httptest.NewRequest(http.MethodGet, "/api/v1/keys", nil)
	respGetUnauth, err := app.Test(reqGetUnauth)
	if err != nil {
		t.Fatalf("unauthenticated get keys failed: %v", err)
	}
	if respGetUnauth.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauth get keys, got %d", respGetUnauth.StatusCode)
	}

	// GET /api/v1/keys - Authenticated -> 200 OK
	reqGetAuth := httptest.NewRequest(http.MethodGet, "/api/v1/keys", nil)
	reqGetAuth.Header.Set("Authorization", "Bearer "+validToken)
	respGetAuth, err := app.Test(reqGetAuth)
	if err != nil {
		t.Fatalf("auth get keys failed: %v", err)
	}
	if respGetAuth.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for get keys, got %d", respGetAuth.StatusCode)
	}

	var credsList []*model.S3Credential
	listBody, _ := io.ReadAll(respGetAuth.Body)
	if err := json.Unmarshal(listBody, &credsList); err != nil {
		t.Fatalf("failed to decode keys list: %v", err)
	}
	if len(credsList) != 1 || credsList[0].AccessKeyID != "AKIAKEY1234567890123" {
		t.Errorf("unexpected keys list: %+v", credsList)
	}

	// DELETE /api/v1/keys/:key_id - Unauthenticated -> 401
	reqDelUnauth := httptest.NewRequest(http.MethodDelete, "/api/v1/keys/AKIAKEY1234567890123", nil)
	respDelUnauth, err := app.Test(reqDelUnauth)
	if err != nil {
		t.Fatalf("unauth delete key failed: %v", err)
	}
	if respDelUnauth.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauth delete key, got %d", respDelUnauth.StatusCode)
	}

	// DELETE /api/v1/keys/:key_id - Authenticated success -> 204 No Content
	reqDelAuth := httptest.NewRequest(http.MethodDelete, "/api/v1/keys/AKIAKEY1234567890123", nil)
	reqDelAuth.Header.Set("Authorization", "Bearer "+validToken)
	respDelAuth, err := app.Test(reqDelAuth)
	if err != nil {
		t.Fatalf("auth delete key failed: %v", err)
	}
	if respDelAuth.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204 for delete key, got %d", respDelAuth.StatusCode)
	}

	// DELETE /api/v1/keys/:key_id - Not found -> 404
	reqDelNotFound := httptest.NewRequest(http.MethodDelete, "/api/v1/keys/UNKNOWNKEY", nil)
	reqDelNotFound.Header.Set("Authorization", "Bearer "+validToken)
	respDelNotFound, err := app.Test(reqDelNotFound)
	if err != nil {
		t.Fatalf("auth delete key not found failed: %v", err)
	}
	if respDelNotFound.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for unknown key delete, got %d", respDelNotFound.StatusCode)
	}
}

func TestAuthHandler_ValidateKey(t *testing.T) {
	app := fiber.New(fiber.Config{
		ErrorHandler: httperr.ErrorHandler,
	})

	mockSvc := &mockAuthService{
		validateAccessKeyFunc: func(ctx context.Context, key string) (*model.S3Credential, error) {
			switch key {
			case "AKIAACTIVEKEY123":
				return &model.S3Credential{
					AccessKeyID:     key,
					SecretAccessKey: "secret_12345",
					Status:          model.StatusActive,
				}, nil
			case "AKIAREVOKEDKEY123":
				return nil, model.ErrCredentialRevoked
			default:
				return nil, repository.ErrCredentialNotFound
			}
		},
	}

	h := NewAuthHandler(mockSvc)
	h.RegisterRoutes(app)

	// 1. Missing key -> 400 Bad Request
	reqMissing := httptest.NewRequest(http.MethodGet, "/internal/v1/validate-key", nil)
	respMissing, err := app.Test(reqMissing)
	if err != nil {
		t.Fatalf("missing key request failed: %v", err)
	}
	if respMissing.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for missing access key, got %d", respMissing.StatusCode)
	}

	// 2. Active key via query parameter -> 200 OK
	reqActiveQuery := httptest.NewRequest(http.MethodGet, "/internal/v1/validate-key?access_key_id=AKIAACTIVEKEY123", nil)
	respActiveQuery, err := app.Test(reqActiveQuery)
	if err != nil {
		t.Fatalf("active key query request failed: %v", err)
	}
	if respActiveQuery.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for active key via query, got %d", respActiveQuery.StatusCode)
	}

	var cred model.S3Credential
	body, _ := io.ReadAll(respActiveQuery.Body)
	if err := json.Unmarshal(body, &cred); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if cred.AccessKeyID != "AKIAACTIVEKEY123" || cred.Status != model.StatusActive {
		t.Errorf("unexpected cred in response: %+v", cred)
	}

	// 3. Active key via X-Access-Key-ID header -> 200 OK
	reqActiveHeader := httptest.NewRequest(http.MethodGet, "/internal/v1/validate-key", nil)
	reqActiveHeader.Header.Set("X-Access-Key-ID", "AKIAACTIVEKEY123")
	respActiveHeader, err := app.Test(reqActiveHeader)
	if err != nil {
		t.Fatalf("active key header request failed: %v", err)
	}
	if respActiveHeader.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for active key via header, got %d", respActiveHeader.StatusCode)
	}

	// 4. Revoked key -> 401 Unauthorized
	reqRevoked := httptest.NewRequest(http.MethodGet, "/internal/v1/validate-key?access_key_id=AKIAREVOKEDKEY123", nil)
	respRevoked, err := app.Test(reqRevoked)
	if err != nil {
		t.Fatalf("revoked key request failed: %v", err)
	}
	if respRevoked.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for revoked key, got %d", respRevoked.StatusCode)
	}

	// 5. Unknown key -> 404 Not Found
	reqUnknown := httptest.NewRequest(http.MethodGet, "/internal/v1/validate-key?access_key_id=AKIAUNKNOWN", nil)
	respUnknown, err := app.Test(reqUnknown)
	if err != nil {
		t.Fatalf("unknown key request failed: %v", err)
	}
	if respUnknown.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for unknown key, got %d", respUnknown.StatusCode)
	}
}
