package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
)

type mockPinger struct {
	err error
}

func (m *mockPinger) Ping(ctx context.Context) error {
	return m.err
}

type mockAuthService struct {
	registerFunc func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error)
	loginFunc    func(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error)
}

func (m *mockAuthService) Register(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
	if m.registerFunc != nil {
		return m.registerFunc(ctx, req)
	}
	return nil, errors.New("not implemented")
}

func (m *mockAuthService) Login(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error) {
	if m.loginFunc != nil {
		return m.loginFunc(ctx, req)
	}
	return nil, errors.New("not implemented")
}

func TestServer_HealthRoutes(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: ":9095",
	}

	pinger := &mockPinger{err: nil}
	healthH := handler.NewHealthHandler(cfg, pinger)
	srv := New(cfg, nil, healthH)

	endpoints := []string{"/api/v1/health"}
	for _, endpoint := range endpoints {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		resp, err := srv.App().Test(req)
		if err != nil {
			t.Fatalf("request to %s failed: %v", endpoint, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 for %s, got %d", endpoint, resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("failed reading response body: %v", err)
		}

		var payload struct {
			Status  string `json:"status"`
			Service string `json:"service"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("failed parsing json: %v", err)
		}
		if payload.Status != "SERVING" {
			t.Errorf("expected status SERVING, got %s", payload.Status)
		}
		if payload.Service != "auth-svc" {
			t.Errorf("expected service auth-svc, got %s", payload.Service)
		}
	}
}

func TestServer_AuthRoutes(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: ":9095",
	}

	svc := &mockAuthService{
		registerFunc: func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
			return &model.RegisterResponse{
				ID:        uuid.New(),
				Username:  req.Username,
				Email:     req.Email,
				Role:      model.RoleUser,
				CreatedAt: time.Now().UTC(),
			}, nil
		},
	}
	authH := handler.NewAuthHandler(svc)
	srv := New(cfg, authH, nil)

	endpoints := []string{"/api/v1/register"}
	for _, endpoint := range endpoints {
		bodyBytes, _ := json.Marshal(model.RegisterRequest{
			Username: "testuser",
			Email:    "test@example.com",
			Password: "password123",
		})
		req := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")

		resp, err := srv.App().Test(req)
		if err != nil {
			t.Fatalf("request to %s failed: %v", endpoint, err)
		}
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected status 201 for %s, got %d", endpoint, resp.StatusCode)
		}
	}
}

func TestServer_LoginRoutes(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: ":9095",
	}

	svc := &mockAuthService{
		loginFunc: func(ctx context.Context, req model.LoginRequest) (*model.LoginResponse, error) {
			return &model.LoginResponse{
				Token:     "mock-jwt-token",
				ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
				ID:        uuid.New(),
				Username:  req.Username,
				Email:     "test@example.com",
				Role:      model.RoleUser,
			}, nil
		},
	}
	authH := handler.NewAuthHandler(svc)
	srv := New(cfg, authH, nil)

	endpoints := []string{"/api/v1/login"}
	for _, endpoint := range endpoints {
		bodyBytes, _ := json.Marshal(model.LoginRequest{
			Username: "testuser",
			Password: "password123",
		})
		req := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")

		resp, err := srv.App().Test(req)
		if err != nil {
			t.Fatalf("request to %s failed: %v", endpoint, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 for %s, got %d", endpoint, resp.StatusCode)
		}
	}
}

func TestServer_ErrorHandler(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: ":9095",
	}

	svc := &mockAuthService{
		registerFunc: func(ctx context.Context, req model.RegisterRequest) (*model.RegisterResponse, error) {
			return nil, model.ErrInvalidUsername
		},
	}
	authH := handler.NewAuthHandler(svc)
	srv := New(cfg, authH, nil)

	bodyBytes, _ := json.Marshal(model.RegisterRequest{
		Username: "ab",
		Email:    "invalid",
		Password: "123",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := srv.App().Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestServer_DashboardRoute_Protected(t *testing.T) {
	jwtSecret := "server-test-jwt-secret-key-32-chars!"
	cfg := config.Config{
		HTTPAddr:  ":9095",
		JWTSecret: jwtSecret,
		JWTExpiry: 1 * time.Hour,
	}

	authH := handler.NewAuthHandler(&mockAuthService{})
	pinger := &mockPinger{err: nil}
	healthH := handler.NewHealthHandler(cfg, pinger)
	srv := New(cfg, authH, healthH)

	// 1. Dashboard without token -> 401 Unauthorized
	reqUnauth := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	respUnauth, err := srv.App().Test(reqUnauth)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if respUnauth.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauthenticated dashboard, got %d", respUnauth.StatusCode)
	}

	// 2. Dashboard with valid token -> 200 OK
	userID := uuid.New()
	token, err := jwt.GenerateToken(userID, "alice", model.RoleUser, jwtSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	reqAuth := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	reqAuth.Header.Set("Authorization", "Bearer "+token)
	respAuth, err := srv.App().Test(reqAuth)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if respAuth.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for authenticated dashboard, got %d", respAuth.StatusCode)
	}

	// 3. Verify health endpoint remains open without token -> 200 OK
	reqHealth := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	respHealth, err := srv.App().Test(reqHealth)
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	if respHealth.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for open health endpoint, got %d", respHealth.StatusCode)
	}
}

func TestServer_Lifecycle(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: ":9095",
	}

	srv := New(cfg, nil, nil)
	if srv.App() == nil {
		t.Fatal("expected non-nil app")
	}

	if err := srv.Shutdown(); err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}

	if err := srv.ShutdownWithTimeout(1 * time.Second); err != nil {
		t.Fatalf("expected clean timeout shutdown, got %v", err)
	}
}

func TestServer_LogRoutes(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	cfg := config.Config{
		HTTPAddr: ":9095",
	}
	authH := handler.NewAuthHandler(&mockAuthService{})
	healthH := handler.NewHealthHandler(cfg, &mockPinger{})

	srv := New(cfg, authH, healthH, logger)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatal("expected debug logs to be emitted")
	}

	type logRecord struct {
		Level        string `json:"level"`
		Msg          string `json:"msg"`
		Method       string `json:"method"`
		Path         string `json:"path"`
		Endpoint     string `json:"endpoint"`
		FullEndpoint string `json:"full_endpoint"`
		URL          string `json:"url"`
	}

	seenPaths := make(map[string]logRecord)
	for _, line := range lines {
		var rec logRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("failed to parse log json: %v", err)
		}
		if rec.Level != "DEBUG" {
			t.Errorf("expected level DEBUG, got %s", rec.Level)
		}
		if rec.Msg != "active handler registered" {
			t.Errorf("expected msg 'active handler registered', got %s", rec.Msg)
		}
		if rec.Path != rec.Endpoint {
			t.Errorf("expected endpoint to match path, got endpoint=%s path=%s", rec.Endpoint, rec.Path)
		}
		expectedFull := "http://localhost:9095" + rec.Path
		if rec.FullEndpoint != expectedFull {
			t.Errorf("expected full_endpoint=%s, got %s", expectedFull, rec.FullEndpoint)
		}
		if rec.URL != expectedFull {
			t.Errorf("expected url=%s, got %s", expectedFull, rec.URL)
		}
		seenPaths[rec.Path] = rec
	}

	expectedPaths := []string{
		"/api/v1/health",
		"/api/v1/dashboard",
		"/api/v1/register",
		"/api/v1/login",
	}

	for _, expectedPath := range expectedPaths {
		if _, exists := seenPaths[expectedPath]; !exists {
			t.Errorf("expected active handler for path %s was not logged", expectedPath)
		}
	}

	// Test nil logger safety
	srv.LogRoutes(nil)
}

func TestServer_FormatEndpoint(t *testing.T) {
	tests := []struct {
		httpAddr string
		path     string
		expected string
	}{
		{
			httpAddr: ":9095",
			path:     "/api/v1/health",
			expected: "http://localhost:9095/api/v1/health",
		},
		{
			httpAddr: "127.0.0.1:9095",
			path:     "/api/v1/login",
			expected: "http://127.0.0.1:9095/api/v1/login",
		},
		{
			httpAddr: "0.0.0.0:9095",
			path:     "/api/v1/register",
			expected: "http://0.0.0.0:9095/api/v1/register",
		},
		{
			httpAddr: "http://auth.example.com",
			path:     "/api/v1/dashboard",
			expected: "http://auth.example.com/api/v1/dashboard",
		},
		{
			httpAddr: "",
			path:     "/api/v1/health",
			expected: "/api/v1/health",
		},
		{
			httpAddr: ":9095",
			path:     "api/v1/health",
			expected: "http://localhost:9095/api/v1/health",
		},
	}

	for _, tc := range tests {
		srv := &Server{
			cfg: config.Config{HTTPAddr: tc.httpAddr},
		}
		got := srv.formatEndpoint(tc.path)
		if got != tc.expected {
			t.Errorf("formatEndpoint(%q, %q) = %q, expected %q", tc.httpAddr, tc.path, got, tc.expected)
		}
	}
}


