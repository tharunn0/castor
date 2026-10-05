package server

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

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/handler"
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
