package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/config"
)

type mockPinger struct {
	err error
}

func (m *mockPinger) Ping(ctx context.Context) error {
	return m.err
}

func TestHealthHandler(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: ":9095",
	}

	tests := []struct {
		name           string
		pinger         Pinger
		expectedStatus int
		expectedState  string
		expectedDB     string
	}{
		{
			name:           "healthy database",
			pinger:         &mockPinger{err: nil},
			expectedStatus: http.StatusOK,
			expectedState:  "SERVING",
			expectedDB:     "HEALTHY",
		},
		{
			name:           "unhealthy database",
			pinger:         &mockPinger{err: errors.New("connection refused")},
			expectedStatus: http.StatusServiceUnavailable,
			expectedState:  "NOT_SERVING",
			expectedDB:     "UNHEALTHY",
		},
		{
			name:           "nil database pinger",
			pinger:         nil,
			expectedStatus: http.StatusOK,
			expectedState:  "SERVING",
			expectedDB:     "UNKNOWN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			h := NewHealthHandler(cfg, tc.pinger)
			h.RegisterRoutes(app)

			endpoints := []string{"/health", "/healthz"}
			for _, endpoint := range endpoints {
				req := httptest.NewRequest(http.MethodGet, endpoint, nil)
				resp, err := app.Test(req)
				if err != nil {
					t.Fatalf("request to %s failed: %v", endpoint, err)
				}
				if resp.StatusCode != tc.expectedStatus {
					t.Fatalf("expected status %d for %s, got %d", tc.expectedStatus, endpoint, resp.StatusCode)
				}

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("failed reading response body: %v", err)
				}

				var payload struct {
					Status   string `json:"status"`
					Service  string `json:"service"`
					Database struct {
						Engine string `json:"engine"`
						Status string `json:"status"`
					} `json:"database"`
				}

				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatalf("invalid json response from %s: %v", endpoint, err)
				}

				if payload.Status != tc.expectedState {
					t.Errorf("expected status %s, got %s", tc.expectedState, payload.Status)
				}
				if payload.Service != "auth-svc" {
					t.Errorf("expected service auth-svc, got %s", payload.Service)
				}
				if payload.Database.Engine != "postgres-17" {
					t.Errorf("expected engine postgres-17, got %s", payload.Database.Engine)
				}
				if payload.Database.Status != tc.expectedDB {
					t.Errorf("expected database status %s, got %s", tc.expectedDB, payload.Database.Status)
				}
			}
		})
	}
}
