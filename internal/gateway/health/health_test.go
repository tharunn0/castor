package health

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tharunn0/castor/internal/gateway/config"
)

func TestHealthEndpoints(t *testing.T) {
	cfg := config.Config{
		ChunkSize:   4096,
		WriteQuorum: 2,
		DataNodes:   []string{"node1", "node2"},
	}

	app := NewApp(cfg)

	endpoints := []string{"/health", "/healthz"}
	for _, endpoint := range endpoints {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		resp, err := app.Test(req)
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

		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("invalid json response from %s: %v", endpoint, err)
		}

		if payload["status"] != "SERVING" {
			t.Errorf("expected status SERVING, got %v", payload["status"])
		}
		if payload["service"] != "gateway-svc" {
			t.Errorf("expected service gateway-svc, got %v", payload["service"])
		}
	}
}
