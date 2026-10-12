package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPClient_ValidateKey(t *testing.T) {
	createdAt := time.Date(2026, 10, 9, 11, 45, 0, 0, time.UTC)

	tests := []struct {
		name        string
		accessKeyID string
		handler     http.HandlerFunc
		expected    *ValidateKeyResponse
		expectedErr error
	}{
		{
			name:        "empty access key",
			accessKeyID: "",
			expectedErr: ErrInvalidAccessKey,
		},
		{
			name:        "whitespace access key",
			accessKeyID: "   ",
			expectedErr: ErrInvalidAccessKey,
		},
		{
			name:        "active credential success",
			accessKeyID: "AKIA7X9M2Q4L8V1P3K6J",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("access_key_id") != "AKIA7X9M2Q4L8V1P3K6J" {
					http.Error(w, `{"error":"wrong key"}`, http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(ValidateKeyResponse{
					AccessKeyID:     "AKIA7X9M2Q4L8V1P3K6J",
					SecretAccessKey: "vN8kL3pQ9wR2mS5tU7xY1aB4cE6dF8gH0jK2mP4r",
					UserID:          "e67406a4-4a57-4cc3-9a3b-9e4eb017c699",
					Label:           "MacBook Pro CLI",
					Status:          StatusActive,
					CreatedAt:       createdAt,
				})
			},
			expected: &ValidateKeyResponse{
				AccessKeyID:     "AKIA7X9M2Q4L8V1P3K6J",
				SecretAccessKey: "vN8kL3pQ9wR2mS5tU7xY1aB4cE6dF8gH0jK2mP4r",
				UserID:          "e67406a4-4a57-4cc3-9a3b-9e4eb017c699",
				Label:           "MacBook Pro CLI",
				Status:          StatusActive,
				CreatedAt:       createdAt,
			},
		},
		{
			name:        "credential not found 404",
			accessKeyID: "AKIANOTFOUND12345678",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			expectedErr: ErrCredentialNotFound,
		},
		{
			name:        "credential revoked 401",
			accessKeyID: "AKIAREVOKED123456789",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
			},
			expectedErr: ErrCredentialRevoked,
		},
		{
			name:        "credential revoked in payload",
			accessKeyID: "AKIAREVOKEDPAYLOAD12",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(ValidateKeyResponse{
					AccessKeyID: "AKIAREVOKEDPAYLOAD12",
					Status:      StatusRevoked,
				})
			},
			expectedErr: ErrCredentialRevoked,
		},
		{
			name:        "credential inactive in payload",
			accessKeyID: "AKIAINACTIVE12345678",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(ValidateKeyResponse{
					AccessKeyID: "AKIAINACTIVE12345678",
					Status:      "DISABLED",
				})
			},
			expectedErr: ErrCredentialNotFound,
		},
		{
			name:        "server error 500",
			accessKeyID: "AKIASERVERERR1234567",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "database unreachable", http.StatusInternalServerError)
			},
			expectedErr: errors.New("unexpected status code 500: database unreachable"),
		},
		{
			name:        "malformed response",
			accessKeyID: "AKIAMALFORMED1234567",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("{invalid-json"))
			},
			expectedErr: errors.New("decode response"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var server *httptest.Server
			baseURL := "http://localhost:0"
			if tt.handler != nil {
				server = httptest.NewServer(tt.handler)
				defer server.Close()
				baseURL = server.URL
			}

			client := NewHTTPClient(baseURL)
			resp, err := client.ValidateKey(context.Background(), tt.accessKeyID)

			if tt.expectedErr != nil {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.expectedErr.Error())
				}
				if !errors.Is(err, tt.expectedErr) && !contains(err.Error(), tt.expectedErr.Error()) {
					t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.AccessKeyID != tt.expected.AccessKeyID ||
				resp.SecretAccessKey != tt.expected.SecretAccessKey ||
				resp.UserID != tt.expected.UserID ||
				resp.Status != tt.expected.Status {
				t.Fatalf("response mismatch, got %+v, want %+v", resp, tt.expected)
			}
		})
	}
}

func TestHTTPClient_TraceContextPropagation(t *testing.T) {
	var capturedTraceparent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceparent = r.Header.Get("traceparent")
		_ = json.NewEncoder(w).Encode(ValidateKeyResponse{
			AccessKeyID: "AKIAVALIDKEY12345678",
			Status:      StatusActive,
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, WithPropagator(propagation.TraceContext{}))

	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	_, err := client.ValidateKey(ctx, "AKIAVALIDKEY12345678")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedTraceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("expected traceparent header to be propagated, got %q", capturedTraceparent)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(substr) > 0 && errors.New(s).Error() != "" && containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) > 0 && stringContains(s, substr))
}

func stringContains(s, substr string) bool {
	for i := range len(s) - len(substr) + 1 {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
