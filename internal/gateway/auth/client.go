package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var (
	ErrCredentialNotFound = errors.New("credential not found")
	ErrCredentialRevoked  = errors.New("credential is revoked")
	ErrInvalidAccessKey   = errors.New("access key id cannot be empty")
)

type CredentialStatus string

const (
	StatusActive  CredentialStatus = "ACTIVE"
	StatusRevoked CredentialStatus = "REVOKED"
)

type ValidateKeyResponse struct {
	AccessKeyID     string           `json:"access_key_id"`
	SecretAccessKey string           `json:"secret_access_key"`
	UserID          string           `json:"user_id"`
	Label           string           `json:"label,omitempty"`
	Status          CredentialStatus `json:"status"`
	CreatedAt       time.Time        `json:"created_at"`
}

type Client interface {
	ValidateKey(ctx context.Context, accessKeyID string) (*ValidateKeyResponse, error)
}

type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
	propagator propagation.TextMapPropagator
}

type ClientOption func(*HTTPClient)

func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *HTTPClient) {
		if client != nil {
			c.httpClient = client
		}
	}
}

func WithPropagator(p propagation.TextMapPropagator) ClientOption {
	return func(c *HTTPClient) {
		if p != nil {
			c.propagator = p
		}
	}
}

func NewHTTPClient(baseURL string, opts ...ClientOption) *HTTPClient {
	c := &HTTPClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		propagator: otel.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *HTTPClient) ValidateKey(ctx context.Context, accessKeyID string) (*ValidateKeyResponse, error) {
	if strings.TrimSpace(accessKeyID) == "" {
		return nil, ErrInvalidAccessKey
	}

	endpoint := fmt.Sprintf("%s/internal/v1/validate-key?access_key_id=%s", c.baseURL, url.QueryEscape(accessKeyID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if c.propagator != nil {
		c.propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var keyResp ValidateKeyResponse
		if err := json.NewDecoder(resp.Body).Decode(&keyResp); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		if keyResp.Status == StatusRevoked {
			return nil, ErrCredentialRevoked
		}
		if keyResp.Status != StatusActive {
			return nil, ErrCredentialNotFound
		}
		return &keyResp, nil
	case http.StatusNotFound:
		return nil, ErrCredentialNotFound
	case http.StatusUnauthorized:
		return nil, ErrCredentialRevoked
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}
