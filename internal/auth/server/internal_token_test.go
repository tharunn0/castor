package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/model"
)

type validatingAuthService struct {
	mockAuthService
}

func (*validatingAuthService) ValidateAccessKey(ctx context.Context, accessKeyID string) (*model.S3Credential, error) {
	return &model.S3Credential{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: "secret",
		Status:          model.StatusActive,
	}, nil
}

func TestServer_ValidateKey_InternalToken(t *testing.T) {
	tests := []struct {
		name       string
		cfgToken   string
		reqToken   string
		wantStatus int
	}{
		{name: "no token configured allows request", cfgToken: "", reqToken: "", wantStatus: http.StatusOK},
		{name: "missing token rejected", cfgToken: "s3cr3t", reqToken: "", wantStatus: http.StatusForbidden},
		{name: "wrong token rejected", cfgToken: "s3cr3t", reqToken: "nope", wantStatus: http.StatusForbidden},
		{name: "correct token allowed", cfgToken: "s3cr3t", reqToken: "s3cr3t", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Config{HTTPAddr: ":9095", JWTSecret: "jwt", InternalToken: tt.cfgToken}
			srv := New(cfg, handler.NewAuthHandler(&validatingAuthService{}), nil)

			req := httptest.NewRequest(http.MethodGet, "/internal/v1/validate-key?access_key_id=AKIATEST", nil)
			if tt.reqToken != "" {
				req.Header.Set(InternalTokenHeader, tt.reqToken)
			}
			resp, err := srv.App().Test(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}
