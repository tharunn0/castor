package httperr

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
)

func TestTranslate(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedMsg    string
	}{
		{
			name:           "nil error",
			err:            nil,
			expectedStatus: fiber.StatusOK,
			expectedMsg:    "",
		},
		{
			name:           "fiber error",
			err:            fiber.NewError(fiber.StatusBadRequest, "malformed payload"),
			expectedStatus: fiber.StatusBadRequest,
			expectedMsg:    "malformed payload",
		},
		{
			name:           "domain validation error",
			err:            model.ErrInvalidUsername,
			expectedStatus: fiber.StatusBadRequest,
			expectedMsg:    model.ErrInvalidUsername.Error(),
		},
		{
			name:           "wrapped domain validation error",
			err:            fmt.Errorf("validation failed: %w", model.ErrInvalidEmail),
			expectedStatus: fiber.StatusBadRequest,
			expectedMsg:    model.ErrInvalidEmail.Error(),
		},
		{
			name:           "repository conflict error",
			err:            repository.ErrUserAlreadyExists,
			expectedStatus: fiber.StatusConflict,
			expectedMsg:    repository.ErrUserAlreadyExists.Error(),
		},
		{
			name:           "repository not found error",
			err:            repository.ErrUserNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedMsg:    repository.ErrUserNotFound.Error(),
		},
		{
			name:           "jwt invalid token error",
			err:            jwt.ErrInvalidToken,
			expectedStatus: fiber.StatusUnauthorized,
			expectedMsg:    jwt.ErrInvalidToken.Error(),
		},
		{
			name:           "jwt expired token error",
			err:            jwt.ErrExpiredToken,
			expectedStatus: fiber.StatusUnauthorized,
			expectedMsg:    jwt.ErrExpiredToken.Error(),
		},
		{
			name:           "invalid credentials error",
			err:            model.ErrInvalidCredentials,
			expectedStatus: fiber.StatusUnauthorized,
			expectedMsg:    model.ErrInvalidCredentials.Error(),
		},
		{
			name:           "unmapped internal server error",
			err:            errors.New("db connection failure"),
			expectedStatus: fiber.StatusInternalServerError,
			expectedMsg:    "internal server error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, msg := Translate(tc.err)
			if status != tc.expectedStatus {
				t.Errorf("expected status %d, got %d", tc.expectedStatus, status)
			}
			if msg != tc.expectedMsg {
				t.Errorf("expected message %q, got %q", tc.expectedMsg, msg)
			}
		})
	}
}

func TestErrorHandler_Integration(t *testing.T) {
	app := fiber.New(fiber.Config{
		ErrorHandler: ErrorHandler,
	})

	app.Get("/test-conflict", func(c fiber.Ctx) error {
		return repository.ErrUserAlreadyExists
	})

	req := httptest.NewRequest(http.MethodGet, "/test-conflict", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	if resp.StatusCode != fiber.StatusConflict {
		t.Fatalf("expected status %d, got %d", fiber.StatusConflict, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed reading response body: %v", err)
	}

	expectedJSON := `{"error":"user already exists"}`
	if string(body) != expectedJSON {
		t.Errorf("expected body %s, got %s", expectedJSON, string(body))
	}
}
