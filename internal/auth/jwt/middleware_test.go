package jwt

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/model"
)

func TestMiddleware_AuthorizationHeader(t *testing.T) {
	app := fiber.New()
	app.Use(NewMiddleware(testSecret))

	app.Get("/protected", func(c fiber.Ctx) error {
		claims, ok := GetClaims(c)
		if !ok || claims == nil {
			return fiber.NewError(fiber.StatusInternalServerError, "missing claims in context")
		}
		uid, _ := GetUserID(c)
		role, _ := GetUserRole(c)

		return c.JSON(fiber.Map{
			"user_id":  uid.String(),
			"username": claims.Username,
			"role":     string(role),
		})
	})

	userID := uuid.New()
	token, err := GenerateToken(userID, "alice", model.RoleUser, testSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	// 1. Success with Authorization: Bearer <token>
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var payload struct {
		UserID   string `json:"user_id"`
		Username string `json:"username"`
		Role     string `json:"role"`
	}
	_ = json.Unmarshal(body, &payload)
	if payload.UserID != userID.String() || payload.Username != "alice" || payload.Role != string(model.RoleUser) {
		t.Errorf("unexpected payload: %+v", payload)
	}

	// 2. Case-insensitive bearer
	reqCase := httptest.NewRequest(http.MethodGet, "/protected", nil)
	reqCase.Header.Set("Authorization", "bearer "+token)
	respCase, _ := app.Test(reqCase)
	if respCase.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 for lowercase bearer, got %d", respCase.StatusCode)
	}

	// 3. Missing authorization
	reqMissing := httptest.NewRequest(http.MethodGet, "/protected", nil)
	respMissing, _ := app.Test(reqMissing)
	if respMissing.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 for missing token, got %d", respMissing.StatusCode)
	}

	// 4. Expired token
	expiredToken, _ := GenerateToken(userID, "alice", model.RoleUser, testSecret, -1*time.Hour)
	reqExpired := httptest.NewRequest(http.MethodGet, "/protected", nil)
	reqExpired.Header.Set("Authorization", "Bearer "+expiredToken)
	respExpired, _ := app.Test(reqExpired)
	if respExpired.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 for expired token, got %d", respExpired.StatusCode)
	}

	// 5. Invalid token signature
	tamperedToken, _ := GenerateToken(userID, "alice", model.RoleUser, "wrong-secret-key", 1*time.Hour)
	reqTampered := httptest.NewRequest(http.MethodGet, "/protected", nil)
	reqTampered.Header.Set("Authorization", "Bearer "+tamperedToken)
	respTampered, _ := app.Test(reqTampered)
	if respTampered.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 for tampered token, got %d", respTampered.StatusCode)
	}
}

func TestMiddleware_Cookie(t *testing.T) {
	app := fiber.New()
	app.Use(NewMiddlewareWithConfig(MiddlewareConfig{
		Secret:     testSecret,
		CookieName: "custom_jwt",
	}))

	app.Get("/cookie-protected", func(c fiber.Ctx) error {
		uid, ok := GetUserID(c)
		if !ok {
			return fiber.NewError(fiber.StatusInternalServerError, "user_id not found")
		}
		return c.SendString(uid.String())
	})

	userID := uuid.New()
	token, err := GenerateToken(userID, "bob", model.RoleUser, testSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	// 1. Success via custom cookie
	req := httptest.NewRequest(http.MethodGet, "/cookie-protected", nil)
	req.AddCookie(&http.Cookie{
		Name:  "custom_jwt",
		Value: token,
	})

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	// 2. Success via fallback cookie "token"
	reqFallback := httptest.NewRequest(http.MethodGet, "/cookie-protected", nil)
	reqFallback.AddCookie(&http.Cookie{
		Name:  "token",
		Value: token,
	})

	respFallback, _ := app.Test(reqFallback)
	if respFallback.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 via fallback token cookie, got %d", respFallback.StatusCode)
	}
}

func TestRequireRole(t *testing.T) {
	app := fiber.New()
	app.Use(NewMiddleware(testSecret))

	app.Get("/admin-only", RequireRole(model.RoleAdmin), func(c fiber.Ctx) error {
		return c.SendString("admin access granted")
	})

	app.Get("/user-or-admin", RequireRole(model.RoleAdmin, model.RoleUser), func(c fiber.Ctx) error {
		return c.SendString("access granted")
	})

	adminID := uuid.New()
	adminToken, _ := GenerateToken(adminID, "superadmin", model.RoleAdmin, testSecret, 1*time.Hour)

	userID := uuid.New()
	userToken, _ := GenerateToken(userID, "regularuser", model.RoleUser, testSecret, 1*time.Hour)

	// Admin accesses admin-only route -> 200 OK
	reqAdmin := httptest.NewRequest(http.MethodGet, "/admin-only", nil)
	reqAdmin.Header.Set("Authorization", "Bearer "+adminToken)
	respAdmin, _ := app.Test(reqAdmin)
	if respAdmin.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 for admin, got %d", respAdmin.StatusCode)
	}

	// Regular user accesses admin-only route -> 403 Forbidden
	reqForbidden := httptest.NewRequest(http.MethodGet, "/admin-only", nil)
	reqForbidden.Header.Set("Authorization", "Bearer "+userToken)
	respForbidden, _ := app.Test(reqForbidden)
	if respForbidden.StatusCode != http.StatusForbidden {
		t.Errorf("expected status 403 for user on admin-only route, got %d", respForbidden.StatusCode)
	}

	// Regular user accesses multi-role route -> 200 OK
	reqMulti := httptest.NewRequest(http.MethodGet, "/user-or-admin", nil)
	reqMulti.Header.Set("Authorization", "Bearer "+userToken)
	respMulti, _ := app.Test(reqMulti)
	if respMulti.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 for user on multi-role route, got %d", respMulti.StatusCode)
	}

	// Unauthenticated request to role-guarded route (without middleware setting claims)
	bareApp := fiber.New()
	bareApp.Get("/unauthenticated", RequireRole(model.RoleAdmin), func(c fiber.Ctx) error {
		return c.SendString("should not reach here")
	})
	reqBare := httptest.NewRequest(http.MethodGet, "/unauthenticated", nil)
	respBare, _ := bareApp.Test(reqBare)
	if respBare.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 for unauthenticated request, got %d", respBare.StatusCode)
	}
}
