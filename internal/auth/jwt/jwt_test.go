package jwt

import (
	"errors"
	"testing"
	"time"

	golangjwt "github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/model"
)

const testSecret = "test-jwt-secret-key-32-chars-long!"

func TestGenerateAndVerifyToken(t *testing.T) {
	userID := uuid.New()
	username := "testuser"
	role := model.RoleUser
	ttl := 1 * time.Hour

	tokenStr, err := GenerateToken(userID, username, role, testSecret, ttl)
	if err != nil {
		t.Fatalf("unexpected error generating token: %v", err)
	}
	if tokenStr == "" {
		t.Fatal("expected non-empty token string")
	}

	claims, err := VerifyToken(tokenStr, testSecret)
	if err != nil {
		t.Fatalf("unexpected error verifying token: %v", err)
	}

	if claims.UserID != userID {
		t.Errorf("expected UserID %v, got %v", userID, claims.UserID)
	}
	if claims.Username != username {
		t.Errorf("expected Username %s, got %s", username, claims.Username)
	}
	if claims.Role != role {
		t.Errorf("expected Role %s, got %s", role, claims.Role)
	}
	if claims.Subject != userID.String() {
		t.Errorf("expected Subject %s, got %s", userID.String(), claims.Subject)
	}
	if claims.Issuer != "castor-auth" {
		t.Errorf("expected Issuer castor-auth, got %s", claims.Issuer)
	}
}

func TestGenerateToken_ValidationErrors(t *testing.T) {
	userID := uuid.New()

	if _, err := GenerateToken(userID, "user", model.RoleUser, "", 1*time.Hour); !errors.Is(err, ErrEmptySecret) {
		t.Errorf("expected ErrEmptySecret, got %v", err)
	}

	if _, err := GenerateToken(uuid.Nil, "user", model.RoleUser, testSecret, 1*time.Hour); !errors.Is(err, model.ErrInvalidUserID) {
		t.Errorf("expected ErrInvalidUserID, got %v", err)
	}

	if _, err := GenerateToken(userID, "user", model.Role("INVALID"), testSecret, 1*time.Hour); !errors.Is(err, model.ErrInvalidRole) {
		t.Errorf("expected ErrInvalidRole, got %v", err)
	}
}

func TestGenerateUserToken(t *testing.T) {
	u := &model.User{
		ID:        uuid.New(),
		Username:  "adminuser",
		Role:      model.RoleAdmin,
		CreatedAt: time.Now().UTC(),
	}

	tokenStr, err := GenerateUserToken(u, testSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error generating user token: %v", err)
	}

	claims, err := VerifyToken(tokenStr, testSecret)
	if err != nil {
		t.Fatalf("unexpected error verifying user token: %v", err)
	}
	if claims.UserID != u.ID || claims.Role != model.RoleAdmin {
		t.Errorf("claims mismatch: %+v", claims)
	}

	if _, err := GenerateUserToken(nil, testSecret, 1*time.Hour); err == nil {
		t.Error("expected error with nil user, got nil")
	}
}

func TestVerifyToken_Errors(t *testing.T) {
	userID := uuid.New()

	// Empty secret
	if _, err := VerifyToken("some-token", ""); !errors.Is(err, ErrEmptySecret) {
		t.Errorf("expected ErrEmptySecret, got %v", err)
	}

	// Empty token string
	if _, err := VerifyToken("   ", testSecret); !errors.Is(err, ErrMissingToken) {
		t.Errorf("expected ErrMissingToken, got %v", err)
	}

	// Malformed token
	if _, err := VerifyToken("not-a-valid-token", testSecret); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("expected ErrInvalidToken, got %v", err)
	}

	// Wrong secret
	tokenStr, err := GenerateToken(userID, "user", model.RoleUser, testSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}
	if _, err := VerifyToken(tokenStr, "different-secret-key-1234567890"); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("expected ErrInvalidToken for wrong secret, got %v", err)
	}

	// Expired token
	expiredTokenStr, err := GenerateToken(userID, "user", model.RoleUser, testSecret, -1*time.Hour)
	if err != nil {
		t.Fatalf("generate expired token failed: %v", err)
	}
	if _, err := VerifyToken(expiredTokenStr, testSecret); !errors.Is(err, ErrExpiredToken) {
		t.Errorf("expected ErrExpiredToken, got %v", err)
	}

	// Signing method none
	noneClaims := Claims{
		UserID: userID,
		RegisteredClaims: golangjwt.RegisteredClaims{
			Subject: userID.String(),
		},
	}
	noneToken := golangjwt.NewWithClaims(golangjwt.SigningMethodNone, noneClaims)
	noneTokenStr, err := noneToken.SignedString(golangjwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("generate none token failed: %v", err)
	}
	if _, err := VerifyToken(noneTokenStr, testSecret); !errors.Is(err, ErrInvalidSigningMethod) && !errors.Is(err, ErrInvalidToken) {
		t.Errorf("expected ErrInvalidSigningMethod or ErrInvalidToken, got %v", err)
	}
}
