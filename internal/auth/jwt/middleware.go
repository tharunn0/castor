package jwt

import (
	"context"
	"slices"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/authctx"
	"github.com/tharunn0/castor/internal/auth/model"
)

type contextKey struct{ name string }

var (
	claimsCtxKey   = contextKey{name: "jwt_claims"}
	userIDCtxKey   = contextKey{name: "jwt_user_id"}
	userRoleCtxKey = contextKey{name: "jwt_user_role"}
)

const (
	ContextKeyUser   = "user"
	ContextKeyClaims = "claims"
	ContextKeyUserID = "user_id"
	ContextKeyRole   = "role"

	ContextKeyUsername = "username"

	DefaultCookieName = "jwt_token"
)

type MiddlewareConfig struct {
	Secret     string
	CookieName string
}

func NewMiddleware(secret string) fiber.Handler {
	return NewMiddlewareWithConfig(MiddlewareConfig{
		Secret:     secret,
		CookieName: DefaultCookieName,
	})
}

func NewMiddlewareWithConfig(cfg MiddlewareConfig) fiber.Handler {
	cookieName := cfg.CookieName
	if cookieName == "" {
		cookieName = DefaultCookieName
	}

	return func(c fiber.Ctx) error {
		var tokenStr string

		authHeader := c.Get("Authorization")
		if authHeader != "" {
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
				tokenStr = strings.TrimSpace(parts[1])
			}
		}

		if tokenStr == "" {
			if cookieVal := c.Cookies(cookieName); cookieVal != "" {
				tokenStr = cookieVal
			} else if altCookie := c.Cookies("token"); altCookie != "" {
				tokenStr = altCookie
			}
		}

		if tokenStr == "" {
			return fiber.NewError(fiber.StatusUnauthorized, ErrMissingToken.Error())
		}

		claims, err := VerifyToken(tokenStr, cfg.Secret)
		if err != nil {
			return fiber.NewError(fiber.StatusUnauthorized, err.Error())
		}

		c.Locals(ContextKeyUser, claims)
		c.Locals(ContextKeyClaims, claims)
		c.Locals(ContextKeyUserID, claims.UserID)
		c.Locals(ContextKeyRole, claims.Role)
		c.Locals(ContextKeyUsername, claims.Username)

		reqCtx := c.Context()
		if reqCtx != nil {
			reqCtx = authctx.WithUser(reqCtx, authctx.UserContext{
				UserID:   claims.UserID,
				Username: claims.Username,
				Role:     claims.Role,
			})
			reqCtx = WithClaims(reqCtx, claims)
			reqCtx = WithUserID(reqCtx, claims.UserID)
			reqCtx = WithUserRole(reqCtx, claims.Role)
			c.SetContext(reqCtx)
		}

		return c.Next()
	}
}

func RequireRole(roles ...model.Role) fiber.Handler {
	return func(c fiber.Ctx) error {
		claims, ok := GetClaims(c)
		if !ok || claims == nil {
			return fiber.NewError(fiber.StatusUnauthorized, "unauthorized")
		}

		if slices.Contains(roles, claims.Role) {
			return c.Next()
		}

		return fiber.NewError(fiber.StatusForbidden, "forbidden: insufficient permissions")
	}
}

func WithClaims(ctx context.Context, claims *Claims) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, claimsCtxKey, claims)
}

func WithUserID(ctx context.Context, userID uuid.UUID) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, userIDCtxKey, userID)
}

func WithUserRole(ctx context.Context, role model.Role) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, userRoleCtxKey, role)
}

func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	if ctx == nil {
		return nil, false
	}
	val := ctx.Value(claimsCtxKey)
	if val == nil {
		return nil, false
	}
	claims, ok := val.(*Claims)
	return claims, ok && claims != nil
}

func UserIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	if claims, ok := ClaimsFromContext(ctx); ok && claims != nil && claims.UserID != uuid.Nil {
		return claims.UserID, true
	}
	if ctx == nil {
		return uuid.Nil, false
	}
	val := ctx.Value(userIDCtxKey)
	if val == nil {
		return uuid.Nil, false
	}
	uID, ok := val.(uuid.UUID)
	return uID, ok && uID != uuid.Nil
}

func UserRoleFromContext(ctx context.Context) (model.Role, bool) {
	if claims, ok := ClaimsFromContext(ctx); ok && claims != nil && claims.Role != "" {
		return claims.Role, true
	}
	if ctx == nil {
		return "", false
	}
	val := ctx.Value(userRoleCtxKey)
	if val == nil {
		return "", false
	}
	role, ok := val.(model.Role)
	return role, ok && role != ""
}

func GetClaims(c fiber.Ctx) (*Claims, bool) {
	if c == nil {
		return nil, false
	}
	if val := c.Locals(ContextKeyClaims); val != nil {
		if claims, ok := val.(*Claims); ok && claims != nil {
			return claims, true
		}
	}
	return ClaimsFromContext(c.Context())
}

func GetUserID(c fiber.Ctx) (uuid.UUID, bool) {
	if claims, ok := GetClaims(c); ok && claims != nil && claims.UserID != uuid.Nil {
		return claims.UserID, true
	}
	if c == nil {
		return uuid.Nil, false
	}
	if val := c.Locals(ContextKeyUserID); val != nil {
		if uID, ok := val.(uuid.UUID); ok && uID != uuid.Nil {
			return uID, true
		}
	}
	return UserIDFromContext(c.Context())
}

func GetUserRole(c fiber.Ctx) (model.Role, bool) {
	if claims, ok := GetClaims(c); ok && claims != nil && claims.Role != "" {
		return claims.Role, true
	}
	if c == nil {
		return "", false
	}
	if val := c.Locals(ContextKeyRole); val != nil {
		if role, ok := val.(model.Role); ok && role != "" {
			return role, true
		}
	}
	return UserRoleFromContext(c.Context())
}
