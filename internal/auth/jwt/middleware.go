package jwt

import (
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/model"
)

const (
	ContextKeyUser   = "user"
	ContextKeyClaims = "claims"
	ContextKeyUserID = "user_id"
	ContextKeyRole   = "role"

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

		return c.Next()
	}
}

func RequireRole(roles ...model.Role) fiber.Handler {
	return func(c fiber.Ctx) error {
		claims, ok := GetClaims(c)
		if !ok || claims == nil {
			return fiber.NewError(fiber.StatusUnauthorized, "unauthorized")
		}

		for _, r := range roles {
			if claims.Role == r {
				return c.Next()
			}
		}

		return fiber.NewError(fiber.StatusForbidden, "forbidden: insufficient permissions")
	}
}

func GetClaims(c fiber.Ctx) (*Claims, bool) {
	val := c.Locals(ContextKeyClaims)
	if val == nil {
		return nil, false
	}
	claims, ok := val.(*Claims)
	return claims, ok
}

func GetUserID(c fiber.Ctx) (uuid.UUID, bool) {
	if claims, ok := GetClaims(c); ok {
		return claims.UserID, true
	}
	return uuid.Nil, false
}

func GetUserRole(c fiber.Ctx) (model.Role, bool) {
	if claims, ok := GetClaims(c); ok {
		return claims.Role, true
	}
	return "", false
}
