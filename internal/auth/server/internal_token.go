package server

import (
	"crypto/subtle"

	"github.com/gofiber/fiber/v3"
)

const InternalTokenHeader = "X-Castor-Internal-Token"

// requireInternalToken returns 403 rather than 401 so the gateway reports a
// token mismatch as a misconfiguration instead of treating every key as revoked.
func requireInternalToken(token string) fiber.Handler {
	return func(c fiber.Ctx) error {
		got := c.Get(InternalTokenHeader)
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			return fiber.NewError(fiber.StatusForbidden, "invalid internal token")
		}
		return c.Next()
	}
}
