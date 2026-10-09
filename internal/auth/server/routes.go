package server

import (
	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/jwt"
)

func RegisterRoutes(app *fiber.App, cfg config.Config, authHandler *handler.AuthHandler, healthHandler *handler.HealthHandler) {
	v1 := app.Group("/api/v1")

	if healthHandler != nil {
		v1.Get("/health", healthHandler.Check)
	}

	if authHandler != nil {
		authMW := jwt.NewMiddleware(cfg.JWTSecret)
		v1.Post("/register", authHandler.Register)
		v1.Post("/login", authHandler.Login)
		v1.Get("/dashboard", authMW, authHandler.Dashboard)
		v1.Post("/keys", authMW, authHandler.CreateKey)
		v1.Get("/keys", authMW, authHandler.ListKeys)
		v1.Delete("/keys/:key_id", authMW, authHandler.RevokeKey)

		internal := app.Group("/internal/v1")
		internal.Get("/validate-key", authHandler.ValidateKey)
	}
}
