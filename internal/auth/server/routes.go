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
		v1.Post("/register", authHandler.Register)
		v1.Post("/login", authHandler.Login)
		v1.Get("/dashboard", jwt.NewMiddleware(cfg.JWTSecret), authHandler.Dashboard)
	}
}
