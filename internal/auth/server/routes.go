package server

import (
	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/handler"
)

func RegisterRoutes(app *fiber.App, authHandler *handler.AuthHandler, healthHandler *handler.HealthHandler) {
	v1 := app.Group("/api/v1")

	if healthHandler != nil {
		v1.Get("/health", healthHandler.Check)
	}

	if authHandler != nil {
		v1.Post("/register", authHandler.Register)
		v1.Post("/login", authHandler.Login)
	}
}
