package server

import (
	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/handler"
)

func RegisterRoutes(app *fiber.App, authHandler *handler.AuthHandler, healthHandler *handler.HealthHandler) {
	if healthHandler != nil {
		app.Get("/health", healthHandler.Check)
		app.Get("/healthz", healthHandler.Check)
	}

	if authHandler != nil {
		app.Post("/register", authHandler.Register)
		app.Post("/api/auth/register", authHandler.Register)
	}
}
