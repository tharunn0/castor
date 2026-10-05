package health

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/httperr"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

func NewApp(cfg config.Config, pinger Pinger) *fiber.App {
	app := fiber.New(fiber.Config{
		ErrorHandler: httperr.ErrorHandler,
	})
	handler := NewHandler(cfg, pinger)

	app.Get("/health", handler)
	app.Get("/healthz", handler)

	return app
}

func NewHandler(cfg config.Config, pinger Pinger) fiber.Handler {
	return func(c fiber.Ctx) error {
		dbStatus := "UNKNOWN"
		isHealthy := true

		if pinger != nil {
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
			defer cancel()

			if err := pinger.Ping(ctx); err != nil {
				dbStatus = "UNHEALTHY"
				isHealthy = false
			} else {
				dbStatus = "HEALTHY"
			}
		}

		status := fiber.StatusOK
		respStatus := "SERVING"
		if !isHealthy {
			status = fiber.StatusServiceUnavailable
			respStatus = "NOT_SERVING"
		}

		return c.Status(status).JSON(fiber.Map{
			"status":    respStatus,
			"service":   "auth-svc",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"database": fiber.Map{
				"engine": "postgres-17",
				"status": dbStatus,
			},
		})
	}
}
