package handler

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/config"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type HealthHandler struct {
	cfg    config.Config
	pinger Pinger
}

func NewHealthHandler(cfg config.Config, pinger Pinger) *HealthHandler {
	return &HealthHandler{
		cfg:    cfg,
		pinger: pinger,
	}
}

func (h *HealthHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/health", h.Check)
	app.Get("/healthz", h.Check)
}

func (h *HealthHandler) Check(c fiber.Ctx) error {
	dbStatus := "UNKNOWN"
	isHealthy := true

	if h.pinger != nil {
		ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
		defer cancel()

		if err := h.pinger.Ping(ctx); err != nil {
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
