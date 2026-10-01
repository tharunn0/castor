package health

import (
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/gateway/config"
)

func NewApp(cfg config.Config) *fiber.App {
	app := fiber.New()
	handler := NewHandler(cfg)

	app.Get("/health", handler)
	app.Get("/healthz", handler)

	return app
}

func NewHandler(cfg config.Config) fiber.Handler {
	return func(c fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status":    "SERVING",
			"service":   "gateway-svc",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"cluster":   "castor",
			"version":   "v0.1.0",
			"config": fiber.Map{
				"write_quorum": cfg.WriteQuorum,
				"data_nodes":   len(cfg.DataNodes),
				"chunk_size":   cfg.ChunkSize,
			},
		})
	}
}
