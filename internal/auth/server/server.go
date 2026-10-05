package server

import (
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/httperr"
)

type Server struct {
	app *fiber.App
	cfg config.Config
}

func New(cfg config.Config, authHandler *handler.AuthHandler, healthHandler *handler.HealthHandler) *Server {
	app := fiber.New(fiber.Config{
		ErrorHandler: httperr.ErrorHandler,
	})

	RegisterRoutes(app, authHandler, healthHandler)

	return &Server{
		app: app,
		cfg: cfg,
	}
}

func (s *Server) App() *fiber.App {
	return s.app
}

func (s *Server) Listen() error {
	return s.app.Listen(s.cfg.HTTPAddr, fiber.ListenConfig{
		DisableStartupMessage: true,
	})
}

func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}

func (s *Server) ShutdownWithTimeout(timeout time.Duration) error {
	return s.app.ShutdownWithTimeout(timeout)
}
