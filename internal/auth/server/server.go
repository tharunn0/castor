package server

import (
	"log/slog"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/httperr"
)

type Server struct {
	app    *fiber.App
	cfg    config.Config
	logger *slog.Logger
}

func New(cfg config.Config, authHandler *handler.AuthHandler, healthHandler *handler.HealthHandler, loggers ...*slog.Logger) *Server {
	app := fiber.New(fiber.Config{
		ErrorHandler: httperr.ErrorHandler,
	})

	RegisterRoutes(app, cfg, authHandler, healthHandler)

	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}

	srv := &Server{
		app:    app,
		cfg:    cfg,
		logger: logger,
	}

	if logger != nil {
		srv.LogRoutes(logger)
	}

	return srv
}

func (s *Server) App() *fiber.App {
	return s.app
}

func (s *Server) LogRoutes(logger *slog.Logger) {
	if logger == nil {
		return
	}

	for _, route := range s.app.GetRoutes(true) {
		fullEndpoint := s.formatEndpoint(route.Path)
		logger.Debug("active handler registered",
			"method", route.Method,
			"path", route.Path,
			"endpoint", route.Path,
			"full_endpoint", fullEndpoint,
			"url", fullEndpoint,
		)
	}
}

func (s *Server) formatEndpoint(path string) string {
	if s.cfg.HTTPAddr == "" {
		return path
	}

	base := s.cfg.HTTPAddr
	if strings.HasPrefix(base, ":") {
		base = "localhost" + base
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(base, "/") + path
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

