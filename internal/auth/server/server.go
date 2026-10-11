package server

import (
	"log/slog"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/httperr"
	"github.com/tharunn0/castor/internal/telemetry"
)

type Server struct {
	app    *fiber.App
	cfg    config.Config
	logger *slog.Logger
}

type Option func(*serverOptions)

type serverOptions struct {
	logger    *slog.Logger
	telemetry *telemetry.Telemetry
}

func WithLogger(l *slog.Logger) Option {
	return func(o *serverOptions) {
		o.logger = l
	}
}

func WithTelemetry(tel *telemetry.Telemetry) Option {
	return func(o *serverOptions) {
		o.telemetry = tel
	}
}

func New(cfg config.Config, authHandler *handler.AuthHandler, healthHandler *handler.HealthHandler, opts ...any) *Server {
	var sOpts serverOptions
	for _, opt := range opts {
		switch v := opt.(type) {
		case Option:
			v(&sOpts)
		case *slog.Logger:
			sOpts.logger = v
		case *telemetry.Telemetry:
			sOpts.telemetry = v
		}
	}

	app := fiber.New(fiber.Config{
		ErrorHandler: httperr.ErrorHandler,
	})

	if sOpts.telemetry != nil {
		app.Use(telemetry.FiberMiddleware(telemetry.FiberConfig{
			ServiceName: "auth-svc",
			Tracer:      sOpts.telemetry.Tracer,
			Metrics:     sOpts.telemetry.Metrics,
			Logger:      sOpts.logger,
		}))
		RegisterRoutes(app, cfg, authHandler, healthHandler, sOpts.telemetry.Metrics)
	} else {
		RegisterRoutes(app, cfg, authHandler, healthHandler)
	}

	srv := &Server{
		app:    app,
		cfg:    cfg,
		logger: sOpts.logger,
	}

	if sOpts.logger != nil {
		srv.LogRoutes(sOpts.logger)
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
