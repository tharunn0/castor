package admin

import (
	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/metadata/consensus"
)

type RaftStatusProvider interface {
	Status() consensus.RaftStatus
}

type Server struct {
	app      *fiber.App
	addr     string
	provider RaftStatusProvider
}

func New(addr string, provider RaftStatusProvider) *Server {
	app := fiber.New()
	s := &Server{
		app:      app,
		addr:     addr,
		provider: provider,
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.app.Get("/admin/raft/status", s.handleRaftStatus)
	s.app.Get("/healthz", s.handleHealthz)
}

func (s *Server) handleRaftStatus(c fiber.Ctx) error {
	if s.provider == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "raft consensus not configured",
		})
	}
	return c.Status(fiber.StatusOK).JSON(s.provider.Status())
}

func (s *Server) handleHealthz(c fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status":  "SERVING",
		"service": "metadata-svc",
	})
}

func (s *Server) Start() error {
	return s.app.Listen(s.addr, fiber.ListenConfig{
		DisableStartupMessage: true,
	})
}

func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}

func (s *Server) App() *fiber.App {
	return s.app
}
