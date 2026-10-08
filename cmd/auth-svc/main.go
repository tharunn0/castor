package main

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/database"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/repository"
	"github.com/tharunn0/castor/internal/auth/server"
	"github.com/tharunn0/castor/internal/auth/service"
	"github.com/tharunn0/castor/internal/telemetry"
)

func main() {
	logger := telemetry.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger.Info("starting auth service",
		"http_addr", cfg.HTTPAddr,
	)

	pool, err := database.NewPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to initialize postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := database.Ping(context.Background(), pool, 2*time.Second); err != nil {
		logger.Warn("unable to connect to postgres", "error", err)
	} else {
		logger.Info("connected to postgres database successfully")
	}

	repo := repository.NewPostgresRepository(pool)
	svc := service.NewService(cfg, repo)
	authHandler := handler.NewAuthHandler(svc)
	healthHandler := handler.NewHealthHandler(cfg, repo)

	srv := server.New(cfg, authHandler, healthHandler, logger)

	go func() {
		logger.Info("auth HTTP server listening", "addr", cfg.HTTPAddr)
		if err := srv.Listen(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("auth HTTP server error", "error", err)
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownChan
	logger.Info("shutting down auth service", "signal", sig.String())

	_ = srv.Shutdown()
	logger.Info("auth service stopped cleanly")
}
