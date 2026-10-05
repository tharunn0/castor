package main

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/health"
	"github.com/tharunn0/castor/internal/auth/repository"
	"github.com/tharunn0/castor/internal/telemetry"
)

func main() {
	logger := telemetry.NewLogger()
	cfg := config.Load()

	logger.Info("starting auth service",
		"http_addr", cfg.HTTPAddr,
	)

	var pool *pgxpool.Pool
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Warn("invalid postgres 17 configuration url", "error", err)
	} else {
		p, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err != nil {
			logger.Warn("failed to initialize postgres 17 pool", "error", err)
		} else {
			pool = p
			defer pool.Close()

			pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := pool.Ping(pingCtx); err != nil {
				logger.Warn("unable to connect to postgres 17 database", "error", err)
			} else {
				logger.Info("connected to postgres 17 database successfully")
			}
			cancel()
		}
	}

	repo := repository.NewPostgresRepository(pool)
	healthApp := health.NewApp(cfg, repo)

	go func() {
		logger.Info("auth HTTP server listening", "addr", cfg.HTTPAddr)
		if err := healthApp.Listen(cfg.HTTPAddr, fiber.ListenConfig{
			DisableStartupMessage: true,
		}); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("auth HTTP server error", "error", err)
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownChan
	logger.Info("shutting down auth service", "signal", sig.String())

	_ = healthApp.Shutdown()
	logger.Info("auth service stopped cleanly")
}
