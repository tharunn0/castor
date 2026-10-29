package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/joho/godotenv"
	"github.com/tharunn0/castor/internal/auth/config"
	"github.com/tharunn0/castor/internal/auth/database"
	"github.com/tharunn0/castor/internal/auth/handler"
	"github.com/tharunn0/castor/internal/auth/repository"
	"github.com/tharunn0/castor/internal/auth/server"
	"github.com/tharunn0/castor/internal/auth/service"
	authtel "github.com/tharunn0/castor/internal/auth/telemetry"
	"github.com/tharunn0/castor/internal/telemetry"
	"github.com/tharunn0/castor/internal/version"
)

func main() {
	_ = godotenv.Load()
	ctx := context.Background()
	tel, err := telemetry.Init(ctx, "auth-svc")
	if err != nil {
		slog.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}
	defer func() {
		_ = tel.Shutdown(context.Background())
	}()

	logger := tel.Logger
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger.Info("starting auth service",
		"version", version.Version,
		"commit", version.Commit,
		"http_addr", cfg.HTTPAddr,
	)

	pool, err := database.NewPoolWithConfig(context.Background(), cfg,
		otelpgx.WithTracerProvider(tel.TracerProvider),
	)
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

	authMetrics := authtel.NewAuthMetrics(tel.Metrics)

	repo := repository.NewPostgresRepository(pool)
	svc := service.NewService(cfg, repo).
		WithLogger(logger).
		WithTracer(tel.Tracer).
		WithMetrics(authMetrics)

	authHandler := handler.NewAuthHandler(svc)
	healthHandler := handler.NewHealthHandler(cfg, repo)

	srv := server.New(cfg, authHandler, healthHandler,
		server.WithLogger(logger),
		server.WithTelemetry(tel),
	)

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

	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 10 * time.Second
	}
	if err := srv.ShutdownWithTimeout(shutdownTimeout); err != nil {
		logger.Error("error shutting down auth server", "error", err)
	}
	logger.Info("auth service stopped cleanly")
}
