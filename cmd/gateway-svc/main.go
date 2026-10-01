package main

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"
	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/gateway/backend"
	"github.com/tharunn0/castor/internal/gateway/config"
	"github.com/tharunn0/castor/internal/gateway/health"
	"github.com/tharunn0/castor/internal/gateway/storage"
	"github.com/tharunn0/castor/internal/telemetry"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := telemetry.NewLogger()
	cfg := config.Load()

	logger.Info("starting gateway service",
		"s3_addr", cfg.S3Addr,
		"console_addr", cfg.ConsoleAddr,
		"metadata_addr", cfg.MetadataAddr,
		"data_nodes", cfg.DataNodes,
	)

	metaConn, err := grpc.NewClient(cfg.MetadataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to metadata service", "error", err, "addr", cfg.MetadataAddr)
		os.Exit(1)
	}
	defer metaConn.Close()
	metaClient := castorv1.NewMetadataServiceClient(metaConn)

	engine := storage.New(cfg, metaClient)
	defer engine.Close()

	be := backend.New(engine, metaClient)

	healthApp := health.NewApp(cfg)
	healthHandler := health.NewHandler(cfg)

	go func() {
		logger.Info("gateway HTTP health server listening", "addr", cfg.ConsoleAddr)
		if err := healthApp.Listen(cfg.ConsoleAddr, fiber.ListenConfig{
			DisableStartupMessage: true,
		}); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("health server error", "error", err)
		}
	}()

	gwCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwCfg := &embedgw.Config{
		Ports:             []string{cfg.S3Addr},
		RootUserAccess:    cfg.RootAccessKey,
		RootUserSecret:    cfg.RootSecretKey,
		Region:            "us-east-1",
		Quiet:             true,
		MaxConnections:    65536,
		MaxRequests:       65536,
		MultipartMaxParts: 10000,
		S3Options: []s3api.Option{
			s3api.WithRoute("GET", "/health", healthHandler),
			s3api.WithRoute("GET", "/healthz", healthHandler),
		},
	}

	gwErrChan := make(chan error, 1)
	go func() {
		logger.Info("gateway S3 API server listening", "addr", cfg.S3Addr)
		if err := embedgw.RunVersityGW(gwCtx, be, gwCfg); err != nil && !errors.Is(err, context.Canceled) {
			gwErrChan <- err
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-shutdownChan:
		logger.Info("shutting down gateway service", "signal", sig.String())
	case err := <-gwErrChan:
		logger.Error("gateway S3 server error", "error", err)
	}

	cancel()
	_ = healthApp.Shutdown()
	logger.Info("gateway service stopped cleanly")
}
