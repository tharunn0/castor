package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/server"
	"github.com/tharunn0/castor/internal/data/storage"
	datatelemetry "github.com/tharunn0/castor/internal/data/telemetry"
	"github.com/tharunn0/castor/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

const maxChunkSize = 4 << 20

func main() {
	ctx := context.Background()
	tel, err := telemetry.Init(ctx, "data-svc")
	if err != nil {
		slog.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}
	defer func() {
		_ = tel.Shutdown(context.Background())
	}()

	logger := tel.Logger
	cfg := config.Load()

	dataMetrics := datatelemetry.NewDataMetrics(cfg.NodeId, tel.Metrics)

	logger.Info("starting data service",
		"node_id", cfg.NodeId,
		"grpc_addr", cfg.GRPCAddr,
		"metrics_addr", cfg.MetricsAddr,
		"data_dir", cfg.DataDir,
	)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to bind gRPC port", "error", err, "addr", cfg.GRPCAddr)
		os.Exit(1)
	}

	store, err := storage.New(cfg.DataDir, maxChunkSize,
		storage.WithLogger(logger),
		storage.WithMetrics(dataMetrics),
	)
	if err != nil {
		logger.Error("failed to initialize storage", "error", err, "dir", cfg.DataDir)
		os.Exit(1)
	}

	_ = store.UpdateCapacityMetrics(dataMetrics)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
				return info == nil || info.FullMethodName != "/castor.v1.DataService/HealthCheck"
			}),
		)),
	)
	dataServer := server.New(store, cfg,
		server.WithLogger(logger),
		server.WithMetrics(dataMetrics),
	)
	castorv1.RegisterDataServiceServer(grpcServer, dataServer)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", tel.Metrics.Handler)
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	metricsServer := &http.Server{
		Addr:    cfg.MetricsAddr,
		Handler: metricsMux,
	}

	go func() {
		logger.Info("data HTTP metrics server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics HTTP server error", "error", err)
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	serverErrChan := make(chan error, 1)
	go func() {
		logger.Info("data gRPC server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErrChan <- err
		}
	}()

	select {
	case sig := <-shutdownChan:
		logger.Info("shutting down data service", "signal", sig.String())
	case err := <-serverErrChan:
		logger.Error("data gRPC server error", "error", err)
	}

	grpcServer.GracefulStop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("metrics HTTP server shutdown failed", "error", err)
	}

	logger.Info("data service stopped cleanly")
}
