package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"github.com/tharunn0/castor/internal/metadata/server"
	"github.com/tharunn0/castor/internal/metadata/store"
	metatelemetry "github.com/tharunn0/castor/internal/metadata/telemetry"
	"github.com/tharunn0/castor/internal/telemetry"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	tel, err := telemetry.Init(ctx, "metadata-svc")
	if err != nil {
		slog.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}
	defer func() {
		_ = tel.Shutdown(context.Background())
	}()

	logger := tel.Logger
	cfg := config.Load()

	_ = metatelemetry.NewMetadataMetrics(cfg.NodeID, tel.Metrics)

	logger.Info("starting metadata service",
		"node_id", cfg.NodeID,
		"grpc_addr", cfg.GRPCAddr,
		"raft_addr", cfg.RaftAddr,
		"metrics_addr", cfg.MetricsAddr,
		"data_dir", cfg.DataDir,
		"bootstrap", cfg.RaftBootstrap,
	)

	badgerDir := filepath.Join(cfg.DataDir, "badger")
	dbStore, err := store.Open(badgerDir, false)
	if err != nil {
		logger.Error("failed to open metadata store", "error", err, "path", badgerDir)
		os.Exit(1)
	}

	fsm := consensus.NewFSM(dbStore, logger)
	raftNode, err := consensus.NewRaftNode(cfg, logger, fsm)
	if err != nil {
		logger.Error("failed to initialize raft node", "error", err)
		_ = dbStore.Close()
		os.Exit(1)
	}

	metaServer := server.New(dbStore, raftNode)
	grpcServer := grpc.NewServer(telemetry.ServerOption())
	castorv1.RegisterMetadataServiceServer(grpcServer, metaServer)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to bind gRPC port", "error", err, "addr", cfg.GRPCAddr)
		_ = raftNode.Shutdown()
		_ = dbStore.Close()
		os.Exit(1)
	}

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
		logger.Info("metadata HTTP metrics server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics HTTP server error", "error", err)
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	serverErrChan := make(chan error, 1)
	go func() {
		logger.Info("metadata gRPC server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErrChan <- err
		}
	}()

	select {
	case sig := <-shutdownChan:
		logger.Info("shutting down metadata service", "signal", sig.String())
	case err := <-serverErrChan:
		logger.Error("metadata gRPC server error", "error", err)
	}

	grpcServer.GracefulStop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("metrics HTTP server shutdown failed", "error", err)
	}

	if err := raftNode.Shutdown(); err != nil {
		logger.Error("error during raft shutdown", "error", err)
	}

	if err := dbStore.Close(); err != nil {
		logger.Error("error closing metadata store", "error", err)
	}

	logger.Info("metadata service stopped cleanly")
}
