package main

import (
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/server"
	"github.com/tharunn0/castor/internal/data/storage"
	"github.com/tharunn0/castor/internal/telemetry"
	"google.golang.org/grpc"
)

const maxChunkSize = 4 << 20

func main() {
	logger := telemetry.NewLogger()
	cfg := config.Load()

	logger.Info("starting data service",
		"node_id", cfg.NodeId,
		"grpc_addr", cfg.GRPCAddr,
		"data_dir", cfg.DataDir,
	)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to bind gRPC port", "error", err, "addr", cfg.GRPCAddr)
		os.Exit(1)
	}

	store, err := storage.New(cfg.DataDir, maxChunkSize)
	if err != nil {
		logger.Error("failed to initialize storage", "error", err, "dir", cfg.DataDir)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	dataServer := server.New(store, cfg)
	castorv1.RegisterDataServiceServer(grpcServer, dataServer)

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
	logger.Info("data service stopped cleanly")
}
