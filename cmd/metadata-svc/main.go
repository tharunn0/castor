package main

import (
	"errors"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/metadata/config"
	"github.com/tharunn0/castor/internal/metadata/consensus"
	"github.com/tharunn0/castor/internal/metadata/server"
	"github.com/tharunn0/castor/internal/metadata/store"
	"github.com/tharunn0/castor/internal/telemetry"
	"google.golang.org/grpc"
)

func main() {
	logger := telemetry.NewLogger()
	cfg := config.Load()

	logger.Info("starting metadata service",
		"node_id", cfg.NodeID,
		"grpc_addr", cfg.GRPCAddr,
		"raft_addr", cfg.RaftAddr,
		"data_dir", cfg.DataDir,
		"bootstrap", cfg.RaftBootstrap,
	)

	badgerDir := filepath.Join(cfg.DataDir, "badger")
	dbStore, err := store.Open(badgerDir, false)
	if err != nil {
		logger.Error("failed to open metadata store", "error", err, "path", badgerDir)
		os.Exit(1)
	}

	fsm := consensus.NewFSM(dbStore)
	raftNode, err := consensus.NewRaftNode(cfg, logger, fsm)
	if err != nil {
		logger.Error("failed to initialize raft node", "error", err)
		_ = dbStore.Close()
		os.Exit(1)
	}

	metaServer := server.New(dbStore, raftNode)
	grpcServer := grpc.NewServer()
	castorv1.RegisterMetadataServiceServer(grpcServer, metaServer)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to bind gRPC port", "error", err, "addr", cfg.GRPCAddr)
		_ = raftNode.Shutdown()
		_ = dbStore.Close()
		os.Exit(1)
	}

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

	if err := raftNode.Shutdown(); err != nil {
		logger.Error("error during raft shutdown", "error", err)
	}

	if err := dbStore.Close(); err != nil {
		logger.Error("error closing metadata store", "error", err)
	}

	logger.Info("metadata service stopped cleanly")
}
