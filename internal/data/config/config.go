package config

import (
	"os"

	"github.com/joho/godotenv"
)

type NodeConfig struct {
	NodeId   string
	GRPCAddr string
	DataDir  string
}

func Load() NodeConfig {
	_ = godotenv.Load()

	id := getEnv("NODE_ID", "data-1")
	grpcAddr := getEnv("GRPC_ADDR", ":9101")
	dataDir := getEnv("DATA_DIR", "data")

	return NodeConfig{
		NodeId:   id,
		GRPCAddr: grpcAddr,
		DataDir:  dataDir,
	}
}

func getEnv(env, def string) string {
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	return v
}
