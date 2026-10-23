package config

import (
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type NodeConfig struct {
	NodeId            string
	GRPCAddr          string
	MetricsAddr       string
	DataDir           string
	MetadataAddr      string
	MetadataNodes     []string
	AdvertiseAddr     string
	HeartbeatInterval time.Duration
}

func Load() NodeConfig {
	_ = godotenv.Load()

	id := getEnv("NODE_ID", "data-1")
	grpcAddr := getEnv("GRPC_ADDR", ":9101")
	metricsAddr := getEnv("METRICS_ADDR", ":9104")
	dataDir := getEnv("DATA_DIR", "data")
	metadataAddr := getEnv("METADATA_ADDR", "127.0.0.1:9090")
	metadataNodesRaw := getEnv("METADATA_NODES", metadataAddr)

	var metadataNodes []string
	for node := range strings.SplitSeq(metadataNodesRaw, ",") {
		trimmed := strings.TrimSpace(node)
		if trimmed != "" {
			metadataNodes = append(metadataNodes, trimmed)
		}
	}
	if len(metadataNodes) == 0 && metadataAddr != "" {
		metadataNodes = []string{metadataAddr}
	}

	advertiseAddr := getEnv("ADVERTISE_ADDR", grpcAddr)

	heartbeatInterval := 3 * time.Second
	if raw := os.Getenv("HEARTBEAT_INTERVAL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			heartbeatInterval = d
		}
	}

	return NodeConfig{
		NodeId:            id,
		GRPCAddr:          grpcAddr,
		MetricsAddr:       metricsAddr,
		DataDir:           dataDir,
		MetadataAddr:      metadataAddr,
		MetadataNodes:     metadataNodes,
		AdvertiseAddr:     advertiseAddr,
		HeartbeatInterval: heartbeatInterval,
	}
}

func getEnv(env, def string) string {
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	return v
}
