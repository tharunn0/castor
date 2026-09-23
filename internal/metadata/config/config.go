package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds runtime configuration for the metadata-svc node.
type Config struct {
	NodeID        string
	GRPCAddr      string
	RaftAddr      string
	DataDir       string
	RaftBootstrap bool
	Peers         map[string]string // map[nodeID]raftAddr
}

// Load loads metadata-svc configuration from environment variables.
func Load() Config {
	_ = godotenv.Load()

	nodeID := getEnv("NODE_ID", "metadata-1")
	grpcAddr := getEnv("GRPC_ADDR", ":9090")
	raftAddr := getEnv("RAFT_ADDR", "127.0.0.1:9091")
	dataDir := getEnv("DATA_DIR", "data/metadata")
	bootstrapStr := getEnv("RAFT_BOOTSTRAP", "true")

	bootstrap, err := strconv.ParseBool(bootstrapStr)
	if err != nil {
		bootstrap = true
	}

	peersRaw := getEnv("RAFT_PEERS", "")
	peers := make(map[string]string)
	if peersRaw != "" {
		for _, entry := range strings.Split(peersRaw, ",") {
			parts := strings.Split(strings.TrimSpace(entry), "=")
			if len(parts) == 2 {
				peers[parts[0]] = parts[1]
			}
		}
	}

	return Config{
		NodeID:        nodeID,
		GRPCAddr:      grpcAddr,
		RaftAddr:      raftAddr,
		DataDir:       dataDir,
		RaftBootstrap: bootstrap,
		Peers:         peers,
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
