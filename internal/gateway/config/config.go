package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	S3Addr               string
	ConsoleAddr          string
	MetadataAddr         string
	MetadataNodes        []string
	DataNodes            []string
	RootAccessKey        string
	RootSecretKey        string
	ChunkSize            int64
	WriteQuorum          int
	MaxConcurrentUploads int
}

func Load() Config {
	_ = godotenv.Load()

	s3Addr := getEnv("S3_ADDR", ":9000")
	consoleAddr := getEnv("CONSOLE_ADDR", ":9001")
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

	dataNodesRaw := getEnv("DATA_NODES", "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103")
	var dataNodes []string
	for node := range strings.SplitSeq(dataNodesRaw, ",") {
		trimmed := strings.TrimSpace(node)
		if trimmed != "" {
			dataNodes = append(dataNodes, trimmed)
		}
	}

	rootAccessKey := getEnv("ROOT_ACCESS_KEY_ID", "admin")
	rootSecretKey := getEnv("ROOT_SECRET_ACCESS_KEY", "admin123")

	chunkSize := int64(4 * 1024 * 1024)
	if v := os.Getenv("CHUNK_SIZE"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			chunkSize = parsed
		}
	}

	writeQuorum := 2
	if v := os.Getenv("WRITE_QUORUM"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			writeQuorum = parsed
		}
	}

	maxConcurrentUploads := 64
	if v := os.Getenv("MAX_CONCURRENT_UPLOADS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			maxConcurrentUploads = parsed
		}
	}

	return Config{
		S3Addr:               s3Addr,
		ConsoleAddr:          consoleAddr,
		MetadataAddr:         metadataAddr,
		MetadataNodes:        metadataNodes,
		DataNodes:            dataNodes,
		RootAccessKey:        rootAccessKey,
		RootSecretKey:        rootSecretKey,
		ChunkSize:            chunkSize,
		WriteQuorum:          writeQuorum,
		MaxConcurrentUploads: maxConcurrentUploads,
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
