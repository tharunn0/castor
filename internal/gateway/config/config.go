package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/tharunn0/castor/internal/telemetry"
)

const (
	DefaultRootAccessKey = "admin"
	DefaultRootSecretKey = "admin123"
)

var (
	ErrDefaultRootCredsInProd = errors.New("ROOT_ACCESS_KEY_ID and ROOT_SECRET_ACCESS_KEY must be changed from their defaults in prod and staging")
	ErrInternalTokenRequired  = errors.New("AUTH_INTERNAL_TOKEN is required in prod and staging")
	ErrIncompleteTLSConfig    = errors.New("S3_TLS_CERT_FILE and S3_TLS_KEY_FILE must be set together")
)

type Config struct {
	S3Addr                 string
	S3TLSCertFile          string
	S3TLSKeyFile           string
	ConsoleAddr            string
	MetadataAddr           string
	MetadataNodes          []string
	AuthAddr               string
	AuthInternalToken      string
	Env                    string
	RootAccessKey          string
	RootSecretKey          string
	ChunkSize              int64
	WriteQuorum            int
	WriteQuorumGracePeriod time.Duration
	MaxConcurrentUploads   int

	// MetadataCallTimeout bounds one RPC attempt against one metadata node.
	MetadataCallTimeout time.Duration
	// MetadataLeaderWait is how long to keep retrying while no metadata node
	// accepts the request (e.g. during a Raft election) before failing with 503.
	MetadataLeaderWait time.Duration
	// DataChunkTimeout bounds one chunk transfer to or from one data node.
	DataChunkTimeout time.Duration
}

const (
	DefaultMetadataCallTimeout = 3 * time.Second
	DefaultMetadataLeaderWait  = 3 * time.Second
	DefaultDataChunkTimeout    = 30 * time.Second
)

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

	authAddr := getEnv("AUTH_ADDR", "http://127.0.0.1:9095")
	rootAccessKey := getEnv("ROOT_ACCESS_KEY_ID", DefaultRootAccessKey)
	rootSecretKey := getEnv("ROOT_SECRET_ACCESS_KEY", DefaultRootSecretKey)

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

	writeQuorumGracePeriod := 100 * time.Millisecond
	if v := os.Getenv("WRITE_QUORUM_GRACE_PERIOD"); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil && parsed >= 0 {
			writeQuorumGracePeriod = parsed
		}
	}

	maxConcurrentUploads := 64
	if v := os.Getenv("MAX_CONCURRENT_UPLOADS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			maxConcurrentUploads = parsed
		}
	}

	return Config{
		MetadataCallTimeout:    parsePositiveDuration("METADATA_CALL_TIMEOUT", DefaultMetadataCallTimeout),
		MetadataLeaderWait:     parsePositiveDuration("METADATA_LEADER_WAIT", DefaultMetadataLeaderWait),
		DataChunkTimeout:       parsePositiveDuration("DATA_CHUNK_TIMEOUT", DefaultDataChunkTimeout),
		S3Addr:                 s3Addr,
		S3TLSCertFile:          os.Getenv("S3_TLS_CERT_FILE"),
		S3TLSKeyFile:           os.Getenv("S3_TLS_KEY_FILE"),
		ConsoleAddr:            consoleAddr,
		MetadataAddr:           metadataAddr,
		MetadataNodes:          metadataNodes,
		AuthAddr:               authAddr,
		AuthInternalToken:      os.Getenv("AUTH_INTERNAL_TOKEN"),
		Env:                    telemetry.GetEnv(),
		RootAccessKey:          rootAccessKey,
		RootSecretKey:          rootSecretKey,
		ChunkSize:              chunkSize,
		WriteQuorum:            writeQuorum,
		WriteQuorumGracePeriod: writeQuorumGracePeriod,
		MaxConcurrentUploads:   maxConcurrentUploads,
	}
}

func (c Config) Validate() error {
	if (c.S3TLSCertFile == "") != (c.S3TLSKeyFile == "") {
		return ErrIncompleteTLSConfig
	}
	// Staging is internet-facing too, so it gets the same secret checks.
	if c.Env != telemetry.EnvProd && c.Env != telemetry.EnvStage {
		return nil
	}
	if c.RootAccessKey == DefaultRootAccessKey || c.RootSecretKey == DefaultRootSecretKey {
		return ErrDefaultRootCredsInProd
	}
	if c.AuthInternalToken == "" {
		return ErrInternalTokenRequired
	}
	return nil
}

func parsePositiveDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
