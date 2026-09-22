package config

import (
	"os"

	"github.com/joho/godotenv"
)

type NodeConfig struct {
	NodeId string
}

func Load() NodeConfig {
	godotenv.Load()

	id := getEnv("NODE_ID", "temp-node-id")

	return NodeConfig{
		NodeId: id,
	}
}

func getEnv(env, def string) string {
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	return v
}
