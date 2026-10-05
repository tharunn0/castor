package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPAddr    string
	DatabaseURL string
	AdminKey    string
}

func Load() Config {
	_ = godotenv.Load()

	return Config{
		HTTPAddr:    getEnv("AUTH_HTTP_ADDR", ":9095"),
		DatabaseURL: getEnv("AUTH_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/castor_auth?sslmode=disable"),
		AdminKey:    getEnv("ADMIN_KEY", getEnv("AUTH_ADMIN_KEY", "")),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
