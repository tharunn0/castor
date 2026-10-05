package config

import (
	"os"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPAddr    string
	DatabaseURL string
	AdminKey    string
	JWTSecret   string
	JWTExpiry   time.Duration
}

func Load() Config {
	_ = godotenv.Load()

	return Config{
		HTTPAddr:    getEnv("AUTH_HTTP_ADDR", ":9095"),
		DatabaseURL: getEnv("AUTH_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/castor_auth?sslmode=disable"),
		AdminKey:    getEnv("ADMIN_KEY", getEnv("AUTH_ADMIN_KEY", "")),
		JWTSecret:   getEnv("AUTH_JWT_SECRET", "castor-auth-jwt-super-secret-key-change-in-production"),
		JWTExpiry:   parseDuration(getEnv("AUTH_JWT_EXPIRY", "24h"), 24*time.Hour),
	}
}

func parseDuration(val string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(val)
	if err != nil {
		return fallback
	}
	return d
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
