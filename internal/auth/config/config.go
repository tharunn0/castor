package config

import (
	"errors"
	"os"
	"time"

	"github.com/joho/godotenv"
)

var ErrAdminSecretKeyRequired = errors.New("admin secret key is required")

type Config struct {
	HTTPAddr       string
	DatabaseURL    string
	AdminKey       string
	AdminSecretKey string
	JWTSecret      string
	JWTExpiry      time.Duration
}

func Load() (Config, error) {
	_ = godotenv.Load()

	adminSecret := getEnv("ADMIN_SECRET_KEY", getEnv("AUTH_ADMIN_SECRET_KEY", getEnv("ADMIN_KEY", getEnv("AUTH_ADMIN_KEY", ""))))

	cfg := Config{
		HTTPAddr:       getEnv("AUTH_HTTP_ADDR", ":9095"),
		DatabaseURL:    getEnv("AUTH_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/castor_auth?sslmode=disable"),
		AdminKey:       adminSecret,
		AdminSecretKey: adminSecret,
		JWTSecret:      getEnv("AUTH_JWT_SECRET", "castor-auth-jwt-super-secret-key-change-in-production"),
		JWTExpiry:      parseDuration(getEnv("AUTH_JWT_EXPIRY", "24h"), 24*time.Hour),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if c.AdminSecretKey == "" && c.AdminKey == "" {
		return ErrAdminSecretKeyRequired
	}
	return nil
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
