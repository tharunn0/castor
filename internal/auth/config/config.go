package config

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

var ErrAdminSecretKeyRequired = errors.New("admin secret key is required")

type Config struct {
	HTTPAddr        string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration

	DatabaseURL         string
	DBMaxConns          int32
	DBMinConns          int32
	DBMaxConnLifetime   time.Duration
	DBMaxConnIdleTime   time.Duration
	DBHealthCheckPeriod time.Duration
	DBConnectTimeout    time.Duration

	AdminKey       string
	AdminSecretKey string
	BcryptCost     int
	JWTSecret      string
	JWTExpiry      time.Duration
}

func Load() (Config, error) {
	_ = godotenv.Load()

	adminSecret := getEnv("ADMIN_SECRET_KEY", getEnv("AUTH_ADMIN_SECRET_KEY", getEnv("ADMIN_KEY", getEnv("AUTH_ADMIN_KEY", ""))))

	dbMaxConns := parseInt32(getEnv("AUTH_DB_MAX_CONNS", "25"), 25)
	if dbMaxConns <= 0 {
		dbMaxConns = 25
	}

	dbMinConns := parseInt32(getEnv("AUTH_DB_MIN_CONNS", "5"), 5)
	if dbMinConns < 0 {
		dbMinConns = 5
	}
	if dbMinConns > dbMaxConns {
		dbMinConns = dbMaxConns
	}

	bcryptCost := parseInt(getEnv("AUTH_BCRYPT_COST", "12"), 12)
	if bcryptCost < 4 || bcryptCost > 31 {
		bcryptCost = 12
	}

	cfg := Config{
		HTTPAddr:        getEnv("AUTH_HTTP_ADDR", ":9095"),
		ReadTimeout:     parseDuration(getEnv("AUTH_READ_TIMEOUT", "10s"), 10*time.Second),
		WriteTimeout:    parseDuration(getEnv("AUTH_WRITE_TIMEOUT", "10s"), 10*time.Second),
		IdleTimeout:     parseDuration(getEnv("AUTH_IDLE_TIMEOUT", "120s"), 120*time.Second),
		ShutdownTimeout: parseDuration(getEnv("AUTH_SHUTDOWN_TIMEOUT", "10s"), 10*time.Second),

		DatabaseURL:         getEnv("AUTH_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/castor_auth?sslmode=disable"),
		DBMaxConns:          dbMaxConns,
		DBMinConns:          dbMinConns,
		DBMaxConnLifetime:   parseDuration(getEnv("AUTH_DB_MAX_CONN_LIFETIME", "1h"), 1*time.Hour),
		DBMaxConnIdleTime:   parseDuration(getEnv("AUTH_DB_MAX_CONN_IDLE_TIME", "30m"), 30*time.Minute),
		DBHealthCheckPeriod: parseDuration(getEnv("AUTH_DB_HEALTH_CHECK_PERIOD", "1m"), 1*time.Minute),
		DBConnectTimeout:    parseDuration(getEnv("AUTH_DB_CONNECT_TIMEOUT", "5s"), 5*time.Second),

		AdminKey:       adminSecret,
		AdminSecretKey: adminSecret,
		BcryptCost:     bcryptCost,
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

func parseInt32(val string, fallback int32) int32 {
	n, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(n)
}

func parseInt(val string, fallback int) int {
	n, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return n
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
