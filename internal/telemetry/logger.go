package telemetry

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

const (
	EnvProd = "prod"
	EnvDev  = "dev"
	EnvOff  = "off"
)

func GetEnv() string {
	for _, key := range []string{"ENV", "APP_ENV"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return NormalizeEnv(val)
		}
	}
	return EnvDev
}

func NormalizeEnv(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "prod", "production":
		return EnvProd
	case "off", "none", "disable", "disabled":
		return EnvOff
	default:
		return EnvDev
	}
}

func NewLogger(env ...string) *slog.Logger {
	targetEnv := GetEnv()
	if len(env) > 0 && strings.TrimSpace(env[0]) != "" {
		targetEnv = NormalizeEnv(env[0])
	}

	var handler slog.Handler
	switch targetEnv {
	case EnvProd:
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	case EnvOff:
		handler = slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
			Level: slog.LevelError,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})
	}

	return slog.New(handler)
}

// InitLogger initializes a new logger, sets it as the default slog logger, and returns it.
func InitLogger(env ...string) *slog.Logger {
	logger := NewLogger(env...)
	slog.SetDefault(logger)
	return logger
}
