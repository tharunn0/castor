package telemetry

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

func GetEnv() string {
	for _, key := range []string{"ENV", "APP_ENV", "ENVIRONMENT"} {
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
	case "stage", "staging":
		return EnvStage
	case "test":
		return EnvTest
	case "off", "none", "disable", "disabled":
		return EnvOff
	default:
		return EnvDev
	}
}

func NewLoggerWithConfig(cfg Config) *slog.Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level:     cfg.LogLevel,
		AddSource: cfg.LogAddSource,
	}

	if cfg.Env == EnvOff || !cfg.LogStdoutEnabled {
		handler = slog.NewTextHandler(io.Discard, opts)
	} else if cfg.LogFormat == LogFormatJSON {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(NewTraceContextHandler(handler))
}

func NewLogger(env ...string) *slog.Logger {
	cfg := LoadConfig("", env...)
	return NewLoggerWithConfig(cfg)
}

func InitLoggerWithConfig(cfg Config) *slog.Logger {
	logger := NewLoggerWithConfig(cfg)
	slog.SetDefault(logger)
	return logger
}

func InitLogger(env ...string) *slog.Logger {
	logger := NewLogger(env...)
	slog.SetDefault(logger)
	return logger
}
