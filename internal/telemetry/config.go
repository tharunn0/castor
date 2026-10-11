package telemetry

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

const (
	EnvDev   = "dev"
	EnvStage = "stage"
	EnvProd  = "prod"
	EnvTest  = "test"
	EnvOff   = "off"
)

const (
	LogFormatText = "text"
	LogFormatJSON = "json"
)

const (
	TraceSamplerAlways      = "always"
	TraceSamplerNever       = "never"
	TraceSamplerRatio       = "ratio"
	TraceSamplerParentBased = "parent_based"
)

type Config struct {
	ServiceName        string
	Env                string
	LogLevel           slog.Level
	LogFormat          string
	LogAddSource       bool
	LogStdoutEnabled   bool
	TracingEnabled     bool
	TraceStdoutEnabled bool
	TraceOTLPEnabled   bool
	OTLPEndpoint       string
	OTLPInsecure       bool
	TraceSampler       string
	TraceSampleRatio   float64
	MetricsEnabled     bool
}

func LoadConfig(serviceName string, envOverrides ...string) Config {
	baseEnv := GetEnv()
	if len(envOverrides) > 0 && strings.TrimSpace(envOverrides[0]) != "" {
		baseEnv = NormalizeEnv(envOverrides[0])
	}

	cfg := defaultConfigForEnv(baseEnv)
	cfg.ServiceName = strings.TrimSpace(serviceName)

	if raw := getFirstEnv("LOG_LEVEL", "LOGGER_LEVEL"); raw != "" {
		if lvl, ok := parseLogLevel(raw); ok {
			cfg.LogLevel = lvl
		}
	}

	if raw := getFirstEnv("LOG_FORMAT", "LOGGER_FORMAT"); raw != "" {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case LogFormatJSON:
			cfg.LogFormat = LogFormatJSON
		case LogFormatText:
			cfg.LogFormat = LogFormatText
		}
	}

	if raw := getFirstEnv("LOG_ADD_SOURCE", "LOGGER_ADD_SOURCE"); raw != "" {
		if val, ok := parseBool(raw); ok {
			cfg.LogAddSource = val
		}
	}

	if raw := getFirstEnv("LOG_STDOUT_ENABLED", "LOGGER_STDOUT_ENABLED"); raw != "" {
		if val, ok := parseBool(raw); ok {
			cfg.LogStdoutEnabled = val
		}
	}

	if raw := getFirstEnv("OTEL_SDK_DISABLED"); raw != "" {
		if disabled, ok := parseBool(raw); ok {
			cfg.TracingEnabled = !disabled
		}
	}
	if raw := getFirstEnv("TRACING_ENABLED", "OTEL_TRACING_ENABLED"); raw != "" {
		if val, ok := parseBool(raw); ok {
			cfg.TracingEnabled = val
		}
	}

	if raw := getFirstEnv("TRACE_STDOUT_ENABLED", "TRACE_EXPORTER_STDOUT"); raw != "" {
		if val, ok := parseBool(raw); ok {
			cfg.TraceStdoutEnabled = val
		}
	}

	if raw := getFirstEnv("TRACE_OTLP_ENABLED", "TRACE_EXPORTER_OTLP", "OTEL_EXPORTER_OTLP_ENABLED"); raw != "" {
		if val, ok := parseBool(raw); ok {
			cfg.TraceOTLPEnabled = val
		}
	}

	if raw := getFirstEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_OTLP_ENDPOINT"); raw != "" {
		cfg.OTLPEndpoint = raw
	}

	if raw := getFirstEnv("OTEL_EXPORTER_OTLP_INSECURE"); raw != "" {
		if val, ok := parseBool(raw); ok {
			cfg.OTLPInsecure = val
		}
	}

	if raw := getFirstEnv("TRACE_SAMPLER", "OTEL_TRACES_SAMPLER"); raw != "" {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "always", "always_on", "alwayson":
			cfg.TraceSampler = TraceSamplerAlways
		case "never", "always_off", "alwaysoff":
			cfg.TraceSampler = TraceSamplerNever
		case "ratio", "traceidratio":
			cfg.TraceSampler = TraceSamplerRatio
		case "parent_based", "parentbased":
			cfg.TraceSampler = TraceSamplerParentBased
		}
	}

	if raw := getFirstEnv("TRACE_SAMPLE_RATIO", "OTEL_TRACES_SAMPLER_ARG"); raw != "" {
		if ratio, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
			if ratio < 0.0 {
				ratio = 0.0
			} else if ratio > 1.0 {
				ratio = 1.0
			}
			cfg.TraceSampleRatio = ratio
		}
	}

	if raw := getFirstEnv("METRICS_ENABLED", "PROMETHEUS_ENABLED"); raw != "" {
		if val, ok := parseBool(raw); ok {
			cfg.MetricsEnabled = val
		}
	}

	return cfg
}

func defaultConfigForEnv(env string) Config {
	switch env {
	case EnvProd:
		return Config{
			Env:                EnvProd,
			LogLevel:           slog.LevelInfo,
			LogFormat:          LogFormatJSON,
			LogAddSource:       false,
			LogStdoutEnabled:   false,
			TracingEnabled:     true,
			TraceStdoutEnabled: false,
			TraceOTLPEnabled:   true,
			OTLPEndpoint:       "localhost:4317",
			OTLPInsecure:       true,
			TraceSampler:       TraceSamplerParentBased,
			TraceSampleRatio:   0.05,
			MetricsEnabled:     true,
		}
	case EnvStage:
		return Config{
			Env:                EnvStage,
			LogLevel:           slog.LevelInfo,
			LogFormat:          LogFormatJSON,
			LogAddSource:       false,
			LogStdoutEnabled:   true,
			TracingEnabled:     true,
			TraceStdoutEnabled: true,
			TraceOTLPEnabled:   true,
			OTLPEndpoint:       "localhost:4317",
			OTLPInsecure:       true,
			TraceSampler:       TraceSamplerAlways,
			TraceSampleRatio:   1.0,
			MetricsEnabled:     true,
		}
	case EnvTest:
		return Config{
			Env:                EnvTest,
			LogLevel:           slog.LevelError,
			LogFormat:          LogFormatText,
			LogAddSource:       false,
			LogStdoutEnabled:   false,
			TracingEnabled:     false,
			TraceStdoutEnabled: false,
			TraceOTLPEnabled:   false,
			OTLPEndpoint:       "localhost:4317",
			OTLPInsecure:       true,
			TraceSampler:       TraceSamplerNever,
			TraceSampleRatio:   0.0,
			MetricsEnabled:     true,
		}
	case EnvOff:
		return Config{
			Env:                EnvOff,
			LogLevel:           slog.LevelError,
			LogFormat:          LogFormatText,
			LogAddSource:       false,
			LogStdoutEnabled:   false,
			TracingEnabled:     false,
			TraceStdoutEnabled: false,
			TraceOTLPEnabled:   false,
			OTLPEndpoint:       "",
			OTLPInsecure:       true,
			TraceSampler:       TraceSamplerNever,
			TraceSampleRatio:   0.0,
			MetricsEnabled:     false,
		}
	default:
		return Config{
			Env:                EnvDev,
			LogLevel:           slog.LevelDebug,
			LogFormat:          LogFormatText,
			LogAddSource:       false,
			LogStdoutEnabled:   true,
			TracingEnabled:     true,
			TraceStdoutEnabled: true,
			TraceOTLPEnabled:   false,
			OTLPEndpoint:       "localhost:4317",
			OTLPInsecure:       true,
			TraceSampler:       TraceSamplerAlways,
			TraceSampleRatio:   1.0,
			MetricsEnabled:     true,
		}
	}
}

func getFirstEnv(keys ...string) string {
	for _, key := range keys {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return ""
}

func parseLogLevel(val string) (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return slog.LevelInfo, false
	}
}

func parseBool(val string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "t", "true", "on", "yes", "enabled":
		return true, true
	case "0", "f", "false", "off", "no", "disabled":
		return false, true
	default:
		return false, false
	}
}
