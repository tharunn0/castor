package telemetry

import (
	"context"
	"log/slog"
	"testing"
)

func TestLoadConfig_Baselines(t *testing.T) {
	tests := []struct {
		env         string
		wantEnv     string
		wantLevel   slog.Level
		wantFormat  string
		wantTracing bool
		wantMetrics bool
	}{
		{EnvDev, EnvDev, slog.LevelDebug, LogFormatText, true, true},
		{EnvProd, EnvProd, slog.LevelInfo, LogFormatJSON, true, true},
		{EnvTest, EnvTest, slog.LevelError, LogFormatText, false, true},
		{EnvOff, EnvOff, slog.LevelError, LogFormatText, false, false},
	}

	for _, tc := range tests {
		cfg := LoadConfig("test-svc", tc.env)
		if cfg.Env != tc.wantEnv {
			t.Errorf("[%s] Env = %q, want %q", tc.env, cfg.Env, tc.wantEnv)
		}
		if cfg.LogLevel != tc.wantLevel {
			t.Errorf("[%s] LogLevel = %v, want %v", tc.env, cfg.LogLevel, tc.wantLevel)
		}
		if cfg.LogFormat != tc.wantFormat {
			t.Errorf("[%s] LogFormat = %q, want %q", tc.env, cfg.LogFormat, tc.wantFormat)
		}
		if cfg.TracingEnabled != tc.wantTracing {
			t.Errorf("[%s] TracingEnabled = %v, want %v", tc.env, cfg.TracingEnabled, tc.wantTracing)
		}
		if cfg.MetricsEnabled != tc.wantMetrics {
			t.Errorf("[%s] MetricsEnabled = %v, want %v", tc.env, cfg.MetricsEnabled, tc.wantMetrics)
		}
	}
}

func TestLoadConfig_LoggingOverrides(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("LOG_ADD_SOURCE", "true")

	cfg := LoadConfig("svc", EnvProd)
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want LevelDebug", cfg.LogLevel)
	}
	if cfg.LogFormat != LogFormatText {
		t.Errorf("LogFormat = %q, want text", cfg.LogFormat)
	}
	if !cfg.LogAddSource {
		t.Errorf("LogAddSource = %v, want true", cfg.LogAddSource)
	}
}

func TestLoadConfig_TracingOverrides(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("TRACE_SAMPLER", "ratio")
	t.Setenv("TRACE_SAMPLE_RATIO", "0.25")

	cfg := LoadConfig("svc", EnvDev)
	if cfg.TracingEnabled {
		t.Errorf("TracingEnabled = %v, want false", cfg.TracingEnabled)
	}
	if cfg.TraceSampler != TraceSamplerRatio {
		t.Errorf("TraceSampler = %q, want ratio", cfg.TraceSampler)
	}
	if cfg.TraceSampleRatio != 0.25 {
		t.Errorf("TraceSampleRatio = %f, want 0.25", cfg.TraceSampleRatio)
	}

	t.Setenv("TRACING_ENABLED", "")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	cfgDisabled := LoadConfig("svc", EnvDev)
	if cfgDisabled.TracingEnabled {
		t.Errorf("TracingEnabled with OTEL_SDK_DISABLED=true = %v, want false", cfgDisabled.TracingEnabled)
	}
}

func TestLoadConfig_MetricsOverrides(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "false")

	cfg := LoadConfig("svc", EnvDev)
	if cfg.MetricsEnabled {
		t.Errorf("MetricsEnabled = %v, want false", cfg.MetricsEnabled)
	}

	m := NewMetricsWithConfig(cfg)
	if m == nil {
		t.Fatal("expected non-nil metrics struct")
	}
}

func TestLoadConfig_RatioBoundaryClamping(t *testing.T) {
	t.Setenv("TRACE_SAMPLE_RATIO", "1.5")
	cfgHigh := LoadConfig("svc", EnvProd)
	if cfgHigh.TraceSampleRatio != 1.0 {
		t.Errorf("TraceSampleRatio = %f, want 1.0", cfgHigh.TraceSampleRatio)
	}

	t.Setenv("TRACE_SAMPLE_RATIO", "-0.5")
	cfgLow := LoadConfig("svc", EnvProd)
	if cfgLow.TraceSampleRatio != 0.0 {
		t.Errorf("TraceSampleRatio = %f, want 0.0", cfgLow.TraceSampleRatio)
	}
}

func TestTelemetry_WithConfigOverrides(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		ServiceName:    "override-svc",
		Env:            EnvDev,
		LogLevel:       slog.LevelWarn,
		LogFormat:      LogFormatJSON,
		TracingEnabled: false,
		MetricsEnabled: false,
	}

	tel, err := InitWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("failed InitWithConfig: %v", err)
	}
	defer func() { _ = tel.Shutdown(ctx) }()

	if tel.Logger == nil {
		t.Fatal("expected non-nil logger")
	}
	if tel.Tracer == nil {
		t.Fatal("expected non-nil tracer")
	}
	_, span := tel.Tracer.Start(ctx, "test-span")
	if span.IsRecording() {
		t.Error("expected non-recording span when tracing disabled")
	}
	span.End()
}
