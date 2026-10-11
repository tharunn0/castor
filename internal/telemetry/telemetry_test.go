package telemetry

import (
	"context"
	"testing"
)

func TestNormalizeEnv(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"prod", EnvProd},
		{"PROD", EnvProd},
		{"production", EnvProd},
		{"stage", EnvStage},
		{"STAGE", EnvStage},
		{"staging", EnvStage},
		{"off", EnvOff},
		{"OFF", EnvOff},
		{"none", EnvOff},
		{"disable", EnvOff},
		{"disabled", EnvOff},
		{"dev", EnvDev},
		{"development", EnvDev},
		{"local", EnvDev},
		{"unknown", EnvDev},
		{"", EnvDev},
	}

	for _, tc := range tests {
		actual := NormalizeEnv(tc.input)
		if actual != tc.expected {
			t.Errorf("NormalizeEnv(%q) = %q; want %q", tc.input, actual, tc.expected)
		}
	}
}

func TestGetEnv(t *testing.T) {
	t.Setenv("ENV", "prod")
	if env := GetEnv(); env != EnvProd {
		t.Errorf("GetEnv() with ENV=prod = %q; want %q", env, EnvProd)
	}

	t.Setenv("ENV", "")
	t.Setenv("APP_ENV", "off")
	if env := GetEnv(); env != EnvOff {
		t.Errorf("GetEnv() with APP_ENV=off = %q; want %q", env, EnvOff)
	}

	t.Setenv("APP_ENV", "")
	t.Setenv("ENVIRONMENT", "production")
	if env := GetEnv(); env != EnvProd {
		t.Errorf("GetEnv() with ENVIRONMENT=production = %q; want %q", env, EnvProd)
	}

	t.Setenv("ENVIRONMENT", "")
	t.Setenv("TELEMETRY_ENV", "dev")
	if env := GetEnv(); env != EnvDev {
		t.Errorf("GetEnv() with TELEMETRY_ENV=dev = %q; want %q", env, EnvDev)
	}
}

func TestNewLogger(t *testing.T) {
	prodLogger := NewLogger("prod")
	if prodLogger == nil {
		t.Fatal("expected non-nil prod logger")
	}
	prodLogger.Info("test prod log")

	stageLogger := NewLogger("stage")
	if stageLogger == nil {
		t.Fatal("expected non-nil stage logger")
	}
	stageLogger.Info("test stage log")

	devLogger := NewLogger("dev")
	if devLogger == nil {
		t.Fatal("expected non-nil dev logger")
	}
	devLogger.Debug("test dev log")

	offLogger := NewLogger("off")
	if offLogger == nil {
		t.Fatal("expected non-nil off logger")
	}
	offLogger.Error("test off log")
}

func TestTelemetry(t *testing.T) {
	ctx := context.Background()

	// Test Off (noop)
	tpOff, shutdownOff, err := NewTracerProvider(ctx, "test-svc", "off")
	if err != nil {
		t.Fatalf("unexpected error for off tracer: %v", err)
	}
	if tpOff == nil {
		t.Fatal("expected non-nil tracer provider for off")
	}
	tracerOff := tpOff.Tracer("test")
	_, spanOff := tracerOff.Start(ctx, "noop-span")
	if spanOff.IsRecording() {
		t.Errorf("expected noop span not to be recording")
	}
	spanOff.End()
	if err := shutdownOff(ctx); err != nil {
		t.Errorf("unexpected error on shutdown: %v", err)
	}

	// Test Init
	tel, err := Init(ctx, "test-svc", "dev")
	if err != nil {
		t.Fatalf("failed to init telemetry: %v", err)
	}
	defer func() {
		_ = tel.Shutdown(ctx)
	}()

	if tel.Logger == nil {
		t.Error("expected non-nil logger in telemetry")
	}
	if tel.Tracer == nil {
		t.Error("expected non-nil tracer in telemetry")
	}

	_, span := tel.Tracer.Start(ctx, "dev-span")
	span.End()
}

func TestTelemetry_Stage(t *testing.T) {
	ctx := context.Background()
	tel, err := Init(ctx, "stage-svc", "stage")
	if err != nil {
		t.Fatalf("failed to init telemetry in stage: %v", err)
	}
	defer func() {
		_ = tel.Shutdown(ctx)
	}()

	if tel.Logger == nil {
		t.Fatal("expected non-nil logger in stage")
	}
	if tel.Tracer == nil {
		t.Fatal("expected non-nil tracer in stage")
	}

	_, span := tel.Tracer.Start(ctx, "stage-span")
	if !span.IsRecording() {
		t.Error("expected recording span in stage")
	}
	span.End()
}

func TestTelemetry_Prod(t *testing.T) {
	ctx := context.Background()
	tel, err := Init(ctx, "prod-svc", "prod")
	if err != nil {
		t.Fatalf("failed to init telemetry in prod: %v", err)
	}
	defer func() {
		_ = tel.Shutdown(ctx)
	}()

	if tel.Logger == nil {
		t.Fatal("expected non-nil logger in prod")
	}
	if tel.Tracer == nil {
		t.Fatal("expected non-nil tracer in prod")
	}
}

func TestTelemetry_ExportersDisabled(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		ServiceName:        "noop-exp-svc",
		Env:                EnvDev,
		TracingEnabled:     true,
		TraceStdoutEnabled: false,
		TraceOTLPEnabled:   false,
	}

	tp, shutdown, err := NewTracerProviderWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	tr := tp.Tracer("test")
	_, span := tr.Start(ctx, "noop-span")
	if span.IsRecording() {
		t.Error("expected non-recording span when both exporters disabled")
	}
	span.End()
}
