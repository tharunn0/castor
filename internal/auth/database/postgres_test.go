package database

import (
	"context"
	"testing"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/tharunn0/castor/internal/auth/config"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewPool_InvalidURL(t *testing.T) {
	_, err := NewPool(context.Background(), "://invalid-dsn")
	if err == nil {
		t.Fatal("expected error for invalid dsn, got nil")
	}
}

func TestNewPool_ValidConfig(t *testing.T) {
	pool, err := NewPool(context.Background(), "postgres://user:pass@localhost:5432/castor_auth?sslmode=disable")
	if err != nil {
		t.Fatalf("unexpected error initializing pool: %v", err)
	}
	defer pool.Close()

	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

func TestPing_NilPool(t *testing.T) {
	err := Ping(context.Background(), nil, time.Second)
	if err == nil {
		t.Fatal("expected error pinging nil pool, got nil")
	}
}

func TestNewPool_WithOtelpgxTracer(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	pool, err := NewPool(context.Background(),
		"postgres://user:pass@localhost:5432/castor_auth?sslmode=disable",
		otelpgx.WithTracerProvider(tp),
	)
	if err != nil {
		t.Fatalf("unexpected error initializing pool: %v", err)
	}
	defer pool.Close()

	if pool == nil {
		t.Fatal("expected non-nil pool")
	}

	tracer := pool.Config().ConnConfig.Tracer
	if tracer == nil {
		t.Fatal("expected non-nil ConnConfig.Tracer")
	}

	tracerInstance := tp.Tracer("test")
	ctx, parentSpan := tracerInstance.Start(context.Background(), "parent.operation")

	tracedCtx := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{
		SQL: "SELECT id FROM users WHERE id = $1",
	})
	tracer.TraceQueryEnd(tracedCtx, nil, pgx.TraceQueryEndData{})
	parentSpan.End()

	spans := exp.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected spans to be captured by otelpgx")
	}

	foundSelect := false
	for _, s := range spans {
		if s.Name == "SELECT" {
			foundSelect = true
			if s.Parent.SpanID() != parentSpan.SpanContext().SpanID() {
				t.Errorf("expected parent span ID %s, got %s", parentSpan.SpanContext().SpanID(), s.Parent.SpanID())
			}
			break
		}
	}
	if !foundSelect {
		t.Errorf("expected span name 'SELECT', got %+v", spans)
	}
}

func TestNewPool_DefaultTracer(t *testing.T) {
	pool, err := NewPool(context.Background(), "postgres://user:pass@localhost:5432/castor_auth?sslmode=disable")
	if err != nil {
		t.Fatalf("unexpected error initializing pool: %v", err)
	}
	defer pool.Close()

	if pool.Config().ConnConfig.Tracer == nil {
		t.Fatal("expected non-nil default ConnConfig.Tracer")
	}
}

func TestNewPoolWithConfig(t *testing.T) {
	cfg := config.Config{
		DatabaseURL:         "postgres://user:pass@localhost:5432/castor_auth?sslmode=disable",
		DBMaxConns:          50,
		DBMinConns:          10,
		DBMaxConnLifetime:   2 * time.Hour,
		DBMaxConnIdleTime:   45 * time.Minute,
		DBHealthCheckPeriod: 2 * time.Minute,
		DBConnectTimeout:    8 * time.Second,
	}

	pool, err := NewPoolWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error initializing pool with config: %v", err)
	}
	defer pool.Close()

	pCfg := pool.Config()
	if pCfg.MaxConns != 50 {
		t.Errorf("expected MaxConns 50, got %d", pCfg.MaxConns)
	}
	if pCfg.MinConns != 10 {
		t.Errorf("expected MinConns 10, got %d", pCfg.MinConns)
	}
	if pCfg.MaxConnLifetime != 2*time.Hour {
		t.Errorf("expected MaxConnLifetime 2h, got %v", pCfg.MaxConnLifetime)
	}
	if pCfg.MaxConnIdleTime != 45*time.Minute {
		t.Errorf("expected MaxConnIdleTime 45m, got %v", pCfg.MaxConnIdleTime)
	}
	if pCfg.HealthCheckPeriod != 2*time.Minute {
		t.Errorf("expected HealthCheckPeriod 2m, got %v", pCfg.HealthCheckPeriod)
	}
	if pCfg.ConnConfig.ConnectTimeout != 8*time.Second {
		t.Errorf("expected ConnectTimeout 8s, got %v", pCfg.ConnConfig.ConnectTimeout)
	}
}
