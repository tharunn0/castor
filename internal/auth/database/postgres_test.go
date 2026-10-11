package database

import (
	"context"
	"testing"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
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
