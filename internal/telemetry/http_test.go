package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextHandler(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("test-tracer")
	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	spanCtx := span.SpanContext()
	if !spanCtx.IsValid() {
		t.Fatal("expected valid span context")
	}

	var buf bytes.Buffer
	jsonHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewTraceContextHandler(jsonHandler))

	logger.InfoContext(ctx, "hello with trace context", "user", "alice")

	var logData map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logData); err != nil {
		t.Fatalf("failed to parse log json: %v", err)
	}

	if logData["trace_id"] != spanCtx.TraceID().String() {
		t.Errorf("expected trace_id %q, got %v", spanCtx.TraceID().String(), logData["trace_id"])
	}
	if logData["span_id"] != spanCtx.SpanID().String() {
		t.Errorf("expected span_id %q, got %v", spanCtx.SpanID().String(), logData["span_id"])
	}
}

func TestFiberMiddleware_TracingAndMetrics(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer := tp.Tracer("fiber-test")
	metrics := NewMetrics("test_fiber", "dev")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	app := fiber.New()
	app.Use(FiberMiddleware(FiberConfig{
		ServiceName: "fiber-test",
		Tracer:      tracer,
		Metrics:     metrics,
		Logger:      logger,
	}))

	app.Get("/api/v1/users/:id", func(c fiber.Ctx) error {
		// Context should contain an active span
		span := trace.SpanFromContext(c.Context())
		if !span.SpanContext().IsValid() {
			return fiber.NewError(fiber.StatusInternalServerError, "missing span in context")
		}
		return c.SendString("ok")
	})

	app.Get("/metrics", FiberPrometheusHandler(metrics))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/42", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	spans := exp.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span recorded")
	}

	foundSpan := false
	for _, s := range spans {
		if strings.Contains(s.Name, "/api/v1/users/:id") {
			foundSpan = true
			break
		}
	}
	if !foundSpan {
		t.Errorf("expected span named with route template /api/v1/users/:id, got names: %v", spans)
	}

	// Verify /metrics endpoint responds
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResp, err := app.Test(metricsReq)
	if err != nil {
		t.Fatalf("metrics endpoint request failed: %v", err)
	}
	if metricsResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from /metrics, got %d", metricsResp.StatusCode)
	}
}
