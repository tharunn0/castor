package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type traceContextHandler struct {
	inner slog.Handler
}

// NewTraceContextHandler wraps an slog.Handler to inject active OpenTelemetry
// trace_id and span_id into log records when present in context.
func NewTraceContextHandler(inner slog.Handler) slog.Handler {
	return &traceContextHandler{inner: inner}
}

func (h *traceContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if ctx != nil {
		span := trace.SpanFromContext(ctx)
		if span.SpanContext().IsValid() {
			record.AddAttrs(
				slog.String("trace_id", span.SpanContext().TraceID().String()),
				slog.String("span_id", span.SpanContext().SpanID().String()),
			)
		}
	}
	return h.inner.Handle(ctx, record)
}

func (h *traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceContextHandler) WithGroup(name string) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithGroup(name)}
}
