package telemetry

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// FiberConfig holds configuration options for Fiber HTTP telemetry middleware.
type FiberConfig struct {
	ServiceName string
	Tracer      trace.Tracer
	Propagator  propagation.TextMapPropagator
	Metrics     *Metrics
	Logger      *slog.Logger
	SkipPaths   map[string]struct{}
}

// FiberMiddleware creates a Fiber v3 handler that manages distributed tracing,
// Prometheus metrics recording, and structured access logging.
func FiberMiddleware(cfg FiberConfig) fiber.Handler {
	if cfg.Tracer == nil {
		if cfg.ServiceName != "" {
			cfg.Tracer = otel.GetTracerProvider().Tracer(cfg.ServiceName)
		} else {
			cfg.Tracer = otel.GetTracerProvider().Tracer("castor-http")
		}
	}
	if cfg.Propagator == nil {
		cfg.Propagator = otel.GetTextMapPropagator()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = DefaultMetrics()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SkipPaths == nil {
		cfg.SkipPaths = map[string]struct{}{
			"/metrics": {},
		}
	}

	return func(c fiber.Ctx) error {
		path := c.Path()
		if _, skip := cfg.SkipPaths[path]; skip {
			return c.Next()
		}

		start := time.Now()
		reqCtx := c.Context()

		// Extract incoming W3C trace context from request headers.
		hdr := make(http.Header)
		for key, val := range c.Request().Header.All() {
			hdr.Add(string(key), string(val))
		}
		extractedCtx := cfg.Propagator.Extract(reqCtx, propagation.HeaderCarrier(hdr))

		method := c.Method()
		spanName := fmt.Sprintf("HTTP %s %s", method, path)

		ctx, span := cfg.Tracer.Start(extractedCtx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(method),
				semconv.URLPath(path),
				attribute.String("http.client_ip", c.IP()),
			),
		)
		defer span.End()

		c.SetContext(ctx)

		err := c.Next()

		duration := time.Since(start)
		status := c.Response().StatusCode()
		if err != nil {
			if fiberErr, ok := err.(*fiber.Error); ok {
				status = fiberErr.Code
			} else {
				status = fiber.StatusInternalServerError
			}
			span.RecordError(err)
		}

		span.SetAttributes(semconv.HTTPResponseStatusCode(status))

		// Resolve route pattern template (e.g. /api/v1/keys/:key_id) to prevent metric cardinality explosion.
		routePattern := path
		if r := c.Route(); r != nil && r.Path != "" {
			routePattern = r.Path
		}
		span.SetName(fmt.Sprintf("HTTP %s %s", method, routePattern))

		statusStr := strconv.Itoa(status)
		if cfg.Metrics != nil {
			if cfg.Metrics.RequestsTotal != nil {
				cfg.Metrics.RequestsTotal.WithLabelValues(method, routePattern, statusStr).Inc()
			}
			if cfg.Metrics.RequestDuration != nil {
				cfg.Metrics.RequestDuration.WithLabelValues(method, routePattern, statusStr).Observe(duration.Seconds())
			}
		}

		logAttrs := []any{
			"method", method,
			"path", path,
			"route", routePattern,
			"status", status,
			"duration_ms", duration.Milliseconds(),
			"ip", c.IP(),
		}
		if status >= 500 {
			cfg.Logger.ErrorContext(ctx, "HTTP server error", append(logAttrs, "error", err)...)
		} else if status >= 400 {
			cfg.Logger.WarnContext(ctx, "HTTP client warning", logAttrs...)
		} else {
			cfg.Logger.InfoContext(ctx, "HTTP request handled", logAttrs...)
		}

		return err
	}
}

// FiberPrometheusHandler wraps the Prometheus HTTP metrics handler for Fiber v3.
func FiberPrometheusHandler(m ...*Metrics) fiber.Handler {
	var handler http.Handler
	if len(m) > 0 && m[0] != nil && m[0].Handler != nil {
		handler = m[0].Handler
	} else {
		handler = MetricsHandler()
	}
	return adaptor.HTTPHandler(handler)
}
