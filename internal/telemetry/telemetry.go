package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Telemetry struct {
	Logger         *slog.Logger
	TracerProvider trace.TracerProvider
	Tracer         trace.Tracer
	Metrics        *Metrics
	Shutdown       func(context.Context) error
}

func NewTracerProviderWithConfig(ctx context.Context, cfg Config) (trace.TracerProvider, func(context.Context) error, error) {
	if !cfg.TracingEnabled || cfg.Env == EnvOff || (!cfg.TraceStdoutEnabled && !cfg.TraceOTLPEnabled) {
		tp := noop.NewTracerProvider()
		return tp, func(context.Context) error { return nil }, nil
	}

	var tpOpts []sdktrace.TracerProviderOption

	if cfg.TraceStdoutEnabled {
		var traceOpts []stdouttrace.Option
		if cfg.Env == EnvDev {
			traceOpts = append(traceOpts, stdouttrace.WithPrettyPrint())
		}

		stdoutExp, err := stdouttrace.New(traceOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize stdout trace exporter: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(stdoutExp))
	}

	if cfg.TraceOTLPEnabled {
		endpoint := strings.TrimSpace(cfg.OTLPEndpoint)
		endpoint = strings.TrimPrefix(endpoint, "http://")
		endpoint = strings.TrimPrefix(endpoint, "https://")
		if endpoint == "" {
			endpoint = "localhost:4317"
		}

		otlpOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(endpoint),
		}
		if cfg.OTLPInsecure {
			otlpOpts = append(otlpOpts, otlptracegrpc.WithInsecure())
		}

		otlpExp, err := otlptracegrpc.New(ctx, otlpOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize otlp trace exporter: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(otlpExp))
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			attribute.String("environment", cfg.Env),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create otel resource: %w", err)
	}

	var sampler sdktrace.Sampler
	switch cfg.TraceSampler {
	case TraceSamplerNever:
		sampler = sdktrace.NeverSample()
	case TraceSamplerRatio:
		sampler = sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio)
	case TraceSamplerParentBased:
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio))
	default:
		sampler = sdktrace.AlwaysSample()
	}

	tpOpts = append(tpOpts, sdktrace.WithResource(res), sdktrace.WithSampler(sampler))

	tp := sdktrace.NewTracerProvider(tpOpts...)

	return tp, tp.Shutdown, nil
}

func NewTracerProvider(ctx context.Context, serviceName string, env ...string) (trace.TracerProvider, func(context.Context) error, error) {
	cfg := LoadConfig(serviceName, env...)
	return NewTracerProviderWithConfig(ctx, cfg)
}

func InitWithConfig(ctx context.Context, cfg Config) (*Telemetry, error) {
	logger := InitLoggerWithConfig(cfg)

	tp, shutdown, err := NewTracerProviderWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := tp.Tracer(cfg.ServiceName)
	metrics := InitMetricsWithConfig(cfg)

	return &Telemetry{
		Logger:         logger,
		TracerProvider: tp,
		Tracer:         tracer,
		Metrics:        metrics,
		Shutdown:       shutdown,
	}, nil
}

func Init(ctx context.Context, serviceName string, env ...string) (*Telemetry, error) {
	cfg := LoadConfig(serviceName, env...)
	return InitWithConfig(ctx, cfg)
}

func GetTracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name, opts...)
}

func NewNoopTracerProvider() trace.TracerProvider {
	return noop.NewTracerProvider()
}

func NewNoopTracer() trace.Tracer {
	return noop.NewTracerProvider().Tracer("noop")
}
