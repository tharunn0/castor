package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

func NewTracerProvider(ctx context.Context, serviceName string, env ...string) (trace.TracerProvider, func(context.Context) error, error) {
	targetEnv := GetEnv()
	if len(env) > 0 && strings.TrimSpace(env[0]) != "" {
		targetEnv = NormalizeEnv(env[0])
	}

	if targetEnv == EnvOff {
		tp := noop.NewTracerProvider()
		return tp, func(context.Context) error { return nil }, nil
	}

	var traceOpts []stdouttrace.Option
	if targetEnv == EnvDev {
		traceOpts = append(traceOpts, stdouttrace.WithPrettyPrint())
	}

	exporter, err := stdouttrace.New(traceOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize stdout trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", targetEnv),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	return tp, tp.Shutdown, nil
}

func Init(ctx context.Context, serviceName string, env ...string) (*Telemetry, error) {
	logger := InitLogger(env...)

	tp, shutdown, err := NewTracerProvider(ctx, serviceName, env...)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := tp.Tracer(serviceName)
	metrics := InitMetrics(serviceName, env...)

	return &Telemetry{
		Logger:         logger,
		TracerProvider: tp,
		Tracer:         tracer,
		Metrics:        metrics,
		Shutdown:       shutdown,
	}, nil
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
