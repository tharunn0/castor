package telemetry

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// ClientDialOption returns a gRPC DialOption configuring OpenTelemetry client stats handling for trace context propagation.
func ClientDialOption() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler())
}

// ServerOption returns a gRPC ServerOption configuring OpenTelemetry server stats handling.
func ServerOption() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler())
}
