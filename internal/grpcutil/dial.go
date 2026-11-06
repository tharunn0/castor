// Package grpcutil holds gRPC client settings shared by Castor services.
package grpcutil

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	MinConnectTimeout   = 2 * time.Second
	MaxReconnectBackoff = 3 * time.Second
)

func ConnectParams() grpc.DialOption {
	return grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  250 * time.Millisecond,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   MaxReconnectBackoff,
		},
		MinConnectTimeout: MinConnectTimeout,
	})
}
