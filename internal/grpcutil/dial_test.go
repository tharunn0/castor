package grpcutil

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// A listener that accepts TCP but never speaks HTTP/2 behaves like a dial to
// a vanished pod IP. With gRPC's defaults the RPC below would wait out the
// caller's whole 15s deadline; ConnectParams must fail it after ~MinConnectTimeout.
func TestConnectParams_FailsFastOnBlackholedPeer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		ConnectParams(),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	_, err = grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	elapsed := time.Since(start)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	if elapsed > MinConnectTimeout+3*time.Second {
		t.Fatalf("RPC failed after %v, want about MinConnectTimeout (%v)", elapsed, MinConnectTimeout)
	}
}
